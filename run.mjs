import { randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";

// Standalone Node 22+ client: no workspace imports, DB, migrations or SDKs.
export const PROTOCOL_VERSION = "v1";
const ROUND_MS = 20 * 60_000;
const REQUEST_MS = 45_000;
const MAX_REQUESTS = 1_200;
const OUTCOMES = new Set(["completed", "idle", "busy", "retry", "yielded", "blocked", "terminal"]);

export function configuration(env) {
  let url;
  try { url = new URL(env.ACADEMY_SCHEDULER_URL); } catch { throw new Error("invalid_scheduler_url"); }
  if (url.protocol !== "https:" || url.username || url.password || url.search || url.hash
    || url.pathname !== "/" || url.port || url.hostname === "localhost"
    || url.hostname.endsWith(".replit.dev")) throw new Error("invalid_scheduler_url");
  const secret = env.ACADEMY_SCHEDULER_SECRET;
  if (!secret || secret.length < 32 || secret.length > 512 || secret.trim() !== secret
    || /[\r\n]/.test(secret)) throw new Error("invalid_scheduler_secret");
  return { origin: url.origin, secret };
}

export function validateReply(value) {
  if (!value || value.protocolVersion !== PROTOCOL_VERSION || !OUTCOMES.has(value.outcome)
    || !Number.isFinite(value.retryAfterMs) || value.retryAfterMs < 0
    || !value.status || value.status.protocolVersion !== PROTOCOL_VERSION
    || !["awaiting_activation", "enabled"].includes(value.status.activation)
    || (value.round !== null && (!value.round || !Number.isFinite(Date.parse(value.round.deadlineAt))
      || (value.round.completedAt !== null && !Number.isFinite(Date.parse(value.round.completedAt)))))) {
    throw new Error("incompatible_scheduler_response");
  }
  return value;
}

export async function runScheduler(config, {
  fetchImpl = fetch, now = Date.now, invocationId = randomUUID(),
  sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  log = (entry) => console.log(JSON.stringify(entry)), checkOnly = false,
  signal,
} = {}) {
  const startedAt = now();
  let deadlineAt = startedAt + ROUND_MS;
  let requests = 0;
  let retries = 0;
  let blocked = false;
  const summary = (outcome) => {
    const result = { protocolVersion: PROTOCOL_VERSION, outcome, requests,
      durationMs: Math.max(0, now() - startedAt), requiresAttention: blocked };
    log(result); // No URLs, bearer values, payloads, identifiers or raw errors.
    return result;
  };
  while (now() < deadlineAt && requests < MAX_REQUESTS) {
    signal?.throwIfAborted();
    const remaining = deadlineAt - now();
    // Do not start a unit that cannot settle within this client's remaining round.
    if (remaining < REQUEST_MS) return summary("yielded");
    requests += 1;
    let response;
    try {
      response = await fetchImpl(`${config.origin}/api/academy/scheduled/${checkOnly ? "worker-status" : "step"}`, {
        method: checkOnly ? "GET" : "POST",
        redirect: "error", // Never forward the restricted key to a redirect target.
        headers: { Authorization: `Bearer ${config.secret}`, "Content-Type": "application/json",
          "X-Academy-Scheduler-Version": PROTOCOL_VERSION },
        ...(checkOnly ? {} : { body: JSON.stringify({ protocolVersion: PROTOCOL_VERSION, invocationId }) }),
        signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(Math.min(REQUEST_MS, remaining))])
          : AbortSignal.timeout(Math.min(REQUEST_MS, remaining)),
      });
    } catch {
      signal?.throwIfAborted();
      if (retries >= 3) throw new Error("scheduler_transport_unavailable");
      retries += 1;
      await sleep(Math.min(1_000 * 2 ** (retries - 1), Math.max(0, deadlineAt - now())));
      // A response may have been lost after commit; same invocation recovers server state.
      continue;
    }
    if (!response.ok) {
      // Drain/cancel the body before the next request; never log its text.
      await response.body?.cancel();
      if ([408, 429, 500, 502, 503, 504].includes(response.status) && retries < 3) {
        const seconds = Number(response.headers.get("retry-after"));
        const wait = Number.isFinite(seconds) && seconds > 0 ? seconds * 1_000 : 1_000 * 2 ** retries;
        if (wait > 5_000 || now() + wait + REQUEST_MS >= deadlineAt) return summary("retry");
        retries += 1;
        await sleep(wait);
        continue;
      }
      throw new Error([400, 401, 403, 409, 426].includes(response.status)
        ? "scheduler_configuration_rejected" : "scheduler_http_unavailable");
    }
    let json;
    try { json = await response.json(); } catch { throw new Error("invalid_scheduler_response"); }
    if (checkOnly) {
      if (!json || json.protocolVersion !== PROTOCOL_VERSION
        || !["awaiting_activation", "enabled"].includes(json.activation)) throw new Error("incompatible_scheduler_response");
      blocked = json.health === "blocked";
      return summary(json.activation === "enabled" ? "checked" : "awaiting_activation");
    }
    const reply = validateReply(json);
    retries = 0;
    if (reply.round) deadlineAt = Math.min(deadlineAt, Date.parse(reply.round.deadlineAt));
    if (reply.outcome === "blocked" || reply.outcome === "terminal") blocked = true;
    if (reply.status.activation !== "enabled") return summary("awaiting_activation");
    if (reply.round?.completedAt || (!reply.lane && reply.outcome === "idle")) return summary(blocked ? "blocked" : "idle");
    // Other request/instance owns this round. Don't occupy a Scheduled machine waiting.
    if (!reply.lane && reply.outcome === "busy") return summary("busy");
    // Lane-local errors do not starve the next lane. Global cooldown ends the round.
    if (!reply.lane && reply.retryAfterMs > 5_000) return summary(blocked ? "blocked" : "retry");
    if (!reply.lane && reply.retryAfterMs > 0) await sleep(Math.min(reply.retryAfterMs, Math.max(0, deadlineAt - now())));
  }
  return summary("yielded");
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const controller = new AbortController();
  const stop = () => controller.abort(new Error("scheduler_stopped"));
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
  try {
    const result = await runScheduler(configuration(process.env), {
      checkOnly: process.argv.includes("--check"), signal: controller.signal,
    });
    if (result.requiresAttention || ["awaiting_activation", "retry"].includes(result.outcome)) process.exitCode = 1;
  } catch (error) {
    // Error text from fetch can include private configuration: only allow own stable codes.
    const code = error instanceof Error && /^(invalid_scheduler_|incompatible_scheduler_|scheduler_)[a-z_]+$/.test(error.message)
      ? error.message : "scheduler_failed";
    console.error(JSON.stringify({ protocolVersion: PROTOCOL_VERSION, error: code }));
    process.exitCode = 1;
  } finally {
    process.removeListener("SIGTERM", stop);
    process.removeListener("SIGINT", stop);
  }
}

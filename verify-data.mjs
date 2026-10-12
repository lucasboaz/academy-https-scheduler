import { configuration, PROTOCOL_VERSION } from "./run.mjs";

const MAX_AUDIT_AGE_MS = 48 * 60 * 60 * 1000;
const MAX_FUTURE_SKEW_MS = 5 * 60 * 1000;

function finish(verified, reason) {
  console.log(JSON.stringify({ verified, reason }));
  if (!verified) process.exitCode = 1;
}

try {
  const { origin, secret } = configuration(process.env);
  const response = await fetch(`${origin}/api/academy/scheduled/worker-status`, {
    method: "GET",
    redirect: "error",
    headers: {
      Authorization: `Bearer ${secret}`,
      "X-Academy-Scheduler-Version": PROTOCOL_VERSION,
    },
    signal: AbortSignal.timeout(20_000),
  });

  if (response.status === 401 || response.status === 403) {
    finish(false, "authentication_rejected");
  } else if (!response.ok) {
    finish(false, "status_unavailable");
  } else {
    const body = await response.json();
    const sweep = body?.observability?.currentSweep;
    if (body?.protocolVersion !== PROTOCOL_VERSION || !sweep) {
      finish(false, "invalid_status_response");
    } else if (sweep.status !== "completed") {
      finish(false, "latest_audit_not_verified");
    } else {
      const completedAt = Date.parse(sweep.completedAt);
      const age = Date.now() - completedAt;
      const counts = [
        [sweep.processedCourses, sweep.expectedCourses],
        [sweep.processedLearners, sweep.expectedLearners],
      ];
      if (!Number.isFinite(completedAt) || age < -MAX_FUTURE_SKEW_MS) {
        finish(false, "invalid_completion_time");
      } else if (age > MAX_AUDIT_AGE_MS) {
        finish(false, "verified_audit_stale");
      } else if (counts.some(([processed, expected]) =>
        !Number.isSafeInteger(processed) || !Number.isSafeInteger(expected) ||
        expected < 0 || processed < expected)) {
        finish(false, "coverage_unverified");
      } else {
        finish(true, "recent_complete_audit_with_coverage");
      }
    }
  }
} catch {
  finish(false, "verification_unavailable");
}

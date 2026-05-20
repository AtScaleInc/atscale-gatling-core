package com.atscale.java.stats;

import java.util.List;

/**
 * Fully-aggregated result of a single Gatling simulation run.
 * Designed as a self-contained unit for persistence in a datastore.
 * <P>
 * Child records:
 *   {@link RequestRecord}     — one row per raw request event
 *   {@link RequestGroupStats} — per-request-name rollup with response-time percentiles
 *   {@link ErrorSummary}      — unique error messages with counts and affected request names
 *   {@link ResponseTimeStats} — percentile breakdown (min/max/mean/p50/p75/p90/p95/p99)
 */
public record SimulationRun(
    String                  systemRunId,
    String                  gatlingVersion,
    String                  simulationClass,
    long                    runStartMs,
    long                    runEndMs,
    String                  description,
    String                  gatlingRunId,
    List<String>            scenarioNames,
    int                     totalRequests,
    int                     okCount,
    int                     koCount,
    int                     droppedCount,
    List<RequestGroupStats> requestGroups,
    List<ErrorSummary>      errors,
    List<RequestRecord>     requests
) {

    public record RequestRecord(
        String       requestName,
        List<String> groups,
        long         startMs,
        long         endMs,
        boolean      ok,
        String       errorMessage,
        boolean      dropped
    ) {
        public int responseTimeMs() {
            return dropped ? 0 : (int) (endMs - startMs);
        }
    }

    public record ResponseTimeStats(
        int min,
        int max,
        int mean,
        int p50,
        int p75,
        int p90,
        int p95,
        int p99
    ) {}

    public record RequestGroupStats(
        String            requestName,
        int               count,
        int               okCount,
        int               koCount,
        ResponseTimeStats responseTimes
    ) {}

    public record ErrorSummary(
        String       message,
        int          count,
        List<String> affectedRequests
    ) {}
}

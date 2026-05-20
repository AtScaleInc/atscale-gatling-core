package com.atscale.java.stats;

import com.atscale.java.executors.MavenTaskDto;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

/**
 * Parses Gatling 3.x binary simulation.log files into {@link SimulationRun} objects.
 * <P>
 * Entry points:
 *   {@link #parse(Path)}     — accepts a file, a run subdirectory, or a root reports directory;
 *                               always returns a list (single-element when a file or subdirectory is given)
 *   {@link #parseFile(Path)} — parse one simulation.log file directly
 * <P>
 * Format overview (big-endian throughout):
 * <P>
 *   RUN record (type byte = 0):
 *     writeString x5  : gatlingVersion, simulationClass, description, scenarioNames[]
 *     writeLong       : runStart (epoch ms)
 *     writeInt        : scenarioCount
 *     writeInt        : assertionCount
 *     writeByteBuffer : pickled assertion data (one per assertion, each length-prefixed)
 * <P>
 *   String encodings:
 *     writeString (RUN record only):
 *       [int: length][bytes][1-byte coder]  — no coder byte when length == 0
 *     writeCachedString (all other records):
 *       N == 0        &rarr; null/empty, return ""
 *       N &lt; 0        &rarr; cache hit at index abs(N)
 *       N &gt; 0        &rarr; first occurrence: cache index N, then [int: len][bytes][1-byte coder if len &gt; 0]
 * <P>
 *   REQUEST record (type byte = 1):
 *     [int: groupDepth][groupDepth x cached strings][cached: requestName]
 *     [int: startOffset ms][int: endOffset ms]
 *     [bool: ok][cached: errorMessage]
 *     endOffset == Integer.MIN_VALUE &rarr; request was never completed (dropped)
 * <P>
 *   USER record (type byte = 2):
 *     [int: scenarioIndex][bool: isStart][int: timestampOffset ms]
 * <P>
 *   GROUP record (type byte = 3):
 *     [int: groupDepth][groupDepth x cached strings]
 *     [int: startOffset][int: endOffset][int: cumulatedResponseTime][bool: ok]
 * <P>
 *   ERROR record (type byte = 4):
 *     [cached: message][int: timestampOffset ms]
 * <P>
 * Description field convention:
 *   A plain description string (e.g. "TPCDS JDBC Tests") is stored as-is.
 *   When an AtScale run ID is embedded, the format is:
 *     "&lt;gatlingRunId&gt; ||| &lt;description&gt;"
 *   The parser splits on " ||| " and populates {@link SimulationRun#gatlingRunId()} accordingly.
 */
public class SimulationLogParser {

    private static final int TYPE_RUN     = 0;
    private static final int TYPE_REQUEST = 1;
    private static final int TYPE_USER    = 2;
    private static final int TYPE_GROUP   = 3;
    private static final int TYPE_ERROR   = 4;

    private static final String LOG_FILENAME = "simulation.log";

    // Internal holder for RUN record fields
    private record RunInfo(
        String       gatlingVersion,
        String       simulationClass,
        long         runStartMs,
        String       description,
        List<String> scenarioNames
    ) {}

    // -------------------------------------------------------------------------
    // Parser state — one instance per file
    // -------------------------------------------------------------------------

    private final Map<Integer, String> stringCache = new HashMap<>();

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Parses one or more simulation.log files rooted at {@code path}.
     *
     * <ul>
     *   <li>If {@code path} is a {@code simulation.log} file — parses it directly.</li>
     *   <li>If {@code path} is a run subdirectory — finds {@code simulation.log} within it.</li>
     *   <li>If {@code path} is a reports root — finds all {@code simulation.log} files in
     *       immediate subdirectories (mirrors the Gatling reports/ layout).</li>
     * </ul>
     *
     * Results are sorted by path so ordering is deterministic.
     */
    public static List<SimulationRun> parse(Path path) throws IOException {
        if (Files.isRegularFile(path)) {
            return List.of(parseFile(path));
        }
        try (Stream<Path> stream = Files.walk(path)) {
            List<Path> logs = stream
                .filter(p -> Files.isRegularFile(p) && p.getFileName().toString().equals(LOG_FILENAME))
                .sorted()
                .toList();
            List<SimulationRun> results = new ArrayList<>(logs.size());
            for (Path log : logs) {
                results.add(parseFile(log));
            }
            return Collections.unmodifiableList(results);
        }
    }

    /** Parses a single simulation.log file and returns the aggregated {@link SimulationRun}. */
    public static SimulationRun parseFile(Path path) throws IOException {
        return new SimulationLogParser().doParse(path);
    }

    // -------------------------------------------------------------------------
    // Parse loop
    // -------------------------------------------------------------------------

    private SimulationRun doParse(Path path) throws IOException {
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(path), 65_536))) {

            RunInfo runInfo = parseRunRecord(in);
            List<SimulationRun.RequestRecord> requests = new ArrayList<>();

            while (true) {
                int type;
                try {
                    type = in.readByte() & 0xFF;
                } catch (EOFException e) {
                    break;
                }
                try {
                    switch (type) {
                        case TYPE_REQUEST -> requests.add(parseRequestRecord(in, runInfo.runStartMs()));
                        case TYPE_USER    -> skipUserRecord(in);
                        case TYPE_GROUP   -> skipGroupRecord(in);
                        case TYPE_ERROR   -> skipErrorRecord(in);
                        case 0xFF         -> { /* EOF sentinel */ }
                        default           -> throw new IOException(
                            "Unknown record type: 0x" + Integer.toHexString(type));
                    }
                } catch (EOFException e) {
                    // Partial record at end of an in-progress file — return what we have
                    break;
                }
                if (type == 0xFF) break;
            }

            return aggregate(path, runInfo, Collections.unmodifiableList(requests));
        }
    }

    // -------------------------------------------------------------------------
    // Aggregation
    // -------------------------------------------------------------------------

    private static SimulationRun aggregate(Path path, RunInfo runInfo,
                                           List<SimulationRun.RequestRecord> requests) {
        String gatlingRunId = (path.getParent() != null)
            ? path.getParent().getFileName().toString()
            : path.getFileName().toString();

        int total   = requests.size();
        int ok      = (int) requests.stream().filter(SimulationRun.RequestRecord::ok).count();
        int ko      = total - ok;
        int dropped = (int) requests.stream().filter(SimulationRun.RequestRecord::dropped).count();

        long runEndMs = requests.stream()
            .filter(r -> !r.dropped())
            .mapToLong(SimulationRun.RequestRecord::endMs)
            .max()
            .orElse(runInfo.runStartMs());

        // Per-request-name rollup preserving encounter order
        Map<String, List<SimulationRun.RequestRecord>> byName = new LinkedHashMap<>();
        for (SimulationRun.RequestRecord r : requests) {
            byName.computeIfAbsent(r.requestName(), k -> new ArrayList<>()).add(r);
        }
        List<SimulationRun.RequestGroupStats> groups = new ArrayList<>();
        for (Map.Entry<String, List<SimulationRun.RequestRecord>> e : byName.entrySet()) {
            List<SimulationRun.RequestRecord> recs = e.getValue();
            int cnt   = recs.size();
            int okCnt = (int) recs.stream().filter(SimulationRun.RequestRecord::ok).count();
            int[] times = recs.stream()
                .filter(r -> !r.dropped())
                .mapToInt(SimulationRun.RequestRecord::responseTimeMs)
                .sorted()
                .toArray();
            groups.add(new SimulationRun.RequestGroupStats(
                e.getKey(), cnt, okCnt, cnt - okCnt, computeStats(times)));
        }

        // Error summaries grouped by message
        Map<String, List<SimulationRun.RequestRecord>> koByMsg = new LinkedHashMap<>();
        for (SimulationRun.RequestRecord r : requests) {
            if (!r.ok() && !r.errorMessage().isEmpty()) {
                koByMsg.computeIfAbsent(r.errorMessage(), k -> new ArrayList<>()).add(r);
            }
        }
        List<SimulationRun.ErrorSummary> errors = new ArrayList<>();
        for (Map.Entry<String, List<SimulationRun.RequestRecord>> e : koByMsg.entrySet()) {
            List<String> affected = e.getValue().stream()
                .map(SimulationRun.RequestRecord::requestName)
                .distinct()
                .collect(Collectors.toList());
            errors.add(new SimulationRun.ErrorSummary(e.getKey(), e.getValue().size(), affected));
        }

        Map<String, String> parsed = MavenTaskDto.parseRunDescription(runInfo.description());
        String atscaleRunId = parsed.get("runId") != null ? parsed.get("runId") : "";
        String description  = parsed.get("description");

        return new SimulationRun(
            gatlingRunId,
            runInfo.gatlingVersion(),
            runInfo.simulationClass(),
            runInfo.runStartMs(),
            runEndMs,
            description,
            atscaleRunId,
            runInfo.scenarioNames(),
            total, ok, ko, dropped,
            Collections.unmodifiableList(groups),
            Collections.unmodifiableList(errors),
            requests
        );
    }

    private static SimulationRun.ResponseTimeStats computeStats(int[] sorted) {
        if (sorted.length == 0) {
            return new SimulationRun.ResponseTimeStats(0, 0, 0, 0, 0, 0, 0, 0);
        }
        int n    = sorted.length;
        int mean = (int) (Arrays.stream(sorted).asLongStream().sum() / n);
        return new SimulationRun.ResponseTimeStats(
            sorted[0], sorted[n - 1], mean,
            percentile(sorted, 50),
            percentile(sorted, 75),
            percentile(sorted, 90),
            percentile(sorted, 95),
            percentile(sorted, 99)
        );
    }

    private static int percentile(int[] sorted, int p) {
        int idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }

    // -------------------------------------------------------------------------
    // Record parsers
    // -------------------------------------------------------------------------

    private RunInfo parseRunRecord(DataInputStream in) throws IOException {
        int type = in.readByte() & 0xFF;
        if (type != TYPE_RUN) {
            throw new IOException("Expected RUN record (type 0), got type " + type);
        }

        String       gatlingVersion  = readString(in);
        String       simulationClass = readString(in);
        long         runStartMs      = in.readLong();
        String       description     = readString(in);
        int          scenarioCount   = in.readInt();
        List<String> scenarioNames   = new ArrayList<>(scenarioCount);
        for (int i = 0; i < scenarioCount; i++) {
            scenarioNames.add(readString(in));
        }
        int assertionCount = in.readInt();
        for (int i = 0; i < assertionCount; i++) {
            int len = in.readInt();
            in.skipBytes(len);
        }

        return new RunInfo(gatlingVersion, simulationClass, runStartMs, description,
                Collections.unmodifiableList(scenarioNames));
    }

    private SimulationRun.RequestRecord parseRequestRecord(DataInputStream in,
                                                           long runStartMs) throws IOException {
        int          groupDepth = in.readInt();
        List<String> groups     = new ArrayList<>(groupDepth);
        for (int i = 0; i < groupDepth; i++) {
            groups.add(readCachedString(in));
        }

        String  requestName = readCachedString(in);
        int     startOffset = in.readInt();
        int     endOffset   = in.readInt();
        boolean ok          = in.readBoolean();
        String  errorMsg    = readCachedString(in);

        boolean isDropped = (endOffset == Integer.MIN_VALUE);
        long    startMs   = runStartMs + startOffset;
        long    endMs     = isDropped ? startMs : runStartMs + endOffset;

        return new SimulationRun.RequestRecord(
            requestName, Collections.unmodifiableList(groups),
            startMs, endMs, ok, errorMsg, isDropped);
    }

    private void skipUserRecord(DataInputStream in) throws IOException {
        in.readInt();
        in.readBoolean();
        in.readInt();
    }

    private void skipGroupRecord(DataInputStream in) throws IOException {
        int depth = in.readInt();
        for (int i = 0; i < depth; i++) readCachedString(in);
        in.readInt();
        in.readInt();
        in.readInt();
        in.readBoolean();
    }

    private void skipErrorRecord(DataInputStream in) throws IOException {
        readCachedString(in);
        in.readInt();
    }

    // -------------------------------------------------------------------------
    // String readers
    // -------------------------------------------------------------------------

    /**
     * Non-cached string used in RUN record:
     *   [4B int: length][bytes][1B coder]
     * No coder byte when length == 0.
     */
    private String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length == 0) return "";
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        in.readByte(); // coder: 0 = LATIN1, 1 = UTF16
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Cached string used in REQUEST, USER, GROUP, ERROR records:
     *   N == 0   &rarr; null/empty sentinel &rarr; ""
     *   N &lt; 0   &rarr; cache hit at index abs(N)
     *   N &gt; 0   &rarr; first occurrence, cache index N:
     *                [4B int: length][bytes][1B coder if length &gt; 0]
     */
    private String readCachedString(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n == 0) return "";
        if (n < 0)  return stringCache.getOrDefault(-n, "");

        int length = in.readInt();
        String s;
        if (length == 0) {
            s = "";
        } else {
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            in.readByte(); // coder byte
            s = new String(bytes, StandardCharsets.UTF_8);
        }
        stringCache.put(n, s);
        return s;
    }
}

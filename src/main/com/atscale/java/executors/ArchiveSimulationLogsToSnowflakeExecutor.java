package com.atscale.java.executors;

import com.atscale.java.stats.SimulationLogParser;
import com.atscale.java.stats.SimulationRun;
import com.atscale.java.utils.AdditionalPropertiesLoader;
import com.atscale.java.utils.PropertiesManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

/**
 * Parses Gatling simulation.log files and uploads their contents to Snowflake.
 * <P>
 * Required argument:
 *   --simulation_log=&lt;path&gt;  Path to a simulation.log file, a run subdirectory, or a
 *                             Gatling results root containing multiple run subdirectories.
 *                             {@link SimulationLogParser#parse(Path)} handles all three forms.
 * <P>
 * Optional argument:
 *   --environment=&lt;name&gt;     Freeform label stored in GATLING_SIM_RUNS.ENVIRONMENT
 *                             (e.g. "staging", "prod"). Defaults to empty.
 * <P>
 * Tables created (if absent) and populated:
 * <P>
 *   GATLING_SIM_RUNS            — one row per simulation.log file; dimension table that ties
 *                                 all data together.  GATLING_RUN_ID joins to the existing
 *                                 GATLING_SQL_HEADERS / GATLING_XMLA_HEADERS tables.
 * <P>
 *   GATLING_SIM_REQUEST_STATS   — per-(run, request-name) rollup: counts + response-time
 *                                 percentiles (min/max/mean/p50/p75/p90/p95/p99).
 * <P>
 *   GATLING_SIM_REQUEST_EVENTS  — one row per raw request event (startMs, endMs, ok, error …).
 * <P>
 *   GATLING_SIM_ERRORS          — aggregated error summaries per run (message, count,
 *                                 affected request names).
 * <P>
 * Idempotency:  before inserting, all existing rows for each SYSTEM_RUN_ID are deleted so
 * the executor can be re-run safely without duplicating data.
 * <P>
 * Transactions:  DDL (CREATE TABLE IF NOT EXISTS) is committed separately.  All DML across
 * all runs in a single invocation is wrapped in one transaction — everything commits or
 * everything rolls back.
 */
public class ArchiveSimulationLogsToSnowflakeExecutor {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(ArchiveSimulationLogsToSnowflakeExecutor.class);

    private static final int BATCH_SIZE = 1000;

    static {
        com.atscale.java.utils.Log4jShutdown.installHook();
    }

    // -------------------------------------------------------------------------
    // Entry point
    // -------------------------------------------------------------------------

    public static void main(String[] args) {
        LOGGER.info("ArchiveSimulationLogsToSnowflakeExecutor started.");
        try {
            LOGGER.info("Command-line arguments {}.", Arrays.toString(args));
            Map<String, String> arguments = parseArgs(args);
            LOGGER.info("Parsed arguments: {}", arguments);

            if (!arguments.containsKey("simulation_log")) {
                LOGGER.error("Missing required argument: --simulation_log=<path_to_simulation_log_or_directory>");
                throw new IllegalArgumentException(
                    "Missing required argument: --simulation_log=<path_to_simulation_log_or_directory>");
            }

            Path   simulationLogPath = Path.of(arguments.get("simulation_log"));
            String environment       = arguments.getOrDefault("environment", "");

            ArchiveSimulationLogsToSnowflakeExecutor executor =
                new ArchiveSimulationLogsToSnowflakeExecutor();
            executor.initAdditionalProperties();
            executor.execute(simulationLogPath, environment);

        } catch (Exception e) {
            LOGGER.error("Error during ArchiveSimulationLogsToSnowflakeExecutor execution", e);
            throw new RuntimeException("ArchiveSimulationLogsToSnowflakeExecutor failed", e);
        }
        LOGGER.info("ArchiveSimulationLogsToSnowflakeExecutor completed.");
        try {
            Thread.sleep(java.time.Duration.ofSeconds(10).toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    // -------------------------------------------------------------------------
    // Core execution
    // -------------------------------------------------------------------------

    protected void execute(Path simulationLogPath, String environment) {
        List<SimulationRun> runs;
        try {
            runs = SimulationLogParser.parse(simulationLogPath);
        } catch (IOException e) {
            throw new RuntimeException(
                "Failed to parse simulation log(s) at " + simulationLogPath, e);
        }

        LOGGER.info("Parsed {} simulation run(s) from {}.", runs.size(), simulationLogPath);
        if (runs.isEmpty()) {
            LOGGER.warn("No simulation.log files found at {}. Nothing to upload.", simulationLogPath);
            return;
        }

        String     jdbcUrl         = getSnowflakeURL();
        Properties connectionProps = getConnectionProperties();

        LOGGER.info("Connecting to Snowflake with URL: {}", jdbcUrl);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps)) {
            LOGGER.info("Connected to Snowflake successfully.");
            boolean originalAutoCommit = conn.getAutoCommit();

            // DDL committed separately — Snowflake auto-commits DDL statements
            try {
                createIfNotExistsObjects(conn);
                conn.commit();
            } catch (SQLException e) {
                try { conn.rollback(); } catch (SQLException rbEx) {
                    LOGGER.error("Rollback failed after DDL error: {}", rbEx.getMessage());
                }
                throw e;
            }

            // Single DML transaction: all runs commit or all roll back
            try {
                conn.setAutoCommit(false);

                for (SimulationRun run : runs) {
                    LOGGER.info("Uploading simulation run: systemRunId={}, gatlingRunId={}, requests={}",
                        run.systemRunId(), run.gatlingRunId(), run.totalRequests());

                    deleteExistingForRun(conn, run.systemRunId());
                    insertSimRun(conn, run, environment, simulationLogPath.toAbsolutePath().toString());
                    insertRequestStats(conn, run);
                    insertRequestEvents(conn, run);
                    insertErrors(conn, run);

                    LOGGER.info("Staged all rows for systemRunId={}.", run.systemRunId());
                }

                conn.commit();
                LOGGER.info("✅ Committed {} simulation run(s) to Snowflake.", runs.size());

            } catch (SQLException e) {
                try {
                    if (!conn.getAutoCommit()) conn.rollback();
                    LOGGER.info("Transaction rolled back.");
                } catch (SQLException rbEx) {
                    LOGGER.error("Rollback failed: {}", rbEx.getMessage());
                }
                throw e;
            } finally {
                try {
                    conn.setAutoCommit(originalAutoCommit);
                } catch (SQLException ex) {
                    LOGGER.warn("Failed to restore auto-commit: {}", ex.getMessage());
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute Snowflake operations", e);
        }

        LOGGER.info("Processed {} simulation run(s) from {}.", runs.size(), simulationLogPath);
    }

    // -------------------------------------------------------------------------
    // DDL — create tables if not present
    // -------------------------------------------------------------------------

    private static void createIfNotExistsObjects(Connection conn) throws SQLException {
        LOGGER.info("Ensuring all required simulation-log Snowflake objects exist...");

        String db     = PropertiesManager.getCustomProperty("snowflake.archive.database");
        String schema = PropertiesManager.getCustomProperty("snowflake.archive.schema");
        exec(conn, "USE DATABASE " + db + ";");
        exec(conn, "USE SCHEMA "   + schema + ";");

        // Dimension table — one row per simulation.log file.
        // GATLING_RUN_ID is the foreign key linking to GATLING_SQL_HEADERS / GATLING_XMLA_HEADERS.
        // SYSTEM_RUN_ID is the unique Gatling run-directory name and the idempotency key.
        exec(conn, """
            CREATE TABLE IF NOT EXISTS GATLING_SIM_RUNS (
              SYSTEM_RUN_ID    VARCHAR(512)     NOT NULL,
              GATLING_RUN_ID   VARCHAR(512),
              GATLING_VERSION  VARCHAR(50),
              SIMULATION_CLASS VARCHAR(512),
              RUN_START_TS     TIMESTAMP_NTZ(9),
              RUN_END_TS       TIMESTAMP_NTZ(9),
              RUN_START_MS     NUMBER(19,0),
              RUN_END_MS       NUMBER(19,0),
              DURATION_MS      NUMBER(19,0),
              DESCRIPTION      VARCHAR(4096),
              SCENARIO_NAMES   VARCHAR(4096),
              TOTAL_REQUESTS   NUMBER(10,0),
              OK_COUNT         NUMBER(10,0),
              KO_COUNT         NUMBER(10,0),
              DROPPED_COUNT    NUMBER(10,0),
              ENVIRONMENT      VARCHAR(256),
              SRC_FILE_PATH    VARCHAR(4096),
              LOADED_AT        TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
            );
            """);

        // Per-request-name rollup: one row per (SYSTEM_RUN_ID, REQUEST_NAME).
        exec(conn, """
            CREATE TABLE IF NOT EXISTS GATLING_SIM_REQUEST_STATS CLUSTER BY (GATLING_RUN_ID) (
              SYSTEM_RUN_ID  VARCHAR(512) NOT NULL,
              GATLING_RUN_ID VARCHAR(512),
              REQUEST_NAME   VARCHAR(512),
              TOTAL_COUNT    NUMBER(10,0),
              OK_COUNT       NUMBER(10,0),
              KO_COUNT       NUMBER(10,0),
              RESP_MIN_MS    NUMBER(10,0),
              RESP_MAX_MS    NUMBER(10,0),
              RESP_MEAN_MS   NUMBER(10,0),
              RESP_P50_MS    NUMBER(10,0),
              RESP_P75_MS    NUMBER(10,0),
              RESP_P90_MS    NUMBER(10,0),
              RESP_P95_MS    NUMBER(10,0),
              RESP_P99_MS    NUMBER(10,0)
            );
            """);

        // Individual request events: one row per REQUEST record in the simulation.log.
        exec(conn, """
            CREATE TABLE IF NOT EXISTS GATLING_SIM_REQUEST_EVENTS CLUSTER BY (GATLING_RUN_ID) (
              SYSTEM_RUN_ID    VARCHAR(512) NOT NULL,
              GATLING_RUN_ID   VARCHAR(512),
              REQUEST_NAME     VARCHAR(512),
              GROUPS           VARCHAR(4096),
              START_TS         TIMESTAMP_NTZ(9),
              END_TS           TIMESTAMP_NTZ(9),
              START_MS         NUMBER(19,0),
              END_MS           NUMBER(19,0),
              RESPONSE_TIME_MS NUMBER(10,0),
              OK               BOOLEAN,
              ERROR_MESSAGE    VARCHAR(16777216),
              DROPPED          BOOLEAN
            );
            """);

        // Aggregated error summaries: one row per unique error message per run.
        exec(conn, """
            CREATE TABLE IF NOT EXISTS GATLING_SIM_ERRORS CLUSTER BY (GATLING_RUN_ID) (
              SYSTEM_RUN_ID     VARCHAR(512) NOT NULL,
              GATLING_RUN_ID    VARCHAR(512),
              ERROR_MESSAGE     VARCHAR(16777216),
              ERROR_COUNT       NUMBER(10,0),
              AFFECTED_REQUESTS VARCHAR(16777216)
            );
            """);

        LOGGER.info("✅ All required simulation-log Snowflake objects verified.");
    }

    // -------------------------------------------------------------------------
    // DML — idempotency: remove any prior data for this run directory
    // -------------------------------------------------------------------------

    private static void deleteExistingForRun(Connection conn, String systemRunId) throws SQLException {
        // Delete child tables first to respect logical ordering (no FK enforcement in Snowflake,
        // but ordering makes intent clear).
        String[] tables = {
            "GATLING_SIM_ERRORS",
            "GATLING_SIM_REQUEST_EVENTS",
            "GATLING_SIM_REQUEST_STATS",
            "GATLING_SIM_RUNS"
        };
        for (String table : tables) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM " + table + " WHERE SYSTEM_RUN_ID = ?")) {
                ps.setString(1, systemRunId);
                int deleted = ps.executeUpdate();
                if (deleted > 0) {
                    LOGGER.info("Deleted {} existing row(s) from {} for systemRunId={}",
                        deleted, table, systemRunId);
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // DML — inserts
    // -------------------------------------------------------------------------

    private static void insertSimRun(Connection conn, SimulationRun run,
                                     String environment, String srcFilePath) throws SQLException {
        final String sql = """
            INSERT INTO GATLING_SIM_RUNS (
              SYSTEM_RUN_ID, GATLING_RUN_ID, GATLING_VERSION, SIMULATION_CLASS,
              RUN_START_TS, RUN_END_TS, RUN_START_MS, RUN_END_MS, DURATION_MS,
              DESCRIPTION, SCENARIO_NAMES,
              TOTAL_REQUESTS, OK_COUNT, KO_COUNT, DROPPED_COUNT,
              ENVIRONMENT, SRC_FILE_PATH
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(   1, run.systemRunId());
            ps.setString(   2, nullIfBlank(run.gatlingRunId()));
            ps.setString(   3, run.gatlingVersion());
            ps.setString(   4, run.simulationClass());
            ps.setTimestamp(5, new Timestamp(run.runStartMs()));
            ps.setTimestamp(6, new Timestamp(run.runEndMs()));
            ps.setLong(     7, run.runStartMs());
            ps.setLong(     8, run.runEndMs());
            ps.setLong(     9, run.runEndMs() - run.runStartMs());
            ps.setString(  10, run.description());
            ps.setString(  11, toJson(run.scenarioNames()));
            ps.setInt(     12, run.totalRequests());
            ps.setInt(     13, run.okCount());
            ps.setInt(     14, run.koCount());
            ps.setInt(     15, run.droppedCount());
            ps.setString(  16, nullIfBlank(environment));
            ps.setString(  17, srcFilePath);
            ps.executeUpdate();
        }
        LOGGER.info("Inserted GATLING_SIM_RUNS row for systemRunId={}.", run.systemRunId());
    }

    private static void insertRequestStats(Connection conn, SimulationRun run) throws SQLException {
        if (run.requestGroups().isEmpty()) return;
        final String sql = """
            INSERT INTO GATLING_SIM_REQUEST_STATS (
              SYSTEM_RUN_ID, GATLING_RUN_ID, REQUEST_NAME,
              TOTAL_COUNT, OK_COUNT, KO_COUNT,
              RESP_MIN_MS, RESP_MAX_MS, RESP_MEAN_MS,
              RESP_P50_MS, RESP_P75_MS, RESP_P90_MS, RESP_P95_MS, RESP_P99_MS
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int count = 0;
            for (SimulationRun.RequestGroupStats stats : run.requestGroups()) {
                SimulationRun.ResponseTimeStats rt = stats.responseTimes();
                ps.setString(1,  run.systemRunId());
                ps.setString(2,  nullIfBlank(run.gatlingRunId()));
                ps.setString(3,  stats.requestName());
                ps.setInt(   4,  stats.count());
                ps.setInt(   5,  stats.okCount());
                ps.setInt(   6,  stats.koCount());
                ps.setInt(   7,  rt.min());
                ps.setInt(   8,  rt.max());
                ps.setInt(   9,  rt.mean());
                ps.setInt(   10, rt.p50());
                ps.setInt(   11, rt.p75());
                ps.setInt(   12, rt.p90());
                ps.setInt(   13, rt.p95());
                ps.setInt(   14, rt.p99());
                ps.addBatch();
                if (++count % BATCH_SIZE == 0) ps.executeBatch();
            }
            if (count % BATCH_SIZE != 0) ps.executeBatch();
            LOGGER.info("Inserted {} request stat row(s) for systemRunId={}.",
                count, run.systemRunId());
        }
    }

    private static void insertRequestEvents(Connection conn, SimulationRun run) throws SQLException {
        if (run.requests().isEmpty()) return;
        final String sql = """
            INSERT INTO GATLING_SIM_REQUEST_EVENTS (
              SYSTEM_RUN_ID, GATLING_RUN_ID, REQUEST_NAME, GROUPS,
              START_TS, END_TS, START_MS, END_MS, RESPONSE_TIME_MS,
              OK, ERROR_MESSAGE, DROPPED
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int count = 0;
            for (SimulationRun.RequestRecord req : run.requests()) {
                ps.setString(   1,  run.systemRunId());
                ps.setString(   2,  nullIfBlank(run.gatlingRunId()));
                ps.setString(   3,  req.requestName());
                ps.setString(   4,  toJson(req.groups()));
                ps.setTimestamp(5,  new Timestamp(req.startMs()));
                ps.setTimestamp(6,  new Timestamp(req.endMs()));
                ps.setLong(     7,  req.startMs());
                ps.setLong(     8,  req.endMs());
                ps.setInt(      9,  req.responseTimeMs());
                ps.setBoolean(  10, req.ok());
                ps.setString(   11, nullIfBlank(req.errorMessage()));
                ps.setBoolean(  12, req.dropped());
                ps.addBatch();
                if (++count % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    LOGGER.debug("Flushed request-events batch (count={}) for systemRunId={}",
                        count, run.systemRunId());
                }
            }
            if (count % BATCH_SIZE != 0) ps.executeBatch();
            LOGGER.info("Inserted {} request event row(s) for systemRunId={}.",
                count, run.systemRunId());
        }
    }

    private static void insertErrors(Connection conn, SimulationRun run) throws SQLException {
        if (run.errors().isEmpty()) return;
        final String sql = """
            INSERT INTO GATLING_SIM_ERRORS (
              SYSTEM_RUN_ID, GATLING_RUN_ID, ERROR_MESSAGE, ERROR_COUNT, AFFECTED_REQUESTS
            ) VALUES (?, ?, ?, ?, ?)
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int count = 0;
            for (SimulationRun.ErrorSummary err : run.errors()) {
                ps.setString(1, run.systemRunId());
                ps.setString(2, nullIfBlank(run.gatlingRunId()));
                ps.setString(3, err.message());
                ps.setInt(   4, err.count());
                ps.setString(5, toJson(err.affectedRequests()));
                ps.addBatch();
                if (++count % BATCH_SIZE == 0) ps.executeBatch();
            }
            if (count % BATCH_SIZE != 0) ps.executeBatch();
            LOGGER.info("Inserted {} error row(s) for systemRunId={}.", count, run.systemRunId());
        }
    }

    // -------------------------------------------------------------------------
    // Connection helpers — identical pattern to other Archive executors
    // -------------------------------------------------------------------------

    private Properties getConnectionProperties() {
        String user      = PropertiesManager.getCustomProperty("snowflake.archive.username");
        String password  = PropertiesManager.getCustomProperty("snowflake.archive.password");
        String warehouse = PropertiesManager.getCustomProperty("snowflake.archive.warehouse");
        String database  = PropertiesManager.getCustomProperty("snowflake.archive.database");
        String schema    = PropertiesManager.getCustomProperty("snowflake.archive.schema");

        String privateKeyFile = null;
        String privateKeyPwd  = null;
        if (PropertiesManager.hasProperty("snowflake.archive.keyfile.path") &&
                PropertiesManager.hasProperty("snowflake.archive.keyfile.password")) {
            privateKeyFile = PropertiesManager.getCustomProperty("snowflake.archive.keyfile.path");
            privateKeyPwd  = PropertiesManager.getCustomProperty("snowflake.archive.keyfile.password");
        } else {
            LOGGER.warn("No private key file/password found for " +
                "snowflake.archive.keyfile.path / snowflake.archive.keyfile.password.");
        }

        String role = null;
        if (PropertiesManager.hasProperty("snowflake.archive.role")) {
            role = PropertiesManager.getCustomProperty("snowflake.archive.role");
        }

        user     = StringUtils.isNotEmpty(user)     ? user.trim()     : user;
        password = StringUtils.isNotEmpty(password) ? password.trim() : password;

        Properties props = new Properties();
        props.put("user",      user);
        props.put("password",  password);
        props.put("warehouse", warehouse);
        props.put("db",        database);
        props.put("schema",    schema);
        if (privateKeyFile != null) props.put("private_key_file",     privateKeyFile);
        if (privateKeyPwd  != null) props.put("private_key_file_pwd", privateKeyPwd);
        if (StringUtils.isNotBlank(role)) props.put("role", role);

        if (LOGGER.isDebugEnabled()) {
            int    pwLen  = StringUtils.isEmpty(password) ? 0 : password.length();
            String masked = pwLen > 0 ? ("***" + pwLen + "chars**") : "(empty)";
            LOGGER.debug("Snowflake connection: user='{}', password={}", user, masked);
        }
        return props;
    }

    private String getSnowflakeURL() {
        String account = PropertiesManager.getCustomProperty("snowflake.archive.account");
        return String.format("jdbc:snowflake://%s.snowflakecomputing.com/", account);
    }

    protected void initAdditionalProperties() {
        AdditionalPropertiesLoader loader = new AdditionalPropertiesLoader();
        PropertiesManager.setCustomProperties(
            loader.fetchAdditionalProperties(AdditionalPropertiesLoader.SecretsManagerType.AWS));
    }

    private static void exec(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    protected static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        try {
            for (String a : args) {
                if (a.startsWith("--") && a.contains("=")) {
                    int i = a.indexOf('=');
                    m.put(a.substring(2, i).toLowerCase(Locale.ROOT), a.substring(i + 1));
                } else if (a.contains("=")) {
                    int i = a.indexOf('=');
                    m.put(a.substring(0, i).toLowerCase(Locale.ROOT), a.substring(i + 1));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to parse arguments. Expected --key=value format.", e);
        }
        return m;
    }

    // -------------------------------------------------------------------------
    // Utilities
    // -------------------------------------------------------------------------

    private static String nullIfBlank(String s) {
        return StringUtils.isBlank(s) ? null : s;
    }

    /** Serialise a string list to a compact JSON array without pulling in a full JSON library. */
    private static String toJson(List<String> list) {
        if (list == null || list.isEmpty()) return "[]";
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("\"")
              .append(list.get(i).replace("\\", "\\\\").replace("\"", "\\\""))
              .append("\"");
        }
        sb.append("]");
        return sb.toString();
    }
}

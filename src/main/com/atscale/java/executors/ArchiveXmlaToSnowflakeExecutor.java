package com.atscale.java.executors;

import com.atscale.java.utils.PropertiesManager;
import com.atscale.java.utils.AdditionalPropertiesLoader;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.atscale.java.utils.RunLogUtils;

import java.nio.file.Path;
import java.sql.*;
import java.time.Duration;
import java.util.*;

public class ArchiveXmlaToSnowflakeExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveXmlaToSnowflakeExecutor.class);
    private static final String STAGE = "XMLA_LOGS_STAGE";
    private static final String RAW_TABLE = "GATLING_RAW_XMLA_LOGS";

    public static void main(String[] args) {
        LOGGER.info("ArchiveXmlaToSnowflakeExecutor started.");
        try {
            Map<String, String> arguments = parseArgs(args);
            Path dataFile = Path.of(arguments.get("data_file"));

            ArchiveXmlaToSnowflakeExecutor executor = new ArchiveXmlaToSnowflakeExecutor();
            executor.initAdditionalProperties();
            executor.execute(dataFile);
        } catch (Exception e) {
            LOGGER.error("Error during ArchiveXmlaToSnowflakeExecutor execution", e);
            throw new RuntimeException("ArchiveXmlaToSnowflakeExecutor failed", e);
        }
        LOGGER.info("ArchiveXmlaToSnowflakeExecutor completed.");
        try {
            Thread.sleep(Duration.ofSeconds(10).toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        org.apache.logging.log4j.LogManager.shutdown();
    }

    protected void execute(Path dataFile) {
        String jdbcUrl = getSnowflakeURL();
        Properties connectionProps = getConnectionProperties();

        List<String> runIds = RunLogUtils.extractGatlingRunIds(dataFile);
        LOGGER.info("Found {} unique XMLA RUN IDs in log file {}:: {}.", runIds.size(), dataFile, runIds);

        LOGGER.info("Connecting to Snowflake with URL: {}", jdbcUrl);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps)) {
            LOGGER.info("Connected to Snowflake successfully.");
            boolean originalAutoCommit = conn.getAutoCommit();

            String stagedFileName = String.format("%s.%s",
                    dataFile.getFileName().toString().replace("'", "''"), "gz");
            String fileUri = dataFile.toUri().toString().replace("'", "''");

            try {
                try {
                    // 0) Ensure required objects exist. DDL may be best committed separately.
                    createIfNotExistsObjects(conn);
                    conn.commit();
                } catch (SQLException e) {
                    conn.rollback();
                    throw e;
                }

                // 1) Upload local file to stage (PUT is not transactional)
                exec(conn, "PUT '" + fileUri + "' @" + STAGE + " AUTO_COMPRESS=TRUE OVERWRITE=TRUE");
                LOGGER.info("Uploaded file {} to stage {} as {}", fileUri, STAGE, stagedFileName);

                // Begin DML transaction: COPY + INSERTs should be atomic together
                conn.setAutoCommit(false);


                // 2) Clean up any prior data for the same run IDs (idempotency step)
                if (!runIds.isEmpty()) {
                    try (PreparedStatement ps = conn.prepareStatement(getCleanRawLogsForRunIdSql())) {
                        final int batchSize = 1000;
                        int count = 0;
                        for (String runId : runIds) {
                            String pattern = "%gatlingRunId='" + runId + "'%";
                            ps.setString(1, pattern);
                            ps.addBatch();
                            if (++count % batchSize == 0) {
                                ps.executeBatch();
                                LOGGER.debug("Deleted raw log batch of {} runIds", batchSize);
                            }
                        }
                        if (count % batchSize != 0) {
                            ps.executeBatch();
                            LOGGER.debug("Deleted final raw log batch of {} runIds", count % batchSize);
                        }
                    }
                }


                // 3) COPY into RAW table (fail the whole transaction on any load error)
                // This part cannot be made idempotent
                exec(conn, getCopyIntoRawSql(stagedFileName));
                LOGGER.info("Copied data from stage {} into table {}", STAGE, RAW_TABLE);


                // 4) Copy into the HEADERS table this step is idempotent
                if (!runIds.isEmpty()) {
                    try (PreparedStatement ps = conn.prepareStatement(getInsertIntoHeadersSql())) {
                        final int batchSize = 1000;
                        int count = 0;
                        for (String runId : runIds) {
                            String pattern = "%gatlingRunId='" + runId + "'%";
                            ps.setString(1, pattern);
                            ps.setString(2, runId);
                            ps.addBatch();
                            if (++count % batchSize == 0) {
                                ps.executeBatch();
                                LOGGER.debug("Inserted header batch of {} runIds", batchSize);
                            }
                        }
                        if (count % batchSize != 0) {
                            ps.executeBatch();
                            LOGGER.debug("Inserted final header batch of {} runIds", count % batchSize);
                        }
                    }
                    LOGGER.info("Inserted header rows into GATLING_XMLA_HEADERS from gatling_raw_xmla_logs");
                }


                // 5) Copy into the RESPONSES table this step is idempotent
                if (!runIds.isEmpty()) {
                    try (PreparedStatement ps = conn.prepareStatement(getInsertIntoResponsesSql())) {
                        final int batchSize = 1000;
                        int count = 0;
                        for (String runId : runIds) {
                            ps.setString(1, runId);
                            ps.setString(2, runId);
                            ps.addBatch();
                            if (++count % batchSize == 0) {
                                ps.executeBatch();
                                LOGGER.debug("Inserted responses batch of {} runIds", batchSize);
                            }
                        }
                        if (count % batchSize != 0) {
                            ps.executeBatch();
                            LOGGER.debug("Inserted final responses batch of {} runIds", count % batchSize);
                        }
                    }
                    LOGGER.info("Inserted response rows into gatling_xmla_responses");
                }




                // cleanup staged file
                try {
                    exec(conn, "REMOVE @" + STAGE + "/" + stagedFileName);
                } catch (SQLException cleanupEx) {
                    LOGGER.warn("Failed to remove staged file {}: {}", stagedFileName, cleanupEx.getMessage());
                }

                LOGGER.info("✅ XMLA Load complete.");
            } catch (SQLException e) {
                try {
                    if (!conn.getAutoCommit()) conn.rollback();
                } catch (SQLException rbEx) {
                    LOGGER.error("Rollback failed: {}", rbEx.getMessage());
                }
                try {
                    exec(conn, "REMOVE @" + STAGE + "/" + stagedFileName);
                } catch (SQLException cleanupEx) {
                    LOGGER.warn("Failed to remove staged file after rollback {}: {}", stagedFileName, cleanupEx.getMessage());
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
        LOGGER.info("Processed {} XMLA RUN IDs in log file {}:: {}.", runIds.size(), dataFile, runIds);

    }

    /** Create stage, file format, and XMLA tables. */
    private static void createIfNotExistsObjects(Connection conn) throws SQLException {
        LOGGER.info("Ensuring all required Snowflake objects for XMLA exist...");

        exec(conn, """
            CREATE STAGE IF NOT EXISTS XMLA_LOGS_STAGE
              FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '\\t');
            """);

        exec(conn, """
            CREATE FILE FORMAT IF NOT EXISTS XMLA_WHOLE_LINE_FMT
              TYPE = 'CSV'
              FIELD_DELIMITER = '\\t'
              SKIP_HEADER = 0
              TRIM_SPACE = FALSE
              FIELD_OPTIONALLY_ENCLOSED_BY = NONE
              EMPTY_FIELD_AS_NULL = FALSE
              NULL_IF = ();
            """);

        exec(conn, """
            CREATE TABLE IF NOT EXISTS GATLING_RAW_XMLA_LOGS (
              RAW_SOAP VARCHAR(16777216),
              SRC_FILENAME VARCHAR(16777216),
              SRC_ROW_NUMBER NUMBER(38,0)
            );
            """);

        // Replaced XMLA_QUERIES with GATLING_XMLA_HEADERS (breakout a=b pairs into columns)
        exec(conn, """
            CREATE TABLE IF NOT EXISTS GATLING_XMLA_HEADERS CLUSTER BY (GATLING_RUN_ID) (
              RUN_KEY NUMBER(19,0),
              TS TIMESTAMP_NTZ(9),
              LEVEL VARCHAR(30),
              LOGGER VARCHAR(100),
              MESSAGE_KIND VARCHAR(100),
              GATLING_RUN_ID VARCHAR(512) NOT NULL,
              STATUS VARCHAR(12),
              GATLING_SESSION_ID NUMBER(8,0),
              MODEL VARCHAR(1024),
              CUBE VARCHAR(1024),
              CATALOG VARCHAR(1024),
              QUERY_NAME VARCHAR(1024),
              ATSCALE_QUERY_ID VARCHAR(256),
              QUERY_HASH VARCHAR(256),
              START_MS NUMBER(38,0),
              END_MS NUMBER(38,0),
              DURATION_MS NUMBER(38,0),
              RESPONSE_SIZE NUMBER(38,0),
              RESPONSE_HASH VARCHAR(256),
              RAW_SOAP VARCHAR(16777216),
              PRIMARY KEY (RUN_KEY)
            );
            """);

        exec(conn, """
            CREATE TABLE IF NOT EXISTS GATLING_XMLA_RESPONSES CLUSTER BY (GATLING_RUN_ID)(
              RUN_KEY NUMBER(19,0),
              GATLING_RUN_ID VARCHAR(512),
              STATUS VARCHAR(12),
              GATLING_SESSION_ID NUMBER(8,0),
              MODEL VARCHAR(1024),
              CUBE VARCHAR(1024),
              CATALOG VARCHAR(1024),
              QUERY_NAME VARCHAR(1024),
              ATSCALE_QUERY_ID VARCHAR(256),
              QUERY_HASH VARCHAR(256),
              RESPONSE_HASH VARCHAR(256),
              SOAP_HEADER VARIANT,
              SOAP_BODY VARIANT,
              SOAP_BODY_HASH VARCHAR(256),
              PRIMARY KEY (RUN_KEY)
            );
            """);

        LOGGER.info("✅ All required XMLA Snowflake schema objects verified.");
    }

    private static String getCopyIntoRawSql(String fileName) {
        return String.format("""
              COPY INTO GATLING_RAW_XMLA_LOGS (RAW_SOAP, SRC_FILENAME, SRC_ROW_NUMBER)
              FROM (
                SELECT
                  $1 AS RAW_SOAP,
                  METADATA$FILENAME AS SRC_FILENAME,
                  METADATA$FILE_ROW_NUMBER AS SRC_ROW_NUMBER
                FROM @XMLA_LOGS_STAGE
              )
              FILES = ('%s')
              FILE_FORMAT = (FORMAT_NAME = XMLA_WHOLE_LINE_FMT)
              PURGE=TRUE
              ON_ERROR = 'ABORT_STATEMENT';
            """, fileName);
    }

    private static String getCleanRawLogsForRunIdSql() {
        return """
            DELETE FROM GATLING_RAW_XMLA_LOGS
            WHERE RAW_SOAP LIKE ?
            """;
    }

    /** Server-side SQL to extract key\=value pairs from RAW_SOAP and insert one row per gatlingRunId. */
    private static String getInsertIntoHeadersSql() {
    return """
            INSERT INTO GATLING_XMLA_HEADERS (
                    -- List all destination columns explicitly
                    RUN_KEY,
                    TS,
                    LEVEL,
                    LOGGER,
                    MESSAGE_KIND,
                    GATLING_RUN_ID,
                    STATUS,
                    GATLING_SESSION_ID,
                    MODEL,
                    CUBE,
                    CATALOG,
                    QUERY_NAME,
                    ATSCALE_QUERY_ID,
                    QUERY_HASH,
                    START_MS,
                    END_MS,
                    DURATION_MS,
                    RESPONSE_SIZE,
                    RESPONSE_HASH,
                    RAW_SOAP
                )
                -- Start of the Common Table Expression definition
                WITH ParsedData AS (
                    SELECT
                        /* ts */
                        to_timestamp_ntz(regexp_substr(raw_soap, '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}')) AS TS,
            
                        /* level */
                        regexp_substr(raw_soap, '^[^ ]+ [^ ]+ ([A-Z]+)', 1, 1, 'e', 1) AS LEVEL,
            
                        /* logger (single cleaned value) */
                        regexp_replace(regexp_substr(raw_soap, ' [A-Za-z0-9_\\\\\\\\\\\\\\\\.]+:', 1, 1), '[: ]', '') AS LOGGER,
            
                        /* message_kind */
                        regexp_substr(raw_soap, '- ([A-Za-z0-9_]+)', 1, 1, 'e', 1) AS MESSAGE_KIND,
            
                        regexp_substr(raw_soap, 'gatlingRunId=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS GATLING_RUN_ID,
                        regexp_substr(raw_soap, 'status=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS STATUS,
                        regexp_substr(raw_soap, 'gatlingSessionId=([^\\\\s]+)', 1, 1, 'e', 1) AS GATLING_SESSION_ID,
                        regexp_substr(raw_soap, 'model=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS MODEL,
                        regexp_substr(raw_soap, 'cube=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS CUBE,
                        regexp_substr(raw_soap, 'catalog=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS CATALOG,
                        regexp_substr(raw_soap, 'queryName=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS QUERY_NAME,
                        regexp_substr(raw_soap, 'atscaleQueryId=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS ATSCALE_QUERY_ID,
                         -- Extract inboundTextAsHash value
                        regexp_substr(raw_soap, 'inboundTextAsHash=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS QUERY_HASH,
                        regexp_substr(raw_soap, 'start=([^\\\\s]+)', 1, 1, 'e', 1) AS START_MS,
                        regexp_substr(raw_soap, 'end=([^\\\\s]+)', 1, 1, 'e', 1) AS END_MS,
                        regexp_substr(raw_soap, 'duration=([^\\\\s]+)', 1, 1, 'e', 1) AS DURATION_MS,
                        regexp_substr(raw_soap, 'responseSize=([^\\\\s]+)', 1, 1, 'e', 1) AS RESPONSE_SIZE,
                        regexp_substr(raw_soap, 'responseHash=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS RESPONSE_HASH,
                         -- Extract the full XML content starting from '<soap:Envelope'
                          COALESCE(
                             NULLIF(regexp_substr(raw_soap, '<soap:Envelope.*</soap:Envelope>', 1, 1, 's'), ''),
                             regexp_substr(raw_soap, 'response=''([^'']*)''', 1, 1, 'e', 1)
                         ) AS RAW_SOAP
                    FROM
                        GATLING_RAW_XMLA_LOGS AS UPLOAD
                    WHERE
                        UPLOAD.RAW_SOAP LIKE ?
                        AND NOT EXISTS (
                            select gatling_run_id from gatling_xmla_headers
                            where gatling_run_id = ?
                            limit 1
                        )
                )
                -- Final SELECT statement to insert data
                SELECT
                    /* stable key built from your join columns using the HASH function */
                    HASH(GATLING_RUN_ID, GATLING_SESSION_ID, MODEL, QUERY_HASH) AS RUN_KEY,
                    TS,
                    LEVEL,
                    LOGGER,
                    MESSAGE_KIND,
                    TRIM(GATLING_RUN_ID) as GATLING_RUN_ID,
                    STATUS,
                    GATLING_SESSION_ID,
                    MODEL,
                    CUBE,
                    CATALOG,
                    QUERY_NAME,
                    ATSCALE_QUERY_ID,
                    QUERY_HASH,
                    START_MS,
                    END_MS,
                    DURATION_MS,
                    RESPONSE_SIZE,
                    RESPONSE_HASH,
                    RAW_SOAP
                FROM
                    ParsedData
                ORDER BY
                    MODEL, CUBE, CATALOG, QUERY_NAME;
            """;
    }

    /** Insert a single response per query: pick first response row per query using ROW_NUMBER() */
    private static String getInsertIntoResponsesSql() {
        return """
                 -- QUERY TO INSERT INTO XMLA_RESPONSES
                INSERT INTO GATLING_XMLA_RESPONSES (
                    RUN_KEY,
                    GATLING_RUN_ID,
                    STATUS,
                    GATLING_SESSION_ID,
                    MODEL,
                    CUBE,
                    CATALOG,
                    QUERY_NAME,
                    ATSCALE_QUERY_ID,
                    QUERY_HASH,
                    RESPONSE_HASH,
                    SOAP_HEADER,
                    SOAP_BODY,
                    SOAP_BODY_HASH
                )
                with modified_soap as (
                SELECT
                *,
                -- Calculate the modified SOAP body as a string once
                IFF(
                  REGEXP_LIKE(TRIM(RAW_SOAP), '^REDACTED$'),'REDACTED',
                  REGEXP_REPLACE(
                    XMLGET(PARSE_XML(RAW_SOAP), 'soap:Body')::VARIANT,
                    '<LastDataUpdate.*?>[^<]*</LastDataUpdate>',
                    '<LastDataUpdate>0</LastDataUpdate>'
                  )
                ) AS MODIFIED_SOAP_BODY_STR
                    FROM
                    GATLING_XMLA_HEADERS
                     WHERE
                        GATLING_RUN_ID = ?
                        AND NOT EXISTS (
                              select gatling_run_id from gatling_xmla_responses
                              where gatling_run_id = ?
                              limit 1
                         )
                    )
                    select
                    RUN_KEY, GATLING_RUN_ID, STATUS, GATLING_SESSION_ID, MODEL,
                    CUBE, CATALOG, QUERY_NAME, ATSCALE_QUERY_ID, QUERY_HASH, RESPONSE_HASH,
                    IFF(
                        REGEXP_LIKE(TRIM(RAW_SOAP), '^REDACTED$'), 'REDACTED'::VARIANT,
                        XMLGET(PARSE_XML(RAW_SOAP),'soap:Header')
                        )AS SOAP_HEADER,
                        MODIFIED_SOAP_BODY_STR::VARIANT AS SOAP_BODY,
                    IFF (
                     REGEXP_LIKE(TRIM(MODIFIED_SOAP_BODY_STR), '^REDACTED$'), 'REDACTED'::VARIANT,
                     HASH(MODIFIED_SOAP_BODY_STR) -- Hash the pre-calculated string
                    ) AS SOAP_BODY_HASH
                    from modified_soap
               """;
    }

    private static void exec(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private Properties getConnectionProperties() {
        String user = PropertiesManager.getCustomProperty("snowflake.archive.username");
        String password = PropertiesManager.getCustomProperty("snowflake.archive.password");
        String warehouse = PropertiesManager.getCustomProperty("snowflake.archive.warehouse");
        String database = PropertiesManager.getCustomProperty("snowflake.archive.database");
        String schema = PropertiesManager.getCustomProperty("snowflake.archive.schema");
        String role = null;

        String privateKeyFile = null;
        String privateKeyPwd = null;

        try {
            privateKeyFile = PropertiesManager.getCustomProperty("snowflake.archive.keyfile.path");
            privateKeyPwd = PropertiesManager.getCustomProperty("snowflake.archive.keyfile.password");
        } catch (Exception e) {
            LOGGER.warn("No private key file or password values found for properties snowflake.archive.keyfile.path or snowflake.archive.keyfile.password.", e);
        }

        if (PropertiesManager.hasProperty("snowflake.archive.role")) {
            role = PropertiesManager.getCustomProperty("snowflake.archive.role");
        }

        user = StringUtils.isNotEmpty(user) ? user.trim() : user;
        password = StringUtils.isNotEmpty(password) ? password.trim() : password;

        Properties props = new Properties();
        props.put("user", user);
        props.put("password", password);
        props.put("warehouse", warehouse);
        props.put("db", database);
        props.put("schema", schema);
        if (privateKeyFile != null) props.put("private_key_file", privateKeyFile);
        if (privateKeyPwd != null) props.put("private_key_file_pwd", privateKeyPwd);

        if (StringUtils.isNotBlank(role)) {
            props.put("role", role);
        }

        if (LOGGER.isDebugEnabled()) {
            int pwLen = password == null ? 0 : password.length();
            String masked = pwLen > 0 ? ("***" + pwLen + "chars**") : "(empty)";
            LOGGER.debug("Snowflake connection properties: user='{}', password={}", user, masked);
        }

        return props;
    }

    private String getSnowflakeURL() {
        String account = PropertiesManager.getCustomProperty("snowflake.archive.account");
        return String.format("jdbc:snowflake://%s.snowflakecomputing.com/", account);
    }

    protected void initAdditionalProperties() {
        AdditionalPropertiesLoader loader = new AdditionalPropertiesLoader();
        PropertiesManager.setCustomProperties(loader.fetchAdditionalProperties(AdditionalPropertiesLoader.SecretsManagerType.AWS));
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (String a : args) {
            if (a.startsWith("--") && a.contains("=")) {
                int i = a.indexOf('=');
                m.put(a.substring(2, i).toLowerCase(Locale.ROOT), a.substring(i + 1));
            }
        }
        return m;
    }
}
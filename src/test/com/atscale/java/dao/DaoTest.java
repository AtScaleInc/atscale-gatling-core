package com.atscale.java.dao;

import com.atscale.java.utils.HashUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.*;
import org.mockito.Mockito;

import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.Map;

public class DaoTest {
    private static final String DB_URL = "jdbc:h2:mem:atscale;MODE=PostgreSQL;DB_CLOSE_DELAY=-1";

    private static final String queriesTableInsert = """
            INSERT INTO engine.queries
            (query_id, user_id, remote_address, query_language, received, query_text, service, session_id, subject_id)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);
            """;
    private static final String queryResultsTableInsert = """
            INSERT INTO engine.query_results
            (query_id, finished, succeeded, result_size, failure_message)
            VALUES(?, ?, ?, ?, ?);
            """;
    private static final String queriesPlannedTableInsert = """
            INSERT INTO engine.queries_planned
            (query_id, planning_started, planning_completed, project_id, cube_id, cube_name, logical_plan, physical_plan)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?);
            """;
    private static final String subqueriesTableInsert = """
            INSERT INTO engine.subqueries
            (subquery_id, query_id, subquery_started, subquery_language, connection_id, subquery_text, subquery_dialect, query_part, is_canary, subgroup_id)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """;

    private static Connection connection;
    private static final List<TestDataDto> testDataList = List.of(new TestDataDto(), new TestDataDto(), new TestDataDto());
    private static final Map<String, TestDataDto> testDataMap = testDataList.stream()
            .collect(Collectors.toMap(data -> data.queryData.queryId.toString(), data -> data));

    @BeforeAll
    public static void setUp() throws SQLException {
        connection = DriverManager.getConnection(DB_URL);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS engine");
            stmt.execute("SET SCHEMA engine");
        }

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                    CREATE TABLE engine.queries (
                        query_id uuid NOT NULL,
                        user_id varchar(256) NULL,
                        remote_address varchar(15) NULL,
                        query_language varchar(32) NOT NULL,
                        received timestamp NOT NULL,
                        query_text text NULL,
                        service text NULL,
                        session_id uuid NULL,
                        subject_id varchar(256) NULL,
                        CONSTRAINT queries_pkey PRIMARY KEY (query_id)
                    );
                    """);

            stmt.execute("""
                    CREATE TABLE engine.query_results (
                        query_id uuid NOT NULL,
                        finished timestamp NOT NULL,
                        succeeded bool NOT NULL,
                        result_size int8 NULL,
                        failure_message text NULL,
                        CONSTRAINT query_results_check CHECK (((NOT succeeded) OR (failure_message IS NULL))),
                        CONSTRAINT query_results_check1 CHECK (((NOT succeeded) OR (result_size IS NOT NULL))),
                        CONSTRAINT query_results_pkey PRIMARY KEY (query_id)
                        );
                    """);

            stmt.execute("""
                    ALTER TABLE engine.query_results 
                    ADD CONSTRAINT query_results_query_id_fkey 
                    FOREIGN KEY (query_id) REFERENCES engine.queries(query_id);
                    """);

            stmt.execute("""
                    CREATE TABLE engine.queries_planned (
                        query_id uuid NOT NULL,
                        planning_started timestamp NOT NULL,
                        planning_completed timestamp NOT NULL,
                        project_id varchar(64) NULL,
                        cube_id varchar(64) NULL,
                        cube_name varchar(128) NULL,
                        logical_plan json NULL,
                        physical_plan json NULL,
                        CONSTRAINT queries_planned_pkey PRIMARY KEY (query_id)
                    );
                    """);

            stmt.execute("""
                    ALTER TABLE engine.queries_planned 
                    ADD CONSTRAINT queries_planned_query_id_fkey 
                    FOREIGN KEY (query_id) REFERENCES engine.queries(query_id);
                    """);

            stmt.execute("""
                    CREATE TABLE engine.subqueries (
                    	subquery_id uuid NOT NULL,
                    	query_id uuid NOT NULL,
                    	subquery_started timestamp NOT NULL,
                    	subquery_language varchar(32) NOT NULL,
                    	connection_id varchar(64) NOT NULL,
                    	subquery_text text NOT NULL,
                    	subquery_dialect varchar(64) NULL,
                    	query_part text NOT NULL,
                    	is_canary bool DEFAULT false NOT NULL,
                    	subgroup_id uuid NULL,
                    	CONSTRAINT subqueries_pkey PRIMARY KEY (subquery_id)
                    );
                    """);

            stmt.execute("""
                    ALTER TABLE engine.subqueries 
                    ADD CONSTRAINT subqueries_query_id_fkey 
                    FOREIGN KEY (query_id) REFERENCES engine.queries(query_id);
                    """);
        }

        insertTestData();
    }

    public static void insertTestData() throws SQLException {
        try(PreparedStatement ps1 = connection.prepareStatement(queriesTableInsert);
        PreparedStatement ps2 = connection.prepareStatement(queryResultsTableInsert);
        PreparedStatement ps3 = connection.prepareStatement(queriesPlannedTableInsert);
        PreparedStatement ps4 = connection.prepareStatement(subqueriesTableInsert)) {
            for (TestDataDto data : testDataList) {
                    ps1.setString(1, data.queryData.queryId.toString());
                    ps1.setString(2, data.queryData.userId);
                    ps1.setString(3, data.queryData.remoteAddress);
                    ps1.setString(4, data.queryData.queryLanguage);
                    ps1.setTimestamp(5, Timestamp.valueOf(data.queryData.received));
                    ps1.setString(6, data.queryData.query_text);
                    ps1.setString(7, data.queryData.service);
                    ps1.setString(8, data.queryData.session_id.toString());
                    ps1.setString(9, data.queryData.subject_id);
                    ps1.addBatch();

                    ps2.setString(1, data.queryResultsData.queryId.toString());
                    ps2.setTimestamp(2, Timestamp.valueOf(data.queryResultsData.finished));
                    ps2.setBoolean(3, data.queryResultsData.succeeded);
                    ps2.setInt(4, data.queryResultsData.resultSetSize);
                    ps2.setString(5, data.queryResultsData.failureMessage);
                    ps2.addBatch();

                    ps3.setString(1, data.queriesPlannedData.queryId.toString());
                    ps3.setTimestamp(2, Timestamp.valueOf(data.queriesPlannedData.planningStarted));
                    ps3.setTimestamp(3, Timestamp.valueOf(data.queriesPlannedData.planningCompleted));
                    ps3.setString(4, data.queriesPlannedData.projectID);
                    ps3.setString(5, data.queriesPlannedData.cubeId);
                    ps3.setString(6, data.queriesPlannedData.cubeName);
                    ps3.setString(7, data.queriesPlannedData.logicalPlan);
                    ps3.setString(8, data.queriesPlannedData.physicalPlan);
                    ps3.addBatch();

                    ps4.setString(1, data.subQueryData.subQueryId.toString());
                    ps4.setString(2, data.subQueryData.queryId.toString());
                    ps4.setTimestamp(3, Timestamp.valueOf(data.subQueryData.started));
                    ps4.setString(4, data.subQueryData.queryLanguage);
                    ps4.setString(5, data.subQueryData.connectionId);
                    ps4.setString(6, data.subQueryData.subqueryText);
                    ps4.setString(7, data.subQueryData.dialect);
                    ps4.setString(8, data.subQueryData.queryPart);
                    ps4.setBoolean(9, data.subQueryData.isCanary);
                    ps4.setString(10, data.subQueryData.subGroupId.toString());
                    ps4.addBatch();
                }
                ps1.executeBatch();
                ps2.executeBatch();
                ps3.executeBatch();
                ps4.executeBatch();
            }
        }

        @Test
        public void testDataRerieval() {
            Properties props = new Properties();

            AtScalePostgresDao spy = org.mockito.Mockito.spy(AtScalePostgresDao.getInstance());
            Mockito.doReturn(DB_URL).when(spy).getDatabaseUrl();
            Mockito.doReturn(props).when(spy).getConnectionProperties();
            List<QueryHistoryDto> queryHistory = spy.getQueryHistory(AtScalePostgresDao.QueryLanguage.SQL, "Sales_Model");

            assertEquals(3, queryHistory.size());

            for(QueryHistoryDto dto : queryHistory) {
                TestDataDto expectedData = testDataMap.get(dto.getAtscaleQueryId());
                assertNotNull(expectedData, "No test data found for query ID: " + dto.getAtscaleQueryId());

                assertEquals(expectedData.queryData.query_text, dto.getInboundText());
                assertEquals(expectedData.queryData.queryLanguage, dto.getQueryLanguage());
                assertEquals(expectedData.queryData.service, dto.getService());
                assertEquals(HashUtil.TO_SHA256(expectedData.queryData.query_text), dto.getInboundTextAsHash());

                assertEquals(expectedData.queryResultsData.resultSetSize, dto.getAvgResultSetSize());

                assertEquals(expectedData.subQueryData.subqueryText, dto.getOutboundText());


                assertEquals(expectedData.queriesPlannedData.cubeName, dto.getCubeName());
                assertEquals(expectedData.queriesPlannedData.projectID, dto.getProjectId());

                assertTrue(dto.getElapsedTimeInSeconds() > 0);
                assertEquals(1, dto.getNumTimes());
            }
        }

        @AfterAll
        static void tearDown() throws SQLException {
            // Close the connection, which keeps the in-memory database alive until this point.
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }

        @Test
        public void testConnection() throws SQLException {
            assert(connection != null && !connection.isClosed());
        }
    }


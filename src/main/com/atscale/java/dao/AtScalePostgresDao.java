package com.atscale.java.dao;

import java.sql.*;
import java.util.Properties;
import com.atscale.java.utils.PropertiesFileReader;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("unused")
public class AtScalePostgresDao {
    public enum QueryLanguage {
        SQL("pgsql"),
        XMLA("analysis"),
        INSTALLER_SQL("sql");

        private final String value;

        QueryLanguage(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
    private final String url;
    private final String user;
    private final String password;
    private static final AtScalePostgresDao INSTANCE = new AtScalePostgresDao();
    private static final String query = """
            SELECT
                        q.service,
                        q.query_language,
                        q.query_text as inbound_text,
                        MAX(s.subquery_text) as outbound_text,
                        p.cube_name,
                        p.project_id,
                        case when MAX(s.subquery_text) like '%as_agg_%' then true else false end as used_agg,
                        COUNT(*)                             AS num_times,
                        AVG(r.finished - p.planning_started) AS elasped_time_in_seconds,
                        AVG(r.result_size)                   AS avg_result_size
                    FROM
                        atscale.engine.queries q
                    INNER JOIN
                        atscale.engine.query_results r
                    ON
                        q.query_id=r.query_id
                    INNER JOIN
                        atscale.engine.queries_planned p
                    ON
                        q.query_id=p.query_id
                    INNER JOIN
                        atscale.engine.subqueries s
                    ON
                        q.query_id=s.query_id
                    WHERE
                        q.query_language = ?
                    AND p.planning_started > current_timestamp - interval '60 day'
                    and p.cube_name = ?
                    AND q.service = 'user-query'
                    AND r.succeeded = true
                    AND LENGTH(q.query_text) > 100
                    AND q.query_text NOT LIKE '/* Virtual query to get the members of a level */%'
                    AND q.query_text NOT LIKE '-- statement does not return rows%'
                    GROUP BY
                        1,
                        2,
                        3,
                        5,
                        6
                    HAVING COUNT(*) >= 1
                    ORDER BY 3
    """;

    private AtScalePostgresDao() {
        this.url = PropertiesFileReader.getAtScalePostgresURL();
        this.user = PropertiesFileReader.getAtScalePostgresUser();
        this.password = PropertiesFileReader.getAtScalePostgresPassword();
    }

    public static AtScalePostgresDao getInstance() {
        return INSTANCE;
    }

    private Connection getConnection() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("currentSchema", "engine");
        return DriverManager.getConnection(url, props);
    }

    public List<QueryHistoryDto> getQueryHistory(QueryLanguage queryLanguage, String cubeName) {
        List<QueryHistoryDto> queryHistory;
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, queryLanguage.getValue());
            stmt.setString(2, cubeName);
            int queryNameIndex = 0; // Index for the query language parameter
            try (ResultSet resultSet = stmt.executeQuery()) {
                queryHistory = extractResults(resultSet);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error fetching query history from the AtScale Postgres database", e);
        }
        return queryHistory;
    }

    public List<QueryHistoryDto> getQueryHistory(String userQuery, String... params) {
        List<QueryHistoryDto> queryHistory;
        try (Connection conn = getConnection();
            PreparedStatement stmt = conn.prepareStatement(userQuery)) {
            for(int i = 1; i <= params.length; i++) {
                String param = params[i - 1];
                stmt.setString(i, param);
            }
            int queryNameIndex = 0; // Index for the query language parameter
            try (ResultSet resultSet = stmt.executeQuery()) {
                queryHistory = extractResults(resultSet);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error fetching query history from the AtScale Postgres database", e);
        }
        return queryHistory;
    }

    private List<QueryHistoryDto> extractResults(ResultSet resultSet) throws SQLException {
        List<QueryHistoryDto> queryHistory = new ArrayList<>();
        int queryNameIndex = 0; // Index for the query language parameter
        while (resultSet.next()) {
            QueryHistoryDto dto = new QueryHistoryDto();
            dto.setQueryName(String.format("Query %s",  queryNameIndex += 1));
            dto.setService(resultSet.getString("service"));
            dto.setQueryLanguage(resultSet.getString("query_language"));
            dto.setInboundText(resultSet.getString("inbound_text"));
            dto.setOutboundText(resultSet.getString("outbound_text"));
            dto.setCubeName(resultSet.getString("cube_name"));
            dto.setProjectId(resultSet.getString("project_id"));
            dto.setAggregateUsed(resultSet.getBoolean("used_agg"));
            dto.setNumTimes(resultSet.getInt("num_times"));
            dto.setElapsedTimeInSeconds(resultSet.getTimestamp("elasped_time_in_seconds"));
            dto.setAvgResultSetSize(resultSet.getInt("avg_result_size"));
            queryHistory.add(dto);
        }
        return queryHistory;
    }
}

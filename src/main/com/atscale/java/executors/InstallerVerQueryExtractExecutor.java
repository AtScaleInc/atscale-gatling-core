package com.atscale.java.executors;

import com.atscale.java.dao.AtScalePostgresDao;
import com.atscale.java.utils.PropertiesManager;
import com.atscale.java.utils.QueryHistoryFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class InstallerVerQueryExtractExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(InstallerVerQueryExtractExecutor.class);

    public static void main(String[] args) {
        InstallerVerQueryExtractExecutor executor = new InstallerVerQueryExtractExecutor();
        executor.execute();
    }

    protected void execute() {
        LOGGER.info("QueryExtractExecutor started.");
        List<String> models = PropertiesManager.getAtScaleModels();

        for(String model : models) {
            LOGGER.info("Processing model: {}", model);
            try {
                cacheJdbcQueries(model);
            } catch (Exception e) {
                LOGGER.error("Error caching JDBC queries for model {}: {}", model, e.getMessage());
            }
            try {
                cacheXmlaQueries(model);
            } catch (Exception e) {
                LOGGER.error("Error caching XMLA queries for model {}: {}", model, e.getMessage()); 
            }
        }
        LOGGER.info("QueryExtractExecutor finished.");
        org.apache.logging.log4j.LogManager.shutdown();
    }

    private void cacheJdbcQueries(String model) {
        // Implement the logic to cache JDBC queries
        // This method should be overridden in subclasses if needed
        LOGGER.info("Caching JDBC queries...");

        final String query = """
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
                            atscale.queries q
                        INNER JOIN
                            atscale.query_results r
                        ON
                            q.query_id=r.query_id
                        INNER JOIN
                            atscale.queries_planned p
                        ON
                            q.query_id=p.query_id
                        INNER JOIN
                            atscale.subqueries s
                        ON
                            q.query_id=s.query_id
                        WHERE
                        q.query_language = ? AND
                        p.planning_started > current_timestamp - interval '60 day'
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
                        ORDER BY 3
    """;


        AtScalePostgresDao dao = AtScalePostgresDao.getInstance();
        QueryHistoryFileUtil queryHistoryFileUtil = new QueryHistoryFileUtil(dao);
        queryHistoryFileUtil.cacheJdbcQueries(model, query, AtScalePostgresDao.QueryLanguage.INSTALLER_SQL.getValue(), model);
    }

    private void cacheXmlaQueries(String model) {
        // Implement the logic to cache XMLA queries
        // This method should be overridden in subclasses if needed
        LOGGER.info("Caching XMLA queries...");

        final String query = """
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
                            atscale.queries q
                        INNER JOIN
                            atscale.query_results r
                        ON
                            q.query_id=r.query_id
                        INNER JOIN
                            atscale.queries_planned p
                        ON
                            q.query_id=p.query_id
                        INNER JOIN
                            atscale.subqueries s
                        ON
                            q.query_id=s.query_id
                        WHERE
                        q.query_language = ? AND
                        p.planning_started > current_timestamp - interval '60 day'
                        and p.cube_name = ?
                        AND q.service = 'user-query'
                        AND r.succeeded = true
                        AND LENGTH(q.query_text) > 100
                        AND q.query_text NOT LIKE '/* Virtual query to get the members of a level */%'
                        AND q.query_text NOT LIKE '-- statement does not return rows%'
                        --AND q.query_text not like '%&%'
                        GROUP BY
                            1,
                            2,
                            3,
                            5,
                            6
                        ORDER BY 3
    """;


        AtScalePostgresDao dao = AtScalePostgresDao.getInstance();
        QueryHistoryFileUtil queryHistoryFileUtil = new QueryHistoryFileUtil(dao);
        queryHistoryFileUtil.cacheXmlaQueries(model, query, AtScalePostgresDao.QueryLanguage.XMLA.getValue(), model);

    }
}

package com.atscale.java.jdbc.scenarios;

import com.atscale.java.jdbc.cases.AtScaleDynamicJdbcActions;
import com.atscale.java.jdbc.cases.NamedQueryActionBuilder;
import com.atscale.java.utils.HashUtil;
import com.atscale.java.utils.PropertiesManager;
import io.gatling.javaapi.core.*;
import org.apache.commons.lang.StringUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.gatling.javaapi.core.CoreDsl.*;

public class AtScaleDynamicQueryBuilderScenario {
    private static final Logger SESSION_LOGGER = LoggerFactory.getLogger("SqlLogger");
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleDynamicQueryBuilderScenario.class);

    public AtScaleDynamicQueryBuilderScenario() {
        super();
    }

    /**
     * Builds a Gatling scenario that executes a series of dynamic JDBC queries.
     * The scenario is constructed using the actions defined in AtScaleDynamicJdbcActions.
     *
     * @return A ScenarioBuilder instance representing the dynamic query execution scenario.
     */
    public ScenarioBuilder buildScenario(String model, String gatlingRunId, String ingestionFilePath, boolean ingestionFileHasHeader) {
        NamedQueryActionBuilder[] namedBuilders;
        if(StringUtils.isNotEmpty(ingestionFilePath)) {
            namedBuilders = AtScaleDynamicJdbcActions.createBuildersIngestedQueries(ingestionFilePath, ingestionFileHasHeader);
            LOGGER.info("Created {} JDBC query builders from ingestion file: {}", namedBuilders.length, ingestionFilePath);
        } else {
            namedBuilders = AtScaleDynamicJdbcActions.createBuildersJdbcQueries(model);
            LOGGER.info("Created {} JDBC query builders from model: {}", namedBuilders.length, model);
        }

        boolean logRows = PropertiesManager.getLogSqlQueryRows(model);
        boolean redactRawData = PropertiesManager.getRedactRawData(model);
        Long throttleBy = PropertiesManager.getAtScaleThrottleMs();
        // Create and return a ScenarioBuilder
        List<ChainBuilder> chains = Arrays.stream(namedBuilders)
        .map(namedBuilder ->
            exec(session -> {
                        return session.set("queryStart", System.currentTimeMillis());
                    }
            ).exec(
                    namedBuilder.builder
            ).exec(session -> {
                long end = System.currentTimeMillis();
                Boolean isJdbcFailed = session.get("jdbcFailed");
                String message = session.get("message");
                if (isJdbcFailed == null) {
                    LOGGER.error("""
                            Unexpected state returned from the galaxio jdbc plugin!
                            JDBC failed flag is null for queryName: {} with inbound hash: {}.
                            Manually set failed state to TRUE, since we cannot detect true state from the plugin.
                            """, namedBuilder.queryName, namedBuilder.inboundTextAsHash);
                    isJdbcFailed = Boolean.TRUE;
                } else if (isJdbcFailed) {
                    LOGGER.error("Returned JDBC failed flag: {} for queryName: {} with inbound hash: {} with message: {}",
                            isJdbcFailed, namedBuilder.queryName, namedBuilder.inboundTextAsHash, message);
                }
                List<?> resultSet = session.getList("queryResultSet");
                long start = session.getLong("queryStart");
                long duration = end - start;
                int rowCount = resultSet.size();
                String status = isJdbcFailed ? "FAILED" : "SUCCEEDED";
                SESSION_LOGGER.info("sqlLog gatlingRunId='{}' status='{}' gatlingSessionId={} model='{}' queryName='{}' atscaleQueryId='{}' inboundTextAsHash='{}' start={} end={} duration={} rows={}", gatlingRunId, status, session.userId(), model, namedBuilder.queryName, namedBuilder.atscaleQueryId, namedBuilder.inboundTextAsHash, start, end, duration, rowCount);
                if (logRows) {
                    int rownum = 0;
                    if (redactRawData) {
                        for (Object row : resultSet) {
                            SESSION_LOGGER.info("sqlLog gatlingRunId='{}' status='{}' gatlingSessionId={} model='{}' queryName='{}' atscaleQueryId='{}' inboundTextAsHash='{}' rownumber={} row={} rowhash={}", gatlingRunId, status, session.userId(), model, namedBuilder.queryName, namedBuilder.atscaleQueryId, namedBuilder.inboundTextAsHash, rownum++, "{REDACTED}", HashUtil.TO_SHA256(row.toString()));
                        }
                    } else {
                        for (Object row : resultSet) {
                            SESSION_LOGGER.info("sqlLog gatlingRunId='{}' status='{}' gatlingSessionId={} model='{}' queryName='{}' atscaleQueryId='{}' inboundTextAsHash='{}' rownumber={} row={} rowhash={}", gatlingRunId, status, session.userId(), model, namedBuilder.queryName, namedBuilder.atscaleQueryId, namedBuilder.inboundTextAsHash, rownum++, row, HashUtil.TO_SHA256(row.toString()));
                        }
                    }
                }
                return session;
        }).pause(Duration.ofMillis(throttleBy))).collect(Collectors.toList());

        return scenario("AtScale Dynamic Query Builder Scenario").exec(chains);
    }
}

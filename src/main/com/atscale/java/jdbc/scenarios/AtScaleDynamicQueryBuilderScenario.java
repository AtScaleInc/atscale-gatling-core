package com.atscale.java.jdbc.scenarios;

import com.atscale.java.jdbc.cases.AtScaleDynamicJdbcActions;
import com.atscale.java.jdbc.cases.NamedQueryActionBuilder;
import com.atscale.java.utils.HashUtil;
import com.atscale.java.utils.PropertiesFileReader;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.ChainBuilder;
import org.apache.commons.lang.StringUtils;
import scala.collection.immutable.Map;
import static io.gatling.javaapi.core.CoreDsl.exec;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.gatling.javaapi.core.CoreDsl.scenario;

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

        boolean logRows = PropertiesFileReader.getLogSqlQueryRows(model);
        Long throttleBy = PropertiesFileReader.getAtScaleThrottleMs();

        List<ChainBuilder> chains = Arrays.stream(namedBuilders)
                .map(namedBuilder ->
                        exec(session -> session
                            .set("queryStart", System.currentTimeMillis())
                        ).exec(
                            namedBuilder.builder
                        ).exec(session -> {
                            long end = System.currentTimeMillis();
                            List<?> resultSet = session.getList("queryResultSet");
                            long start = session.getLong("queryStart");
                            long duration = end - start;
                            int rowCount = resultSet.size();
                            String status = (rowCount < 1) ? "FAILED" : "SUCCEEDED";
                            SESSION_LOGGER.info("sqlLog gatlingRunId='{}' status='{}' gatlingSessionId={} model='{}' queryName='{}' inboundTextAsMd5Hash='{}' start={} end={} duration={} rows={}", gatlingRunId, status, session.userId(), model, namedBuilder.queryName, namedBuilder.inboundTextAsMd5Hash, start, end, duration, rowCount);
                            if (logRows) {
                                int rownum = 0;
                                for (Object row : resultSet) {
                                    SESSION_LOGGER.info("sqlLog gatlingRunId='{}' status='{}' gatlingSessionId={} model='{}' queryName='{}' inboundTextAsMd5Hash='{}' rownumber={} row={} rowhash={}", gatlingRunId, status, session.userId(), model, namedBuilder.queryName, namedBuilder.inboundTextAsMd5Hash, rownum++, row, HashUtil.TO_MD5(row.toString()));
                                }
                            }
                            return session;
                        }).pause(Duration.ofMillis(throttleBy))).collect(Collectors.toList());

        // pause the scenario executions at 10 milliseconds apart
        return scenario("AtScale Dynamic Query Builder Scenario").exec(chains).pause(Duration.ofMillis(10));
    }
}

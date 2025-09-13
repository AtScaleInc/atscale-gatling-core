package com.atscale.java.jdbc.scenarios;

import com.atscale.java.jdbc.cases.AtScaleDynamicJdbcActions;
import com.atscale.java.jdbc.cases.NamedQueryActionBuilder;
import com.atscale.java.utils.HashUtil;
import com.atscale.java.utils.PropertiesFileReader;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.ChainBuilder;
import scala.collection.immutable.Map;
import static io.gatling.javaapi.core.CoreDsl.exec;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.gatling.javaapi.core.CoreDsl.scenario;

public class AtScaleDynamicQueryBuilderScenario {
    private static final Logger SESSION_LOGGER = LoggerFactory.getLogger("SqlLogger");

    public AtScaleDynamicQueryBuilderScenario() {
        super();
    }

    /**
     * Builds a Gatling scenario that executes a series of dynamic JDBC queries.
     * The scenario is constructed using the actions defined in AtScaleDynamicJdbcActions.
     *
     * @return A ScenarioBuilder instance representing the dynamic query execution scenario.
     */
    public ScenarioBuilder buildScenario(String model, String gatlingRunId) {
        NamedQueryActionBuilder[] namedBuilders = AtScaleDynamicJdbcActions.createBuildersJdbcQueries(model);
        boolean logRows = PropertiesFileReader.getLogSqlQueryRows(model);
        Long throttleBy = PropertiesFileReader.getAtScaleThrottleMs();

        List<ChainBuilder> chains = Arrays.stream(namedBuilders)
                .map(namedBuilder ->
                        exec(session -> session
                                .set("queryStart", System.currentTimeMillis())
                                .set("queryResultSet", new java.util.ArrayList<Map<String, Object>>()) // Initialize result set
                        )
                                .exec(namedBuilder.builder)
                                .exec(session -> {
                                    long end = System.currentTimeMillis();
                                    List<?> resultSet = session.getList("queryResultSet");
                                    long start = session.getLong("queryStart");
                                    long duration = end - start;
                                    int rowCount = resultSet.size();
                                    SESSION_LOGGER.info("sqlLog gatlingRunId={} gatlingSessionId={} model='{}' queryName='{}' start={} end={} duration={} rows={}", gatlingRunId, session.userId(), model, namedBuilder.queryName, start, end, duration, rowCount);
                                    if(logRows) {
                                        int rownum = 0;
                                        for (Object row : resultSet) {
                                            SESSION_LOGGER.info("sqlLog gatlingRunId={} gatlingSessionId={} model='{}' queryName='{}' rownumber={} row={} rowhash={}", gatlingRunId, session.userId(), model, namedBuilder.queryName, rownum++, row, HashUtil.TO_MD5(row.toString()));
                                        }
                                    }
                                    return session;
                                }).pause(throttleBy)
                )
                .collect(Collectors.toList());

        // pause the scenario executions at 10 milliseconds apart
        return scenario("AtScale Dynamic Query Builder Scenario").exec(chains).pause(10);
    }
}

package com.atscale.java.jdbc.scenarios;

import com.atscale.java.jdbc.cases.AtScaleDynamicJdbcActions;
import com.atscale.java.jdbc.cases.NamedQueryActionBuilder;
import io.gatling.javaapi.core.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;


import static io.gatling.javaapi.core.CoreDsl.*;
import static org.galaxio.gatling.javaapi.JdbcDsl.allResults;
import static org.galaxio.gatling.javaapi.JdbcDsl.jdbc;

public class AtScaleFeederScenario {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleFeederScenario.class);

    public AtScaleFeederScenario(){
        super();
    }

    public PopulationBuilder buildScenario(String catalog, String model, String gatlingRunId, String ingestionFilePath, boolean ingestionFileHasHeader, List<OpenInjectionStep> openSteps, List<ClosedInjectionStep> closedSteps) {
        NamedQueryActionBuilder[] namedBuilders;
        if (StringUtils.isNotEmpty(ingestionFilePath)) {
            namedBuilders = AtScaleDynamicJdbcActions.createBuildersIngestedQueries(ingestionFilePath, ingestionFileHasHeader, catalog, model);
            LOGGER.info("Created {} JDBC query builders from ingestion file: {}", namedBuilders.length, ingestionFilePath);
        } else {
            namedBuilders = AtScaleDynamicJdbcActions.createBuildersJdbcQueries(catalog, model);
            LOGGER.info("Created {} JDBC query builders from model: {}", namedBuilders.length, model);
        }

        Stream<Map<String, Object>> builderMap = Arrays.stream(namedBuilders)
                .map(nb -> Map.of("builderQuery", (Object) nb));

        int numBuilders = namedBuilders.length;

        // Put the query name and SQL text directly into the feeder as Strings
        Iterator<Map<String, Object>> feeder = Arrays.stream(namedBuilders)
                .map(nb -> Map.of("builderQuery", (Object) nb))
                .iterator();

        // Build a List so logging does not consume the iterator and we can implement a cycling feeder
        List<Map<String, Object>> feederList = Arrays.stream(namedBuilders)
                .map(nb -> Map.of("builderQuery", (Object) nb))
                .collect(Collectors.toList());

        final int totalBuilders = feederList.size();

        // Cycling iterator: never reports empty (good for concurrent users)
        Iterator<Map<String, Object>> cyclingFeeder = new Iterator<>() {
            private int idx = 0;
            @Override
            public boolean hasNext() {
                return totalBuilders > 0;
            }
            @Override
            public Map<String, Object> next() {
                Map<String, Object> item = feederList.get(idx % totalBuilders);
                idx++;
                return item;
            }
        };


        ChainBuilder iterChain =
                feed(cyclingFeeder) // Keep going as long as there is a builder
                .exec(session -> {
                long start = System.currentTimeMillis();
                String queryName = ((NamedQueryActionBuilder) session.get("builderQuery")).queryName;
                String querySql = ((NamedQueryActionBuilder) session.get("builderQuery")).inboundQueryText;
                // Session is immutable — session.set(...) returns a new Session. Return that new session.
                return session
                        .set("queryStart", start)
                        .set("dynamicQueryName", queryName)
                        .set("dynamicSql", querySql);
            })
                    // Use Gatling EL to supply name and SQL (jdbc expects String overload)
                    .exec(jdbc("#{dynamicQueryName}")
                            .query("#{dynamicSql}")
                            .check(allResults().saveAs("queryResultSet")))
                    .exec(session -> {
                        Boolean isJdbcFailed = session.get("jdbcFailed");
                        String status = session.isFailed()? "KO" : "OK";
                        NamedQueryActionBuilder namedBuilder = ((NamedQueryActionBuilder) session.get("builderQuery"));
                        long end = System.currentTimeMillis();
                        List<?> resultSet = session.getList("queryResultSet");
                        long start = session.contains("queryStart") ? session.getLong("queryStart") : 0L;
                        long duration = end - start;
                        int rowCount = resultSet == null ? 0 : resultSet.size();
                        //String status = (resultSet == null || resultSet.isEmpty()) ? "FAILED" : "SUCCEEDED";

                        return session.markAsSucceeded();
                    });

            // Repeat the iteration chain totalBuilders times so each builder is consumed
            ScenarioBuilder scn = scenario("Dynamic JDBC Feeder Scenario")
                    .repeat(numBuilders).on(iterChain);

        if (openSteps != null) {
                return scn.injectOpen(openSteps);
            } else {
                return scn.injectClosed(closedSteps);
            }
        }

}

package com.atscale.java.xmla.scenarios;

import com.atscale.java.utils.PropertiesFileReader;
import com.atscale.java.xmla.cases.AtScaleDynamicXmlaActions;
import com.atscale.java.xmla.cases.NamedHttpRequestActionBuilder;
import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.ScenarioBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Arrays;
import java.util.stream.Collectors;
import static io.gatling.javaapi.core.CoreDsl.exec;
import static io.gatling.javaapi.core.CoreDsl.scenario;

public class AtScaleXmlaScenario {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleXmlaScenario.class);
    private static final Logger SESSION_LOGGER = LoggerFactory.getLogger("XmlaLogger");

    public AtScaleXmlaScenario() {
        super();
    }

    /**
     * Builds a Gatling scenario that executes a series of dynamic JDBC queries.
     * The scenario is constructed using the actions defined in AtScaleDynamicJdbcActions.
     *
     * @return A ScenarioBuilder instance representing the dynamic query execution scenario.
     */
    public ScenarioBuilder buildScenario(String model, String cube, String catalog, String gatlingRunId) {
        boolean logResponseBody = PropertiesFileReader.getLogXmlaResponseBody(model);
        Long throttleBy = PropertiesFileReader.getAtScaleThrottleMs();
        AtScaleDynamicXmlaActions xmlaActions = new AtScaleDynamicXmlaActions();
        NamedHttpRequestActionBuilder[] builders = xmlaActions.createPayloadsXmlaQueries(model, cube, catalog);

        List<ChainBuilder> chains = Arrays.stream(builders)
                .map(namedBuilder ->
                        exec(session -> session
                                .set("queryStart", System.currentTimeMillis())
                        )
                                .exec(namedBuilder.builder)
                                .exec(session -> {
                                    long end = System.currentTimeMillis();
                                    String response = session.getString("responseBody");
                                    long start = session.getLong("queryStart");
                                    long duration = end - start;
                                    int responseSize = response == null? 0: response.length();
                                    if(logResponseBody) {
                                        SESSION_LOGGER.info("xmlaLog gatlingRunId='{}' gatlingSessionId={} model='{}' cube='{}' catalog='{}' queryName='{}' start={} end={} duration={} responseSize={} response={}",
                                                gatlingRunId, session.userId(), model, cube, catalog, namedBuilder.queryName, start, end, duration, responseSize, response);
                                    } else {
                                        SESSION_LOGGER.info("xmlaLog gatlingRunId='{}' gatlingSessionId={} model='{}' cube='{}' catalog='{}' queryName='{}' start={} end={} duration={} responseSize={}",
                                                gatlingRunId, session.userId(), model, cube, catalog, namedBuilder.queryName, start, end, duration, responseSize);
                                    }

                                    return session;
                                }).pause(Duration.ofMillis(throttleBy))).collect(Collectors.toList());

        return scenario("AtScale XMLA Scenario").exec(chains).pause(10);
    }
}

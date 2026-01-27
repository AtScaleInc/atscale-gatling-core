package com.atscale.java.xmla.scenarios;

import com.atscale.java.utils.PropertiesManager;
import com.atscale.java.xmla.cases.AtScaleDynamicXmlaActions;
import com.atscale.java.xmla.cases.NamedHttpRequestActionBuilder;
import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.ScenarioBuilder;
import org.apache.commons.lang.StringUtils;
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
    public ScenarioBuilder buildScenario(String model, String cube, String catalog, String gatlingRunId, String ingestionFile, boolean ingestionFileHasHeader) {
        NamedHttpRequestActionBuilder[] builders;
        boolean logResponseBody = PropertiesManager.getLogXmlaResponseBody(model);
        boolean redactRawData = PropertiesManager.getRedactRawData(model);
        Long throttleBy = PropertiesManager.getAtScaleThrottleMs();
        AtScaleDynamicXmlaActions xmlaActions = new AtScaleDynamicXmlaActions();

        if(StringUtils.isNotEmpty(ingestionFile)) {
            builders = xmlaActions.createPayloadsIngestedXmlaQueries(model, cube, catalog, ingestionFile, ingestionFileHasHeader);
        } else {
            builders = xmlaActions.createPayloadsXmlaQueries(model, cube, catalog);
        }

        List<ChainBuilder> chains = Arrays.stream(builders)
                .map(namedBuilder ->
                        exec(session -> session
                                .set("queryStart", System.currentTimeMillis())
                        )
                                .exec(namedBuilder.builder)
                                .exec(session -> {
                                    long end = System.currentTimeMillis();
                                    String response = session.getString("responseBody");
                                    int statusCode = session.getInt("responseStatus");
                                    boolean isSuccess = statusCode >= 200 && statusCode < 300;
                                    String status = isSuccess ? "SUCCEEDED" : "FAILED";
                                    long start = session.getLong("queryStart");
                                    long duration = end - start;
                                    int responseSize = response == null? 0: response.length();
                                    if(isSuccess) {
                                        LOGGER.info("Query '{}' with QueryId'{}' with hash '{}' returned status code {} and is marked as {}",
                                                namedBuilder.queryName, namedBuilder.atscaleQueryId, namedBuilder.inboundTextAsHash, statusCode, status);
                                    } else {
                                            LOGGER.error("Query '{}' with QueryId'{}' with hash '{}' returned status code {} and is marked as {}",
                                                    namedBuilder.queryName, namedBuilder.atscaleQueryId, namedBuilder.inboundTextAsHash, statusCode, status);
                                    }
                                    if(logResponseBody) {
                                        // Since we are streaming the data from atscale we can only stream once before the stream is exhausted.
                                        // therefore, we can get either to raw response or its hash, but not both.
                                        if(redactRawData) {
                                            SESSION_LOGGER.info("xmlaLog gatlingRunId='{}' status='{}' gatlingSessionId={} model='{}' cube='{}' catalog='{}' queryName='{}' atscaleQueryId='{}' inboundTextAsHash='{}' inboundTextAsBase64='{}' start={} end={} duration={} responseSize={} responseHash='{}' response='{}'",
                                                    gatlingRunId, status, session.userId(), model, cube, catalog, namedBuilder.queryName, namedBuilder.atscaleQueryId, namedBuilder.inboundTextAsHash, namedBuilder.getInboundQueryTextAsBase64(), start, end, duration, responseSize, response, "REDACTED");
                                        } else {
                                            SESSION_LOGGER.info("xmlaLog gatlingRunId='{}' status='{}' gatlingSessionId={} model='{}' cube='{}' catalog='{}' queryName='{}' atscaleQueryId='{}' inboundTextAsHash='{}' inboundTextAsBase64='{}' start={} end={} duration={} responseSize={} responseHash='{}' response='{}'",
                                                    gatlingRunId, status, session.userId(), model, cube, catalog, namedBuilder.queryName, namedBuilder.atscaleQueryId, namedBuilder.inboundTextAsHash, namedBuilder.getInboundQueryTextAsBase64(), start, end, duration, responseSize, "REDACTED", response);
                                        }
                                    } else {
                                        SESSION_LOGGER.info("xmlaLog gatlingRunId='{}' status='{}' gatlingSessionId={} model='{}' cube='{}' catalog='{}' queryName='{}' atscaleQueryId='{}' inboundTextAsHash='{}' inboundTextAsBase64='{}' start={} end={} duration={} responseSize={}",
                                                gatlingRunId, status, session.userId(), model, cube, catalog, namedBuilder.queryName, namedBuilder.atscaleQueryId, namedBuilder.inboundTextAsHash, namedBuilder.getInboundQueryTextAsBase64(), start, end, duration, responseSize);
                                    }
                                    return session;
                                }).pause(Duration.ofMillis(throttleBy))).collect(Collectors.toList());
        return scenario("AtScale XMLA Scenario").exec(chains).pause(Duration.ofMillis(10));
    }
}

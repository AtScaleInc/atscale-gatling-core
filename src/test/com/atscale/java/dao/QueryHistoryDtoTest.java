package com.atscale.java.dao;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.apache.commons.lang3.builder.EqualsBuilder;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class QueryHistoryDtoTest {

    private final QueryHistoryDto dto1 = new QueryHistoryDto();
    private final QueryHistoryDto dto2 = new QueryHistoryDto();
    private final QueryHistoryDto dto3 = new QueryHistoryDto();

    private List<QueryHistoryDto> queryHistoryList;

    @BeforeEach
    public void setUp() {
        // Populate dto1
        dto1.setQueryName("Query 1");
        dto1.setService("user-query");
        dto1.setQueryLanguage("SQL");
        dto1.setInboundText("SELECT * FROM sales WHERE id = 1");
        dto1.setOutboundText("-- plan info --");
        dto1.setCubeName("Internet Sales Model");
        dto1.setProjectId("proj-1");
        dto1.setAggregateUsed(false);
        dto1.setNumTimes(1);
        dto1.setElapsedTimeInSeconds(new Timestamp(System.currentTimeMillis()));
        dto1.setAvgResultSetSize(1);
        dto1.setAtscaleQueryId("qid-1");

        // Populate dto2
        dto2.setQueryName("Query 2");
        dto2.setService("user-query");
        dto2.setQueryLanguage("SQL");
        dto2.setInboundText("SELECT count(*) FROM sales");
        dto2.setOutboundText("-- plan info 2 --");
        dto2.setCubeName("TPC-DS Benchmark Model");
        dto2.setProjectId("proj-2");
        dto2.setAggregateUsed(true);
        dto2.setNumTimes(5);
        dto2.setElapsedTimeInSeconds(new Timestamp(System.currentTimeMillis()));
        dto2.setAvgResultSetSize(100);
        dto2.setAtscaleQueryId("qid-2");

        // Populate dto3
        dto3.setQueryName("Query 3");
        dto3.setService("user-query");
        dto3.setQueryLanguage("MDX");
        dto3.setInboundText("SELECT {Measures.[Sales]} ON COLUMNS FROM [Sales]");
        dto3.setOutboundText("-- plan info 3 --");
        dto3.setCubeName("MDX Model");
        dto3.setProjectId("proj-3");
        dto3.setAggregateUsed(false);
        dto3.setNumTimes(2);
        dto3.setElapsedTimeInSeconds(new Timestamp(System.currentTimeMillis()));
        dto3.setAvgResultSetSize(10);
        dto3.setAtscaleQueryId("qid-3");

        // Create a small list of DTOs that other tests can use
        queryHistoryList = new ArrayList<>();
        queryHistoryList.add(dto1);
        queryHistoryList.add(dto2);
        queryHistoryList.add(dto3);
    }

    @Test
    public void setupProvidesThreeDtos() {
        Assertions.assertNotNull(queryHistoryList, "Setup list should not be null");
        Assertions.assertEquals(3, queryHistoryList.size(), "Setup list should contain three DTOs");
    }

    @Test
    public void testJsonRoundTripStructuralEquality() {
        for (QueryHistoryDto dto : queryHistoryList) {
            testJsonRoundTripStructuralEquality(dto);
            testReflectionEqualsRoundTripUsingReflection(dto);
        }
    }

    private void testJsonRoundTripStructuralEquality(QueryHistoryDto dto) {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        // produce json from dto1
        String json = dto.toJson();
        // read back to DTO
        QueryHistoryDto dtoFromJson = QueryHistoryDto.fromJson(json);

        // convert both to JsonNode trees for structural comparison
        com.fasterxml.jackson.databind.JsonNode nodeOriginal = mapper.valueToTree(dto);
        com.fasterxml.jackson.databind.JsonNode nodeRoundTrip = mapper.valueToTree(dtoFromJson);

        Assertions.assertEquals(nodeOriginal, nodeRoundTrip, "Round-trip JSON should be structurally equal to original DTO");
    }

    private void testReflectionEqualsRoundTripUsingReflection(QueryHistoryDto dto) {
        QueryHistoryDto dtoFromJson = QueryHistoryDto.fromJson(dto.toJson());
        boolean equal = EqualsBuilder.reflectionEquals(dto, dtoFromJson, true);
        Assertions.assertTrue(equal, "Reflection-based equality should hold after JSON round-trip");
    }
}

package com.atscale.java.stats;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SimulationLogParserTest {

    private static final Path GATLING_ROOT        = Paths.get("src/test/resources/gatling");
    private static final Path TPCDS_DIR           = Paths.get("src/test/resources/gatling/tpcds");
    private static final Path TPCDS_FILE          = Paths.get("src/test/resources/gatling/tpcds/simulation.log");
    private static final Path TPCDS_W_RUNID_DIR   = Paths.get("src/test/resources/gatling/tpcds_w_runid");
    private static final Path INTERNETSALES_DIR   = Paths.get("src/test/resources/gatling/internetsales");

    // -------------------------------------------------------------------------
    // parse(Path) routing
    // -------------------------------------------------------------------------

    @Test
    void parse_rootDirectory_findsAllRuns() throws IOException {
        List<SimulationRun> runs = SimulationLogParser.parse(GATLING_ROOT);
        assertEquals(3, runs.size());
    }

    @Test
    void parse_subdirectory_findsSingleRun() throws IOException {
        List<SimulationRun> runs = SimulationLogParser.parse(TPCDS_DIR);
        assertEquals(1, runs.size());
    }

    @Test
    void parse_directFile_returnsSingleRun() throws IOException {
        List<SimulationRun> runs = SimulationLogParser.parse(TPCDS_FILE);
        assertEquals(1, runs.size());
        assertEquals("tpcds", runs.get(0).systemRunId());
    }

    // -------------------------------------------------------------------------
    // TPC-DS run — metadata
    // -------------------------------------------------------------------------

    @Test
    void tpcds_runMetadata() throws IOException {
        SimulationRun run = SimulationLogParser.parse(TPCDS_DIR).get(0);
        assertAll(
            () -> assertEquals("tpcds", run.systemRunId()),
            () -> assertEquals("com.atscale.java.jdbc.simulations.AtScaleClosedInjectionStepSimulation",
                               run.simulationClass()),
            () -> assertEquals("3.14.9", run.gatlingVersion()),
            () -> assertEquals("TPCDS JDBC Tests", run.description()),
            () -> assertEquals("", run.gatlingRunId()),
            () -> assertEquals(List.of("AtScale Dynamic Query Builder Scenario"), run.scenarioNames())
        );
    }

    // -------------------------------------------------------------------------
    // TPC-DS run with embedded AtScale run ID in description
    // -------------------------------------------------------------------------

    @Test
    void tpcds_w_runid_parsesRunIdAndDescription() throws IOException {
        SimulationRun run = SimulationLogParser.parse(TPCDS_W_RUNID_DIR).get(0);
        assertAll(
            () -> assertEquals("2026-05-14-kbnnCnwtti", run.gatlingRunId()),
            () -> assertEquals("TPCDS JDBC Tests", run.description())
        );
    }

    @Test
    void tpcds_w_runid_requestCounts() throws IOException {
        SimulationRun run = SimulationLogParser.parse(TPCDS_W_RUNID_DIR).get(0);
        assertAll(
            () -> assertEquals(320, run.totalRequests()),
            () -> assertEquals(304, run.okCount()),
            () -> assertEquals(16,  run.koCount()),
            () -> assertEquals(0,   run.droppedCount()),
            () -> assertEquals(20,  run.requestGroups().size())
        );
    }

    @Test
    void tpcds_requestCounts() throws IOException {
        SimulationRun run = SimulationLogParser.parse(TPCDS_DIR).get(0);
        assertAll(
            () -> assertEquals(100, run.totalRequests()),
            () -> assertEquals(95,  run.okCount()),
            () -> assertEquals(5,   run.koCount()),
            () -> assertEquals(0,   run.droppedCount()),
            () -> assertEquals(20,  run.requestGroups().size())
        );
    }

    // -------------------------------------------------------------------------
    // TPC-DS run — error detection
    // -------------------------------------------------------------------------

    @Test
    void tpcds_singleErrorSummary() throws IOException {
        SimulationRun run = SimulationLogParser.parse(TPCDS_DIR).get(0);
        assertEquals(1, run.errors().size());
    }

    @Test
    void tpcds_errorSummaryDetails() throws IOException {
        SimulationRun.ErrorSummary err = SimulationLogParser.parse(TPCDS_DIR).get(0).errors().get(0);
        assertAll(
            () -> assertEquals(5, err.count()),
            () -> assertEquals(List.of("TPC-DS_query 98"), err.affectedRequests()),
            () -> assertTrue(err.message().contains("operator does not exist: date >= text"))
        );
    }

    @Test
    void tpcds_koRequestGroup() throws IOException {
        SimulationRun run = SimulationLogParser.parse(TPCDS_DIR).get(0);
        SimulationRun.RequestGroupStats q98 = run.requestGroups().stream()
            .filter(g -> g.requestName().equals("TPC-DS_query 98"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("TPC-DS_query 98 not found"));
        assertAll(
            () -> assertEquals(5, q98.count()),
            () -> assertEquals(0, q98.okCount()),
            () -> assertEquals(5, q98.koCount())
        );
    }

    // -------------------------------------------------------------------------
    // Internet Sales run — metadata and clean pass
    // -------------------------------------------------------------------------

    @Test
    void internetsales_runMetadata() throws IOException {
        SimulationRun run = SimulationLogParser.parse(INTERNETSALES_DIR).get(0);
        assertAll(
            () -> assertEquals("internetsales", run.systemRunId()),
            () -> assertEquals("Internet Sales JDBC Model Tests", run.description()),
            () -> assertEquals("", run.gatlingRunId()),
            () -> assertEquals(306, run.totalRequests()),
            () -> assertEquals(306, run.okCount()),
            () -> assertEquals(0,   run.koCount()),
            () -> assertEquals(0,   run.droppedCount()),
            () -> assertTrue(run.errors().isEmpty()),
            () -> assertEquals(9,   run.requestGroups().size())
        );
    }

    @Test
    void internetsales_query1ResponseTimeStats() throws IOException {
        SimulationRun run = SimulationLogParser.parse(INTERNETSALES_DIR).get(0);
        SimulationRun.RequestGroupStats q1 = run.requestGroups().get(0);
        SimulationRun.ResponseTimeStats rt = q1.responseTimes();
        assertAll(
            () -> assertEquals("Query 1", q1.requestName()),
            () -> assertEquals(34,   q1.count()),
            () -> assertEquals(34,   q1.okCount()),
            () -> assertEquals(0,    q1.koCount()),
            () -> assertEquals(326,  rt.min()),
            () -> assertEquals(3574, rt.max()),
            () -> assertEquals(1171, rt.mean())
        );
    }
}

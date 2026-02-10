package com.atscale.java.jdbc.cases;

import com.atscale.java.dao.QueryHistoryDto;
import com.atscale.java.utils.QueryHistoryFileUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AtScaleDynamicJdbcActionsTest {

    private static final String MODEL = "testModel";
    private static final String CATALOG = "default_catalog";
    private static final String EXPECTED_CATALOG = "\"" + CATALOG + "\"";
    private static final String EXPECTED_MODEL = "\"" + MODEL + "\"";

    @BeforeEach
    void ensureDirs() throws Exception {
        Files.createDirectories(Paths.get(System.getProperty("user.dir"), "ingest"));
        Files.createDirectories(Paths.get(System.getProperty("user.dir"), "queries"));
        // Ensure factory returns null to avoid initializing the Gatling Jdbc DSL during tests
        AtScaleDynamicJdbcActions.ACTION_BUILDER_FACTORY = q -> null;
    }

    @AfterEach
    void cleanup() throws Exception {
        Files.deleteIfExists(Paths.get(System.getProperty("user.dir"), "ingest", "test_ingest.csv"));
        Path jdbcPath = Paths.get(QueryHistoryFileUtil.getJdbcFilePath(MODEL));
        Files.deleteIfExists(jdbcPath);
        AtScaleDynamicJdbcActions.ACTION_BUILDER_FACTORY = null;
    }

    @Test
    void testCreateBuildersIngestedQueries() throws Exception {
        Path ingestPath = Paths.get(System.getProperty("user.dir"), "ingest", "test_ingest.csv");
        String csv = "sampler_name,sql_text\n" +
                "0,SELECT 1 as one FROM ${CatalogName}.${ModelName} WHERE 1 = 1'\n" +
                "1,SELECT 2 as two FROM  ${CatalogName}.${ModelName} as TABLE WHERE TheTable.id = 2\n'";
        Files.write(ingestPath, csv.getBytes());

        NamedQueryActionBuilder[] builders = AtScaleDynamicJdbcActions.createBuildersIngestedQueries("test_ingest.csv", true, CATALOG, MODEL);

        assertNotNull(builders);
        assertEquals(2, builders.length);
        assertEquals("0", builders[0].queryName);
        assertTrue(builders[0].inboundQueryText.contains("SELECT"));
        assertTrue(builders[0].inboundQueryText.contains(EXPECTED_CATALOG));
        assertTrue(builders[0].inboundQueryText.contains(EXPECTED_MODEL));
        assertTrue(builders[1].inboundQueryText.contains("SELECT"));
        assertTrue(builders[1].inboundQueryText.contains(EXPECTED_CATALOG));
        assertTrue(builders[1].inboundQueryText.contains(EXPECTED_MODEL));
    }

    @Test
    void testCreateBuildersJdbcQueries() throws Exception {
        Path jdbcFile = Paths.get(QueryHistoryFileUtil.getJdbcFilePath(MODEL));
        List<QueryHistoryDto> dtos = new ArrayList<>();

        QueryHistoryDto q0 = new QueryHistoryDto();
        q0.setQueryName("q0");
        q0.setInboundText("SELECT 1 as one FROM ${CatalogName}.${ModelName} WHERE 1 = 1'");
        q0.setAtscaleQueryId("42");
        dtos.add(q0);

        QueryHistoryDto q1 = new QueryHistoryDto();
        q1.setQueryName("q1");
        q1.setInboundText("SELECT 2 as two FROM  ${CatalogName}.${ModelName} as TABLE WHERE TheTable.id = 2");
        q1.setAtscaleQueryId("43");
        dtos.add(q1);

        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter().writeValue(jdbcFile.toFile(), dtos);

        NamedQueryActionBuilder[] builders = AtScaleDynamicJdbcActions.createBuildersJdbcQueries(CATALOG, MODEL);
        assertNotNull(builders);
        assertEquals(2, builders.length);
        assertEquals("q0", builders[0].queryName);
        assertTrue(builders[0].inboundQueryText.contains("SELECT"));
        assertTrue(builders[0].inboundQueryText.contains(EXPECTED_CATALOG));
        assertTrue(builders[0].inboundQueryText.contains(EXPECTED_MODEL));
        assertTrue(builders[1].inboundQueryText.contains("SELECT"));
        assertTrue(builders[1].inboundQueryText.contains(EXPECTED_CATALOG));
        assertTrue(builders[1].inboundQueryText.contains(EXPECTED_MODEL));
    }

    @Test
    void testCreateBuildersQuotedJdbcQueries() throws Exception {
        Path jdbcFile = Paths.get(QueryHistoryFileUtil.getJdbcFilePath(MODEL));
        List<QueryHistoryDto> dtos = new ArrayList<>();

        QueryHistoryDto q0 = new QueryHistoryDto();
        q0.setQueryName("q0");
        q0.setInboundText("""
                SELECT 1 as one FROM "${CatalogName}"."${ModelName}" WHERE 1 = 1'""");
        q0.setAtscaleQueryId("42");
        dtos.add(q0);

        QueryHistoryDto q1 = new QueryHistoryDto();
        q1.setQueryName("q1");
        q1.setInboundText("""
        SELECT 2 as two FROM  "${CatalogName}"."${ModelName}" as TABLE WHERE TheTable.id = 2""");
        q1.setAtscaleQueryId("43");
        dtos.add(q1);

        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter().writeValue(jdbcFile.toFile(), dtos);

        NamedQueryActionBuilder[] builders = AtScaleDynamicJdbcActions.createBuildersJdbcQueries(CATALOG, MODEL);
        assertNotNull(builders);
        assertEquals(2, builders.length);
        assertEquals("q0", builders[0].queryName);
        assertTrue(builders[0].inboundQueryText.contains("SELECT"));
        assertTrue(builders[0].inboundQueryText.contains(EXPECTED_CATALOG));
        assertTrue(builders[0].inboundQueryText.contains(EXPECTED_MODEL));
        assertTrue(builders[1].inboundQueryText.contains("SELECT"));
        assertTrue(builders[1].inboundQueryText.contains(EXPECTED_CATALOG));
        assertTrue(builders[1].inboundQueryText.contains(EXPECTED_MODEL));
    }
}


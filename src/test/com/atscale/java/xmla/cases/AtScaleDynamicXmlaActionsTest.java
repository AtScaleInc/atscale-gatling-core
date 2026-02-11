package com.atscale.java.xmla.cases;

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

public class AtScaleDynamicXmlaActionsTest {

    private static final String MODEL = "testModel";
    private static final String CUBE = "TPCDS Benchmark Model";
    private static final String CATALOG = "TPCDS_CATALOG";

    @BeforeEach
    void ensureDirs() throws Exception {
        Files.createDirectories(Paths.get(System.getProperty("user.dir"), "ingest"));
        Files.createDirectories(Paths.get(System.getProperty("user.dir"), "queries"));
    }

    @AfterEach
    void cleanup() throws Exception {
        // remove any files we may have created for the tests
        Files.deleteIfExists(Paths.get(System.getProperty("user.dir"), "ingest", "dax_ingest.csv"));
        Path xmlaPath = Paths.get(QueryHistoryFileUtil.getXmlaFilePath(MODEL));
        Files.deleteIfExists(xmlaPath);

        Files.deleteIfExists(Paths.get(System.getProperty("user.dir"), "ingest", "mdx_ingest.csv"));
        Path mdxPath = Paths.get(QueryHistoryFileUtil.getXmlaFilePath(MODEL));
        Files.deleteIfExists(mdxPath);
    }

    @Test
    void testCreatePayloadsIngestedDaxQueries() throws Exception {
        // Prepare a simple CSV ingestion file with header and two queries
        Path ingestPath = Paths.get(System.getProperty("user.dir"), "ingest", "dax_ingest.csv");

        String daxQuery1 = """
        EVALUATE ROW(Avg Unit Net Profit, [Average Catalog Unit Net Profit]""";

        String daxQuery2 = """
        EVALUATE ROW(Avg Qrtrly Sls Ratio, [Avg Quarter Sales Ratio])""";

        String daxQuery3 = """
        EVALUATE ROW(Cat Sls Avg Coupon Amt, [Catalog Sales Average Coupon Amount])""";


        String csv = "sampler_name,sql_text\n" +
               String.format("DAX-1,\"%s\"\n", daxQuery1) +
               String.format("DAX-2,\"%s\"\n", daxQuery2) +
               String.format("DAX-3,\"%s\"\n", daxQuery3);
        Files.write(ingestPath, csv.getBytes());

        // use a subclass that avoids initializing Gatling DSL by returning null for httpRequest
        AtScaleDynamicXmlaActions actions = new AtScaleDynamicXmlaActions() {
            @Override
            protected io.gatling.javaapi.http.HttpRequestActionBuilder httpRequest(String queryName, String body, String model) {
                return null; // we only want to test creation of payloads and metadata, not actual Gatling builders
            }
        };

        NamedHttpRequestActionBuilder[] builders = actions.createPayloadsIngestedXmlaQueries(MODEL, CUBE, CATALOG, "dax_ingest.csv", true);

        assertNotNull(builders);
        assertEquals(3, builders.length);

        // verify basic fields
        assertEquals("DAX-1", builders[0].queryName);
        assertTrue(builders[0].inboundQueryText.contains(daxQuery1));

        // verify basic fields
        assertEquals("DAX-2", builders[1].queryName);
        assertTrue(builders[1].inboundQueryText.contains(daxQuery2));

        // verify basic fields
        assertEquals("DAX-3", builders[2].queryName);
        assertTrue(builders[2].inboundQueryText.contains(daxQuery3));

        // xml payload should contain provided cube and catalog
        assertTrue(builders[0].xmlPayload.contains("<Cube>" + CUBE + "</Cube>"));
        assertTrue(builders[0].xmlPayload.contains("<Catalog>" + CATALOG + "</Catalog>"));
    }

    @Test
    void testCreatePayloadsDaxQueries() throws Exception {
        // Prepare a JSON queries file that QueryHistoryFileUtil would read
        String q1 = "EVALUATE ROW(Avg Unit Net Profit, [Average Catalog Unit Net Profit]";
        String q2 = "EVALUATE ROW(\"\"Avg Quarterly Store Sales 98 - 99\"\", [Avg Quarterly Store Sales for 1998-1999])";


        Path xmlaFile = Paths.get(QueryHistoryFileUtil.getXmlaFilePath(MODEL));
        List<QueryHistoryDto> dtos = new ArrayList<>();

        QueryHistoryDto dax1 = new QueryHistoryDto();
        dax1.setQueryName("DAX-1");
        dax1.setInboundText(q1);
        dax1.setAtscaleQueryId("DAX-1");
        dtos.add(dax1);

        QueryHistoryDto dax2 = new QueryHistoryDto();
        dax2.setQueryName("DAX-2");
        dax2.setInboundText(q2);
        dax2.setAtscaleQueryId("DAX-2");
        dtos.add(dax2);

        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter().writeValue(xmlaFile.toFile(), dtos);

        AtScaleDynamicXmlaActions actions = new AtScaleDynamicXmlaActions() {
            @Override
            protected io.gatling.javaapi.http.HttpRequestActionBuilder httpRequest(String queryName, String body, String model) {
                return null;
            }
        };
        NamedHttpRequestActionBuilder[] builders = actions.createPayloadsXmlaQueries(MODEL, CUBE, CATALOG);

        assertNotNull(builders);
        assertEquals(2, builders.length);

        assertEquals("DAX-1", builders[0].queryName);
        assertEquals("DAX-1", builders[0].atscaleQueryId);
        assertEquals(q1, builders[0].inboundQueryText);

        assertEquals("DAX-2", builders[1].queryName);
        assertEquals("DAX-2", builders[1].atscaleQueryId);
        assertEquals(q2, builders[1].inboundQueryText);
        // ensure xml payload was generated and contains the cube and catalog
        assertTrue(builders[0].xmlPayload.contains("<Cube>" + CUBE + "</Cube>"));
        assertTrue(builders[0].xmlPayload.contains("<Catalog>" + CATALOG + "</Catalog>"));
    }

    @Test
    void testCreatePayloadsIngestedMdxQueries() throws Exception {
        // Prepare a simple CSV ingestion file with header and two queries
        Path ingestPath = Paths.get(System.getProperty("user.dir"), "ingest", "mdx_ingest.csv");

        String mdxQuery1 = """
        SELECT  FROM [tpcds_benchmark_model] WHERE ([Measures].[Average Catalog Unit Net Profit]) CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS""";
        String mdxQuery2 = """
        SELECT  FROM [${ModelName}] WHERE ([Measures].[Average Catalog Unit Net Profit]) CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS""";

        String mdxQuery2Expected = String.format("SELECT  FROM [%s] WHERE ([Measures].[Average Catalog Unit Net Profit]) CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS", MODEL);

        String csv = "sampler_name,sql_text\n" +
                String.format("DAX-1,\"%s\"\n", mdxQuery1) +
                String.format("DAX-2,\"%s\"\n", mdxQuery2);
        Files.write(ingestPath, csv.getBytes());

        // use a subclass that avoids initializing Gatling DSL by returning null for httpRequest
        AtScaleDynamicXmlaActions actions = new AtScaleDynamicXmlaActions() {
            @Override
            protected io.gatling.javaapi.http.HttpRequestActionBuilder httpRequest(String queryName, String body, String model) {
                return null; // we only want to test creation of payloads and metadata, not actual Gatling builders
            }
        };

        NamedHttpRequestActionBuilder[] builders = actions.createPayloadsIngestedXmlaQueries(MODEL, CUBE, CATALOG, "mdx_ingest.csv", true);

        assertNotNull(builders);
        assertEquals(2, builders.length);

        // verify basic fields
        assertEquals("DAX-1", builders[0].queryName);
        assertTrue(builders[0].inboundQueryText.contains(mdxQuery1));

        // verify basic fields
        assertEquals("DAX-2", builders[1].queryName);
        assertTrue(builders[1].inboundQueryText.contains(mdxQuery2Expected));


        // xml payload should contain provided cube and catalog
        assertTrue(builders[0].xmlPayload.contains("<Cube>" + CUBE + "</Cube>"));
        assertTrue(builders[0].xmlPayload.contains("<Catalog>" + CATALOG + "</Catalog>"));
    }

    @Test
    void testCreatePayloadsMdxQueries() throws Exception {
        // Prepare a JSON queries file that QueryHistoryFileUtil would read
        String mdxQuery1 = """
        SELECT  FROM [tpcds_benchmark_model] WHERE ([Measures].[Average Catalog Unit Net Profit]) CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS""";
        String mdxQuery2 = """
        SELECT  FROM [${ModelName}] WHERE ([Measures].[Average Catalog Unit Net Profit]) CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS""";

        String mdxQuery2Expected = String.format("SELECT  FROM [%s] WHERE ([Measures].[Average Catalog Unit Net Profit]) CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS", MODEL);


        Path xmlaFile = Paths.get(QueryHistoryFileUtil.getXmlaFilePath(MODEL));
        List<QueryHistoryDto> dtos = new ArrayList<>();

        QueryHistoryDto dax1 = new QueryHistoryDto();
        dax1.setQueryName("MDX-1");
        dax1.setInboundText(mdxQuery1);
        dax1.setAtscaleQueryId("MDX-1");
        dtos.add(dax1);

        QueryHistoryDto dax2 = new QueryHistoryDto();
        dax2.setQueryName("MDX-2");
        dax2.setInboundText(mdxQuery2);
        dax2.setAtscaleQueryId("MDX-2");
        dtos.add(dax2);

        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter().writeValue(xmlaFile.toFile(), dtos);

        AtScaleDynamicXmlaActions actions = new AtScaleDynamicXmlaActions() {
            @Override
            protected io.gatling.javaapi.http.HttpRequestActionBuilder httpRequest(String queryName, String body, String model) {
                return null;
            }
        };
        NamedHttpRequestActionBuilder[] builders = actions.createPayloadsXmlaQueries(MODEL, CUBE, CATALOG);

        assertNotNull(builders);
        assertEquals(2, builders.length);

        assertEquals("MDX-1", builders[0].queryName);
        assertEquals("MDX-1", builders[0].atscaleQueryId);
        assertEquals(mdxQuery1, builders[0].inboundQueryText);

        assertEquals("MDX-2", builders[1].queryName);
        assertEquals("MDX-2", builders[1].atscaleQueryId);
        assertEquals(mdxQuery2Expected, builders[1].inboundQueryText);
        // ensure xml payload was generated and contains the cube and catalog
        assertTrue(builders[0].xmlPayload.contains("<Cube>" + CUBE + "</Cube>"));
        assertTrue(builders[0].xmlPayload.contains("<Catalog>" + CATALOG + "</Catalog>"));
    }
}

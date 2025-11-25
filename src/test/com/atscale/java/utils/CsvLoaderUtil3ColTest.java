package com.atscale.java.utils;

import com.atscale.java.dao.QueryHistoryDto;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CsvLoaderUtil3ColTest {
    private final CsvLoaderUtil csvLoader = new CsvLoaderUtil(RandomStringUtils.secure().nextAlphabetic(12) + ".csv", true);
    private final String query0 = CsvLoaderUtilTest.query0;
	private final String query1 = CsvLoaderUtilTest.query1;
	private	final String query2 = CsvLoaderUtilTest.query2;
	private final String query3 = CsvLoaderUtilTest.query3;
	private final String atscaleQueryId0 = UUID.randomUUID().toString();
	private final String atscaleQueryId1 = UUID.randomUUID().toString();
	private final String atscaleQueryId2 = UUID.randomUUID().toString();
	private final String atscaleQueryId3 = UUID.randomUUID().toString();


    @BeforeAll
    public void setup() throws IOException {
        Files.createDirectories(csvLoader.getPath());
        String csvContent = "sampler_name,sql_text\n" +
                String.format("0,%s,\"%s\"\n", atscaleQueryId0, query0) +
				String.format("1,%s,\"%s\"\n", atscaleQueryId1, query1) +
                String.format("2,%s,\"%s\"\n", atscaleQueryId2, query2) +
				String.format("3,%s,\"%s\"\n", atscaleQueryId3, query3);
        Files.write(csvLoader.getFilePath(), csvContent.getBytes());
    }

    @AfterAll
    public void cleanup() throws IOException {
        Files.deleteIfExists(csvLoader.getFilePath());
    }

    @Test
    void testCsvLoaderReadsAndParsesCorrectly() {
        List<QueryHistoryDto> dtos = csvLoader.loadQueriesFromCsv();

        Assertions.assertEquals(4, dtos.size());
        Assertions.assertEquals("0", dtos.get(0).getQueryName());
		Assertions.assertEquals(atscaleQueryId0, dtos.get(0).getAtscaleQueryId());
        Assertions.assertEquals(emulateCsvReaderQuoteStripping(query0), dtos.get(0).getInboundText());
        Assertions.assertEquals("1", dtos.get(1).getQueryName());
		Assertions.assertEquals(atscaleQueryId1, dtos.get(1).getAtscaleQueryId());
        Assertions.assertEquals(emulateCsvReaderQuoteStripping(query1), dtos.get(1).getInboundText());
		Assertions.assertEquals("2", dtos.get(2).getQueryName());
		Assertions.assertEquals(atscaleQueryId2, dtos.get(2).getAtscaleQueryId());
		Assertions.assertEquals(emulateCsvReaderQuoteStripping(query2), dtos.get(2).getInboundText());
		Assertions.assertEquals("3", dtos.get(3).getQueryName());
		Assertions.assertEquals(atscaleQueryId3, dtos.get(3).getAtscaleQueryId());
		Assertions.assertEquals(emulateCsvReaderQuoteStripping(query3), dtos.get(3).getInboundText());
    }


    private String emulateCsvReaderQuoteStripping(String query) {
        return query.replace("\"\"", "\"");
    }
}

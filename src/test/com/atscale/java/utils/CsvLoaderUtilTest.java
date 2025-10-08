package com.atscale.java.utils;

import com.atscale.java.dao.QueryHistoryDto;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.shadow.com.univocity.parsers.csv.Csv;

import java.io.*;
import java.nio.file.*;
import java.util.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CsvLoaderUtilTest {
    private final CsvLoaderUtil csvLoader = new CsvLoaderUtil(RandomStringUtils.secure().nextAlphabetic(10) + ".csv", true);

    @BeforeAll
    public void setup() throws IOException {
        Files.createDirectories(csvLoader.getPath());
        String csvContent = "sampler_name,sql_text\n" +
                "0,\"SELECT 1\"\n" +
                "1,\"SELECT 2\"\n";
        Files.write(csvLoader.getFilePath(), csvContent.getBytes());
    }

    @AfterAll
    public void cleanup() throws IOException {
        Files.deleteIfExists(csvLoader.getFilePath());
    }

    @Test
    void testCsvLoaderReadsAndParsesCorrectly() {
        List<QueryHistoryDto> dtos = csvLoader.loadQueriesFromCsv();

        Assertions.assertEquals(2, dtos.size());
        Assertions.assertEquals("0", dtos.get(0).getQueryName());
        Assertions.assertEquals("SELECT 1", dtos.get(0).getInboundText());
        Assertions.assertEquals("1", dtos.get(1).getQueryName());
        Assertions.assertEquals("SELECT 2", dtos.get(1).getInboundText());
    }
}

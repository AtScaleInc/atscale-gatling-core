package com.atscale.java.utils;

import com.atscale.java.dao.QueryHistoryDto;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QueryHistoryFileUtilTest {
    QueryHistoryFileUtil underTest = new QueryHistoryFileUtil(null);
    private final List<String> models = new ArrayList<>();
    private final Map<String, List<QueryHistoryDto>> testData = new HashMap<>();

    @BeforeAll
    void setup() throws IOException {
        models.add(RandomStringUtils.secure().nextAlphanumeric(10));
        models.add(RandomStringUtils.secure().nextAlphabetic(10));
        models.add(RandomStringUtils.secure().nextAlphabetic(5) + " " + RandomStringUtils.secure().nextAlphabetic(5));

        for (String model : models) {
            List<QueryHistoryDto> dtos = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                QueryHistoryDto dto = new QueryHistoryDto();
                dto.setQueryName(String.valueOf(i));
                dto.setInboundText("SELECT " + (i + 1));
                dto.setAtscaleQueryId(String.valueOf(i));
                dtos.add(dto);
            }
            testData.put(model, dtos);
        }

        for(String model : models) {
            String jdbcPath = QueryHistoryFileUtil.getJdbcFilePath(model);
            String xmlaPath = QueryHistoryFileUtil.getXmlaFilePath(model);
            underTest.writeQueryHistoryToFile(testData.get(model), jdbcPath);
            underTest.writeQueryHistoryToFile(testData.get(model), xmlaPath);
        }
    }

    @AfterAll
    void cleanup() throws IOException {
        for(String model : models) {
            String jdbcPath = QueryHistoryFileUtil.getJdbcFilePath(model);
            String xmlaPath = QueryHistoryFileUtil.getXmlaFilePath(model);
            Files.deleteIfExists(Paths.get(jdbcPath));
            Files.deleteIfExists(Paths.get(xmlaPath));
        }
    }

    @Test
    void testQueryHistoryFileUtilReadsAndParsesCorrectly() throws IOException {
        for(String model : models) {
            String jdbcPath = QueryHistoryFileUtil.getJdbcFilePath(model);
            String xmlaPath = QueryHistoryFileUtil.getXmlaFilePath(model);

            List<QueryHistoryDto> dtosJdbc = QueryHistoryFileUtil.readQueryHistoryFromFile(jdbcPath);
            List<QueryHistoryDto> dtosXmla = QueryHistoryFileUtil.readQueryHistoryFromFile(xmlaPath);
            Assertions.assertEquals(5, dtosJdbc.size());
            Assertions.assertEquals(5, dtosXmla.size());

            for (int i = 0; i < 5; i++) {
                Assertions.assertEquals(String.valueOf(i), dtosJdbc.get(i).getQueryName());
                Assertions.assertEquals("SELECT " + (i + 1), dtosJdbc.get(i).getInboundText());
                Assertions.assertEquals(String.valueOf(i), dtosJdbc.get(i).getAtscaleQueryId());

                Assertions.assertEquals(String.valueOf(i), dtosXmla.get(i).getQueryName());
                Assertions.assertEquals("SELECT " + (i + 1), dtosXmla.get(i).getInboundText());
                Assertions.assertEquals(String.valueOf(i), dtosXmla.get(i).getAtscaleQueryId());
            }
        }
    }
}

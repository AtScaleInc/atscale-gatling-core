package com.atscale.java.utils;

import com.atscale.java.dao.QueryHistoryDto;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Disabled;

@Disabled
public class StructureFileUtilTest {
    @Test
    public void testReadJdbcQueries() throws IOException {
        List<QueryHistoryDto> queries = StructuredFileUtil.readQueryHistoryFromFile(StructuredFileUtil.getJdbcFilePath("internet_sales"));
        assertEquals(16, queries.size());

        for(QueryHistoryDto query : queries) {
            assertNotNull(query.getQueryName());
            assertFalse(query.getQueryName().isEmpty());
            assertNotNull(query.getInboundText());
            assertFalse(query.getInboundText().isEmpty());
        }
    }

    @Test
    public void testReadXmlaQueries() throws IOException {
        List<QueryHistoryDto> queries = StructuredFileUtil.readQueryHistoryFromFile(StructuredFileUtil.getXmlaFilePath("internet_sales"));
        assertEquals(12, queries.size());

        for(QueryHistoryDto query : queries) {
            assertNotNull(query.getQueryName());
            assertFalse(query.getQueryName().isEmpty());
            assertNotNull(query.getInboundText());
            assertFalse(query.getInboundText().isEmpty());
        }
    }

}

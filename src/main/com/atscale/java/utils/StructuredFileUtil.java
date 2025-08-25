package com.atscale.java.utils;

import java.io.IOException;
import java.util.List;
import com.atscale.java.dao.QueryHistoryDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Utility class to read a list of QueryHistoryDto objects from a file.
 */
@SuppressWarnings("unused")
public class StructuredFileUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(StructuredFileUtil.class);
    private static final String delimiter = "\t";

    public static synchronized List<QueryHistoryDto> readQueryHistoryFromFile(String filePath) throws IOException {
        List<QueryHistoryDto> list = new java.util.ArrayList<>();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(filePath))) {
            reader.readLine();  // Skip header
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(delimiter, -1);
                if (parts.length >= 2) {
                    String queryName = parts[0].trim();
                    String inboundText = parts[1].trim();
                    QueryHistoryDto dto = new QueryHistoryDto();
                    dto.setQueryName(queryName);
                    dto.setInboundText(inboundText);
                    list.add(dto);
                }
            }
        }
        return list;
    }

    public static String getXmlaFilePath(String model) {
        return String.format("custom_queries/%s_xmla_queries.txt", model);
    }

    public static String getJdbcFilePath(String model) {
        return String.format("custom_queries/%s_jdbc_queries.txt", model);
    }
}
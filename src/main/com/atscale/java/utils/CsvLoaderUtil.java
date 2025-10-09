package com.atscale.java.utils;

import com.atscale.java.dao.QueryHistoryDto;
import com.opencsv.CSVReader;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvLoaderUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvLoaderUtil.class);
    private String fileName;
    private boolean hasHeader;

    private CsvLoaderUtil(){
       super();
    }

    public CsvLoaderUtil(String fileName, boolean hasHeader) {
       this();
       this.fileName = fileName;
       this.hasHeader = hasHeader;
    }

    public List<QueryHistoryDto> loadQueriesFromCsv() {
        List<QueryHistoryDto> queries = new ArrayList<>();
        Path path = getFilePath();
        try (CSVReader reader = new CSVReader(Files.newBufferedReader(path))) {
            String[] nextLine;
            if (this.hasHeader) {
                reader.readNext(); // Skip header line
            }
            while ((nextLine = reader.readNext()) != null) {
                if (nextLine.length < 2) {
                    LOGGER.warn("Skipping line with insufficient columns: {}", (Object) nextLine);
                    continue;
                }
                QueryHistoryDto query = new QueryHistoryDto();
                query.setQueryName(nextLine[0]);
                query.setInboundText(nextLine[1]);
                queries.add(query);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load queries from CSV file: " + this.fileName, e);
        }
        return queries;
    }

    protected Path getPath() {
        return Paths.get(System.getProperty("user.dir"), "ingest");
    }

    public Path getFilePath() {
        return Paths.get(System.getProperty("user.dir"), "ingest", this.fileName);
    }
}

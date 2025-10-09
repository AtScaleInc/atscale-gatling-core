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
    private final String query0 = """
        DEFINE
	    VAR __DS0Core = 
		SUMMARIZECOLUMNS(
			'Product'[Category.Key0],
			'Product'[Category],
			""Internet_Sales_Amount_Local"", 'CubeMeasures'[Internet Sales Amount Local]
		)

	    VAR __DS0BodyLimited = 
		TOPN(
			1002,
			__DS0Core,
			[Internet_Sales_Amount_Local],
			0,
			'Product'[Category],
			1,
			'Product'[Category.Key0],
			1
		)

        EVALUATE
            __DS0BodyLimited
        
        ORDER BY
            [Internet_Sales_Amount_Local] DESC, 'Product'[Category], 'Product'[Category.Key0]   
        """;

    private final String query1 = """
        DEFINE
			VAR __H0FilterTable = 
				TREATAS({CURRENCY(""1"")}, 'Product'[Category.Key0])
		
			VAR __DS0Core = 
				SUMMARIZECOLUMNS(
					'Customer'[Customer Country.Key0],
					'Customer'[Customer Country],
					""Internet_Sales_Amount_Local"", 'CubeMeasures'[Internet Sales Amount Local],
					""CubeMeasuresInternet_Sales_Amount_Local"", IGNORE(
						CALCULATE('CubeMeasures'[Internet Sales Amount Local], KEEPFILTERS(__H0FilterTable))
					)
				)
		
			VAR __DS0BodyLimited = 
				TOPN(
					1002,
					__DS0Core,
					[Internet_Sales_Amount_Local],
					0,
					'Customer'[Customer Country],
					1,
					'Customer'[Customer Country.Key0],
					1
				)
		
		EVALUATE
			__DS0BodyLimited
		
		ORDER BY
			[Internet_Sales_Amount_Local] DESC,
			'Customer'[Customer Country],
			'Customer'[Customer Country.Key0]
        """;

	private final String query2 = """
		DEFINE
		VAR __DS0Core = 
			SUMMARIZECOLUMNS(
				'Customer'[Customer Country.Key0],
				'Customer'[Customer Country],
				""Internet_Sales_Amount_Local"", 'CubeMeasures'[Internet Sales Amount Local]
			)
	
		VAR __DS0BodyLimited = 
			TOPN(
				1002,
				__DS0Core,
				[Internet_Sales_Amount_Local],
				0,
				'Customer'[Customer Country],
				1,
				'Customer'[Customer Country.Key0],
				1
			)
		
		EVALUATE
			__DS0BodyLimited
		
		ORDER BY
			[Internet_Sales_Amount_Local] DESC,
			'Customer'[Customer Country],
			'Customer'[Customer Country.Key0]
    """;

	private final String query3 = """
		DEFINE
		VAR __H0FilterTable = 
			TREATAS({""Germany""}, 'Customer'[Customer Country.Key0])
	
		VAR __DS0Core = 
			SUMMARIZECOLUMNS(
				'Product'[Category.Key0],
				'Product'[Category],
				""Internet_Sales_Amount_Local"", 'CubeMeasures'[Internet Sales Amount Local],
				""CubeMeasuresInternet_Sales_Amount_Local"", IGNORE(
					CALCULATE('CubeMeasures'[Internet Sales Amount Local], KEEPFILTERS(__H0FilterTable))
				)
			)
	
		VAR __DS0BodyLimited = 
			TOPN(
				1002,
				__DS0Core,
				[Internet_Sales_Amount_Local],
				0,
				'Product'[Category],
				1,
				'Product'[Category.Key0],
				1
			)
	
					EVALUATE
						__DS0BodyLimited
					
			ORDER BY
						[Internet_Sales_Amount_Local] DESC, 'Product'[Category], 'Product'[Category.Key0]
    """;

    @BeforeAll
    public void setup() throws IOException {
        Files.createDirectories(csvLoader.getPath());
        String csvContent = "sampler_name,sql_text\n" +
                String.format("0,\"%s\"\n", query0) +
				String.format("1,\"%s\"\n", query1) +
                String.format("2,\"%s\"\n", query2) +
				String.format("3,\"%s\"\n", query3);
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
        Assertions.assertEquals(emulateCsvReaderQuoteStripping(query0), dtos.get(0).getInboundText());
        Assertions.assertEquals("1", dtos.get(1).getQueryName());
        Assertions.assertEquals(emulateCsvReaderQuoteStripping(query1), dtos.get(1).getInboundText());
		Assertions.assertEquals("2", dtos.get(2).getQueryName());
		Assertions.assertEquals(emulateCsvReaderQuoteStripping(query2), dtos.get(2).getInboundText());
		Assertions.assertEquals("3", dtos.get(3).getQueryName());
		Assertions.assertEquals(emulateCsvReaderQuoteStripping(query3), dtos.get(3).getInboundText());
    }


    private String emulateCsvReaderQuoteStripping(String query) {
        return query.replace("\"\"", "\"");
    }
}

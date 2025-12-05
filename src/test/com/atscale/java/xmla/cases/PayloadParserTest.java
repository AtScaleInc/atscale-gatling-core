package com.atscale.java.xmla.cases;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Assertions;
import javax.xml.parsers.SAXParserFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class PayloadParserTest {
    @Test
    public void simpleParserTest() throws Exception {
        final String simpleXmlPayload = """
            <Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">
                <Body>
                    <ExecuteResponse xmlns="urn:schemas-microsoft-com:xml-analysis">
                        <return>
                            <root xmlns="">
                                <row AtScaleQueryId="12345" />
                            </root>
                        </return>
                    </ExecuteResponse>
                </Body>
            </Envelope>
            """;

       String hash1 = getHash(simpleXmlPayload);
       String hash2 = getHash(simpleXmlPayload);
       Assertions.assertEquals(hash1, hash2);
    }

    @Test
    public void parserShouldIgnoreTimestamps() throws Exception {
        final String earlierTimestampedXmlPayload = """
            <Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">
                <Body>
                    <ExecuteResponse xmlns="urn:schemas-microsoft-com:xml-analysis">
                        <return>
                            <root xmlns="">
                                <row AtScaleQueryId="12345" Data="Some complex data &amp; more data" />
                                <row AtScaleQueryId="67890" Data="Even more complex data &lt; with tags &gt;" />
                                <LastDataUpdate xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">2025-12-04T15:04:36.024341251Z</LastDataUpdate>
                            </root>
                        </return>
                    </ExecuteResponse>
                </Body>
            </Envelope>
            """;

        final String laterTimestampedXmlPayload = """
            <Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">
                <Body>
                    <ExecuteResponse xmlns="urn:schemas-microsoft-com:xml-analysis">
                        <return>
                            <root xmlns="">
                                <row AtScaleQueryId="12345" Data="Some complex data &amp; more data" />
                                <row AtScaleQueryId="67890" Data="Even more complex data &lt; with tags &gt;" />
                                <LastDataUpdate xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">2025-12-04T16:04:39.024341875Z</LastDataUpdate>
                            </root>
                        </return>
                    </ExecuteResponse>
                </Body>
            </Envelope>
            """;

        String hash1 = getHash(earlierTimestampedXmlPayload);
        String hash2 = getHash(laterTimestampedXmlPayload);
        Assertions.assertEquals(hash1, hash2);
    }

    @Test
    public void parserShouldHandleAtScalePayloads() throws Exception {
        // Placeholder test
        File testFile1 = new File("src/test/resources/earlierExampleXmlaPayload.xml");
        File testFile2 = new File("src/test/resources/laterExampleXmlaPayload.xml");

        String hash1 = getHash(testFile1);
        String hash2 = getHash(testFile2);

        Assertions.assertEquals(hash1, hash2);
    }


    private String getHash(String testPayload) throws Exception {
        AtScaleDynamicXmlaActions.HashingSaxHandler handler = new AtScaleDynamicXmlaActions.HashingSaxHandler();
        Assertions.assertNotNull(handler);

        byte[] bytes = testPayload.getBytes(StandardCharsets.UTF_8);
        InputStream inputStream = new ByteArrayInputStream(bytes);

        SAXParserFactory.newNSInstance().newSAXParser().parse(inputStream, handler);

        return handler.getHash();
    }

    private String getHash(File testContent) throws Exception {
        AtScaleDynamicXmlaActions.HashingSaxHandler handler = new AtScaleDynamicXmlaActions.HashingSaxHandler();
        Assertions.assertNotNull(handler);

        InputStream inputStream = new FileInputStream(testContent);

        SAXParserFactory.newNSInstance().newSAXParser().parse(inputStream, handler);

        return handler.getHash();
    }
}

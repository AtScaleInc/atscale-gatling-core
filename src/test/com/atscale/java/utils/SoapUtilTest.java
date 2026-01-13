package com.atscale.java.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.core.util.Assert;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;


public class SoapUtilTest {


    @Test
    public void testSoapBodyExtraction() throws Exception {
        File sampleFile = new File("src/test/resources/internetSalesXmlaPayload.xml");
        try(FileInputStream fis = new FileInputStream(sampleFile)){
            byte[] data = new byte[(int) sampleFile.length()];
            assertTrue(fis.read(data) > 0);
            String sampleXml = new String(data, StandardCharsets.UTF_8);

            String queryId = SoapUtil.extractQueryId(sampleXml);
            assertEquals("c37ee7f7-113c-4e87-8420-154de39c6634", queryId);

            String soapBody = SoapUtil.extractSoapBody(sampleXml);
            Assert.isNonEmpty(soapBody);
            assertTrue(StringUtils.isNotEmpty(soapBody) && soapBody.startsWith("<soap:Body><ExecuteResponse xmlns"));
        }
    }
}

package com.atscale.java.utils;

import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


public class SoapUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(SoapUtil.class);

    public static String extractSoapBody(String xml) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        try {
            Document doc = factory.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
            // clear the lastDataUpdate content to avoid MD5 differences
            NodeList lastDataUpdateList = doc.getElementsByTagNameNS(
                    "http://schemas.microsoft.com/analysisservices/2003/engine", "LastDataUpdate");
            if (lastDataUpdateList.getLength() != 0) { // Nothing to clear
                lastDataUpdateList.item(0).setTextContent(""); // Clear content
            }

            NodeList bodyList = doc.getElementsByTagNameNS("*", "Body");
            if (bodyList.getLength() == 0) return null;
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(bodyList.item(0)), new StreamResult(writer));
            return writer.toString();
        } catch (SAXException | IOException | ParserConfigurationException | TransformerException e) {
            LOGGER.error("Failed to extract SOAP body from {}",xml, e);
        }
        return "FAILED_TO_EXTRACT_SOAP_BODY";
    }

    public static String extractBody(String xml) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        try {
            Document doc = factory.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
            // clear the lastDataUpdate content to avoid MD5 differences
            NodeList lastDataUpdateList = doc.getElementsByTagNameNS(
                    "http://schemas.microsoft.com/analysisservices/2003/engine", "LastDataUpdate");
            if (lastDataUpdateList.getLength() == 0) return xml; // Nothing to clear
            lastDataUpdateList.item(0).setTextContent(""); // Clear content

            NodeList bodyList = doc.getElementsByTagNameNS("*", "Body");
            if (bodyList.getLength() == 0) return null;
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(bodyList.item(0)), new StreamResult(writer));
            return writer.toString();
        } catch (SAXException | IOException | ParserConfigurationException | TransformerException e) {
            LOGGER.error("Failed to extract SOAP body from {}",xml, e);
        }
        return "FAILED_TO_EXTRACT_SOAP_BODY";
    }

    public static String extractQueryId(String xml)  {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        try {
            Document doc = factory.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
            NodeList queryIdList = doc.getElementsByTagNameNS("http://xsd.atscale.com/soap_v1", "queryId");
            if (queryIdList.getLength() == 0) return null;
            return queryIdList.item(0).getTextContent();
        } catch (SAXException | IOException | ParserConfigurationException e) {
            LOGGER.error("Failed to extract QueryId from {}",xml, e);
        }
        return "FAILED_TO_EXTRACT_QUERY_ID";
    }
}

package com.atscale.java.utils;

import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.stream.*;

public class SoapUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(SoapUtil.class);

    public static String extractSoapBody(String xml) {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        StringWriter writer = new StringWriter();
        boolean inBody = false;
        int depth = 0;
        try {
            XMLStreamReader reader = factory.createXMLStreamReader(new StringReader(xml));
            XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
            XMLStreamWriter xmlWriter = outputFactory.createXMLStreamWriter(writer);

            while (reader.hasNext()) {
                int event = reader.next();
                if (event == XMLStreamConstants.START_ELEMENT && "Body".equals(reader.getLocalName())) {
                    inBody = true;
                    depth = 1;
                    xmlWriter.writeStartElement(reader.getPrefix(), reader.getLocalName(), reader.getNamespaceURI());
                    for (int i = 0; i < reader.getNamespaceCount(); i++) {
                        xmlWriter.writeNamespace(reader.getNamespacePrefix(i), reader.getNamespaceURI(i));
                    }
                    for (int i = 0; i < reader.getAttributeCount(); i++) {
                        xmlWriter.writeAttribute(reader.getAttributeLocalName(i), reader.getAttributeValue(i));
                    }
                    continue;
                }
                if (inBody) {
                    switch (event) {
                        case XMLStreamConstants.START_ELEMENT:
                            depth++;
                            xmlWriter.writeStartElement(reader.getPrefix(), reader.getLocalName(), reader.getNamespaceURI());
                            for (int i = 0; i < reader.getNamespaceCount(); i++) {
                                xmlWriter.writeNamespace(reader.getNamespacePrefix(i), reader.getNamespaceURI(i));
                            }
                            for (int i = 0; i < reader.getAttributeCount(); i++) {
                                xmlWriter.writeAttribute(reader.getAttributeLocalName(i), reader.getAttributeValue(i));
                            }
                            break;
                        case XMLStreamConstants.CHARACTERS:
                            xmlWriter.writeCharacters(reader.getText());
                            break;
                        case XMLStreamConstants.END_ELEMENT:
                            xmlWriter.writeEndElement();
                            depth--;
                            if (depth == 0) {
                                xmlWriter.flush();
                                return writer.toString();
                            }
                            break;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to extract SOAP body from {}", xml, e);
            return "FAILED_TO_EXTRACT_SOAP_BODY";
        }
        return null;
    }

    public static String extractQueryId(String xml) {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        try {
            XMLStreamReader reader = factory.createXMLStreamReader(new StringReader(xml));
            while (reader.hasNext()) {
                int event = reader.next();
                if (event == XMLStreamConstants.START_ELEMENT
                        && "queryId".equals(reader.getLocalName())
                        && "http://xsd.atscale.com/soap_v1".equals(reader.getNamespaceURI())) {
                    return reader.getElementText();
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to extract QueryId from {}", xml, e);
        }
        return "FAILED_TO_EXTRACT_QUERY_ID";
    }
}

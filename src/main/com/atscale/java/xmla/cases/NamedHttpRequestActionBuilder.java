package com.atscale.java.xmla.cases;

import io.gatling.javaapi.http.HttpRequestActionBuilder;

public class NamedHttpRequestActionBuilder {
    public final HttpRequestActionBuilder builder;
    public final String queryName;
    public final String inboundTextAsHash;
    public final String xmlPayload;
    public final String atscaleQueryId;

    public NamedHttpRequestActionBuilder(HttpRequestActionBuilder builder, String queryName, String inboundTextAsHash, String xmlPayload, String atscaleQueryId) {
        this.builder = builder;
        this.queryName = queryName;
        this.inboundTextAsHash = inboundTextAsHash;
        this.xmlPayload = xmlPayload;
        this.atscaleQueryId = atscaleQueryId;
    }
}

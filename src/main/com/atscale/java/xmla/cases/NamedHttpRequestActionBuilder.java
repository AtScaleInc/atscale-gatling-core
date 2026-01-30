package com.atscale.java.xmla.cases;

import io.gatling.javaapi.http.HttpRequestActionBuilder;

public class NamedHttpRequestActionBuilder {
    public final HttpRequestActionBuilder builder;
    public final String queryName;
    public final String inboundTextAsHash;
    public final String inboundTextAsBase64;
    public final String xmlPayload;
    public final String atscaleQueryId;
    public final String inboundQueryText;

    public NamedHttpRequestActionBuilder(HttpRequestActionBuilder builder, String queryName, String inboundTextAsHash, String inboundTextAsBase64, String xmlPayload, String atscaleQueryId, String inboundQueryText) {
        this.builder = builder;
        this.queryName = queryName;
        this.inboundTextAsHash = inboundTextAsHash;
        this.inboundTextAsBase64 = inboundTextAsBase64;
        this.xmlPayload = xmlPayload;
        this.atscaleQueryId = atscaleQueryId;
        this.inboundQueryText = inboundQueryText;
    }

    public String getInboundQueryTextAsBase64() {
        if (inboundTextAsBase64 != null && !inboundTextAsBase64.isEmpty()) return inboundTextAsBase64;
        return java.util.Base64.getEncoder().encodeToString(inboundQueryText.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}

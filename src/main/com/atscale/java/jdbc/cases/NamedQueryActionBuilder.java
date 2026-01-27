package com.atscale.java.jdbc.cases;

import org.galaxio.gatling.javaapi.actions.QueryActionBuilder;

public class NamedQueryActionBuilder {
    public final QueryActionBuilder builder;
    public final String queryName;
    public final String inboundTextAsHash;
    public final String inboundTextAsBase64;
    public final String atscaleQueryId;
    public final String inboundQueryText;

    public NamedQueryActionBuilder(QueryActionBuilder builder, String queryName, String inboundTextAsHash, String inboundTextAsBase64, String atscaleQueryId, String inboundQueryText) {
        this.builder = builder;
        this.queryName = queryName;
        this.inboundTextAsHash = inboundTextAsHash;
        this.inboundTextAsBase64 = inboundTextAsBase64;
        this.atscaleQueryId = atscaleQueryId;
        this.inboundQueryText = inboundQueryText;
    }

    public String getInboundQueryTextAsBase64() {
        if (inboundTextAsBase64 != null && !inboundTextAsBase64.isEmpty()) return inboundTextAsBase64;
        return java.util.Base64.getEncoder().encodeToString(inboundQueryText.getBytes());
    }
}

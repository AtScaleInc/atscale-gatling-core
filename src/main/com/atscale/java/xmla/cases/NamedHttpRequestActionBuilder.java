package com.atscale.java.xmla.cases;

import io.gatling.javaapi.http.HttpRequestActionBuilder;

public class NamedHttpRequestActionBuilder {
    public final HttpRequestActionBuilder builder;
    public final String queryName;
    public final String inboundTextAsMd5Hash;

    public NamedHttpRequestActionBuilder(HttpRequestActionBuilder builder, String queryName, String inboundTextAsMd5Hash) {
        this.builder = builder;
        this.queryName = queryName;
        this.inboundTextAsMd5Hash = inboundTextAsMd5Hash;
    }
}

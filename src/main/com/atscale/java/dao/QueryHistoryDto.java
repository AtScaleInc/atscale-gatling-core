package com.atscale.java.dao;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.atscale.java.utils.HashUtil;
import org.apache.commons.lang3.StringUtils;

@SuppressWarnings("unused")
public class QueryHistoryDto {
    private String queryName;
    private String service;
    private String queryLanguage;
    private String inboundText;
    private String inboundTextAsHash;
    private String outboundText;
    private String cubeName;
    private String projectId;
    private boolean aggregateUsed;
    private int numTimes;
    private Double elapsedTimeInSeconds;
    private int avgResultSetSize;
    private String atscaleQueryId;

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getQueryLanguage() {
        return queryLanguage;
    }

    public void setQueryLanguage(String queryLanguage) {
        this.queryLanguage = queryLanguage;
    }

    public String getInboundText() {
        return inboundText;
    }

    public String getInboundTextAsHash() {
        return StringUtils.isEmpty(inboundTextAsHash)? HashUtil.TO_SHA256(inboundText) : inboundTextAsHash;
    }

    @JsonIgnore
    public String getInboundTextAsBase64() {
        return this.inboundText == null ? null : java.util.Base64.getEncoder()
                .encodeToString(this.inboundText.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    public void setInboundText(String inboundText) {
        this.inboundText = inboundText;
        this.inboundTextAsHash = HashUtil.TO_SHA256(inboundText);
    }

    public String getOutboundText() {
        return outboundText;
    }

    public void setOutboundText(String outboundText) {
        this.outboundText = outboundText;
    }

    public String getCubeName() {
        return cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public boolean isAggregateUsed() {
        return aggregateUsed;
    }

    public void setAggregateUsed(boolean aggregateUsed) {
        this.aggregateUsed = aggregateUsed;
    }

    public int getNumTimes() {
        return numTimes;
    }

    public void setNumTimes(int numTimes) {
        this.numTimes = numTimes;
    }

    public Double getElapsedTimeInSeconds() {
        return elapsedTimeInSeconds;
    }

    public void setElapsedTimeInSeconds(Double elapsedTimeInSeconds) {
        this.elapsedTimeInSeconds = elapsedTimeInSeconds;
    }

    public int getAvgResultSetSize() {
        return avgResultSetSize;
    }

    public void setAvgResultSetSize(int avgResultSetSize) {
        this.avgResultSetSize = avgResultSetSize;
    }

    public String getAtscaleQueryId() {
        return atscaleQueryId;
    }
    public void setAtscaleQueryId(String atscaleQueryId) {
        this.atscaleQueryId = atscaleQueryId;
    }

    @Override
    public String toString() {
        return "QueryHistoryDto{" +
                "service='" + service + '\'' +
                ", queryLanguage='" + queryLanguage + '\'' +
                ", inboundText='" + inboundText + '\'' +
                ", inboundTextAsHash='" + inboundTextAsHash + '\'' +
                ", outboundText='" + outboundText + '\'' +
                ", cubeName='" + cubeName + '\'' +
                ", projectId='" + projectId + '\'' +
                ", aggregateUsed=" + aggregateUsed +
                ", numTimes=" + numTimes +
                ", elapsedTimeInSeconds=" + elapsedTimeInSeconds +
                ", avgResultSetSize=" + avgResultSetSize +
                ", atscaleQueryId='" + atscaleQueryId + '\'' +
                '}';
    }

    public String toJson() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error converting QueryHistoryDto to JSON", e);
        }
    }

    public static QueryHistoryDto fromJson(String json) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(json, QueryHistoryDto.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error converting JSON to QueryHistoryDto", e);
        }
    }

    /**
     * Replace placeholders in this DTO's inboundText.
     * Replaces occurrences of "${CatalogName}" and "${ModelName}" with the provided
     * values wrapped in doubled double-quotes (for example: ""myCatalog"").
     * If inboundText is null this method is a no-op.
     *
     * @param catalog the catalog value to inject (may be null)
     * @param model the model value to inject (may be null)
     */
    public void bindJdbc(String catalog, String model) {
        if (this.inboundText == null) {
            return;
        }

        // Build the replacement string: if null => empty string, otherwise a single quoted value like "value"
        String catalogReplacement = catalog == null ? "" : "\"" + catalog + "\""; // produces "value"
        String modelReplacement = model == null ? "" : "\"" + model + "\"";

        // First replace quoted placeholders (i.e. "${CatalogName}" ) so we don't end up with double quotes
        // after replacing the unquoted placeholder variant.
        this.inboundText = this.inboundText.replace("\"${CatalogName}\"", catalogReplacement)
                                           .replace("\"${ModelName}\"", modelReplacement)
                                           // Then replace any remaining unquoted placeholders
                                           .replace("${CatalogName}", catalogReplacement)
                                           .replace("${ModelName}", modelReplacement);

        // Update hash to reflect changed inboundText
        this.inboundTextAsHash = HashUtil.TO_SHA256(this.inboundText);
    }


    public void bindXmla(String catalog, String model) {
        if (this.inboundText == null) {
            return;
        }

        this.inboundText = this.inboundText
                .replace("${CatalogName}", catalog)
                .replace("${ModelName}", model);

        this.inboundTextAsHash = HashUtil.TO_SHA256(this.inboundText);

    }


}

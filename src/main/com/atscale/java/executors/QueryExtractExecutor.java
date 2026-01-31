package com.atscale.java.executors;

import com.atscale.java.dao.AtScalePostgresDao;
import com.atscale.java.utils.AwsSecretsManager;
import com.atscale.java.utils.PropertiesManager;
import com.atscale.java.utils.QueryHistoryFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class QueryExtractExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryExtractExecutor.class);

    static {
        com.atscale.java.utils.Log4jShutdown.installHook();
    }

    public static void main(String[] args) {
        QueryExtractExecutor executor = new QueryExtractExecutor();
        executor.execute();
    }

    protected void execute() {
        LOGGER.info("QueryExtractExecutor started.");
        initializeAdditionalProperties();
        List<String> models = PropertiesManager.getAtScaleModels();
        AtScalePostgresDao dao = AtScalePostgresDao.getInstance();
        QueryHistoryFileUtil queryHistoryFileUtil = new QueryHistoryFileUtil(dao);

        for(String model: models) {
            queryHistoryFileUtil.cacheQueries(model);
        }

        LOGGER.info("QueryExtractExecutor finished.");
    }

    protected void initializeAdditionalProperties() {
            String regionProperty = "aws.region";
            String secretsKeyProperty = "aws.secrets-key";
            if(PropertiesManager.hasProperty(regionProperty) && PropertiesManager.hasProperty(secretsKeyProperty)) {
                LOGGER.info("Loading additional properties from AWS Secrets Manager.");
                String region = PropertiesManager.getCustomProperty(regionProperty);
                String secretsKey = PropertiesManager.getCustomProperty(secretsKeyProperty);
                PropertiesManager.setCustomProperties(new AwsSecretsManager().loadSecrets(region, secretsKey));
            } else {
                LOGGER.warn("AWS region or secret-key property not found. Skipping loading additional properties from AWS Secrets Manager.");
            }
    }
}

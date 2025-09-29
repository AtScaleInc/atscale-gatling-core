package com.atscale.java.executors;

import com.atscale.java.dao.AtScalePostgresDao;
import com.atscale.java.utils.QueryHistoryFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.atscale.java.utils.PropertiesFileReader;
import java.util.List;


public class QueryExtractExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryExtractExecutor.class);

    public static void main(String[] args) {
        QueryExtractExecutor executor = new QueryExtractExecutor();
        executor.execute();
    }

    protected void execute() {
        LOGGER.info("QueryExtractExecutor started.");
        List<String> models = PropertiesFileReader.getAtScaleModels();
        AtScalePostgresDao dao = AtScalePostgresDao.getInstance();
        QueryHistoryFileUtil queryHistoryFileUtil = new QueryHistoryFileUtil(dao);
        for(String model: models) {
            queryHistoryFileUtil.cacheQueries(model);
        }

        org.apache.logging.log4j.LogManager.shutdown();

        LOGGER.info("QueryExtractExecutor finished.");
    }
}

package com.atscale.java.executors;

import com.atscale.java.injectionsteps.OpenStep;
import com.atscale.java.utils.AdditionalPropertiesLoader;
import com.atscale.java.utils.MavenTaskJsonUtil;
import com.atscale.java.utils.MavenTaskUtil;
import com.atscale.java.utils.MavenTaskYamlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class OpenStepConcurrentTaskExecutor extends ConcurrentSimulationExecutor<OpenStep> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenStepConcurrentTaskExecutor.class);
    private List<MavenTaskDto<OpenStep>> simulationTasks;

    public static void main(String[] args) {
        LOGGER.info("ConcurrentTaskExecutor started.");

        if(args.length != 1) {
            LOGGER.error("Missing TaskFile.  Expected the TaskFile name to be passed as the only argument.");
            return;
        }

        String fileName = args[0];
        if (MavenTaskUtil.taskFileNotExists(fileName)){
            LOGGER.error("The specified TaskFile does not exist: {}", fileName);
            return;
        }

        OpenStepConcurrentTaskExecutor executor = new OpenStepConcurrentTaskExecutor();
        if(fileName.endsWith(".yaml") || fileName.endsWith(".yml")) {
            LOGGER.info("Reading tasks from YAML file: {}", fileName);
            executor.simulationTasks = MavenTaskYamlUtil.readOpenStepTasksFromFile(fileName);
            LOGGER.info("Loaded {} tasks from YAML file.", executor.simulationTasks.size());
        } else {
            LOGGER.info("Reading tasks from JSON file: {}", fileName);
            executor.simulationTasks = MavenTaskJsonUtil.readOpenStepTasksFromFile(fileName);
            LOGGER.info("Loaded {} tasks from JSON file.", executor.simulationTasks.size());
        }
        executor.execute();
        LOGGER.info("ConcurrentTaskExecutor completed.");
    }

    @Override
    protected List<MavenTaskDto<OpenStep>> getSimulationTasks() {
        return withAdditionalProperties(simulationTasks);
    }

    private List<MavenTaskDto<OpenStep>> withAdditionalProperties(List<MavenTaskDto<OpenStep>> tasks) {
        Map<String, String> additionalProperties = getAdditionalProperties();
        for(MavenTaskDto<OpenStep> task : tasks) {
            task.setAdditionalProperties(additionalProperties);
        }
        return tasks;
    }

    protected Map<String, String> getAdditionalProperties() {
        AdditionalPropertiesLoader loader = new AdditionalPropertiesLoader();
        return loader.fetchAdditionalProperties(AdditionalPropertiesLoader.SecretsManagerType.AWS);
    }
}

package com.atscale.java.executors;

import com.atscale.java.injectionsteps.ClosedStep;
import com.atscale.java.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ClosedStepConcurrentTaskExecutor extends ConcurrentSimulationExecutor<ClosedStep> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClosedStepConcurrentTaskExecutor.class);
    private List<MavenTaskDto<ClosedStep>> simulationTasks;

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

        ClosedStepConcurrentTaskExecutor executor = new ClosedStepConcurrentTaskExecutor();
        if(fileName.endsWith(".yaml") || fileName.endsWith(".yml")) {
            LOGGER.info("Reading tasks from YAML file: {}", fileName);
            executor.simulationTasks = MavenTaskYamlUtil.readClosedStepTasksFromFile(fileName);
            LOGGER.info("Loaded {} tasks from YAML file.", executor.simulationTasks.size());
        } else {
            LOGGER.info("Reading tasks from JSON file: {}", fileName);
            executor.simulationTasks = MavenTaskJsonUtil.readClosedStepTasksFromFile(fileName);
            LOGGER.info("Loaded {} tasks from JSON file.", executor.simulationTasks.size());
        }
        executor.execute();
        LOGGER.info("ConcurrentTaskExecutor completed.");
    }

    @Override
    @Override
    protected List<MavenTaskDto<ClosedStep>> getSimulationTasks() {
        return withAdditionalProperties(simulationTasks);
    }

    private List<MavenTaskDto<ClosedStep>> withAdditionalProperties(List<MavenTaskDto<ClosedStep>> tasks) {
        Map<String, String> additionalProperties = getAdditionalProperties();
        for(MavenTaskDto<ClosedStep> task : tasks) {
            task.setAdditionalProperties(additionalProperties);
        }
        return tasks;
    }

    protected Map<String, String> getAdditionalProperties() {
        AdditionalPropertiesLoader loader = new AdditionalPropertiesLoader();
        return loader.fetchAdditionalProperties(AdditionalPropertiesLoader.SecretsManagerType.AWS);
    }
}

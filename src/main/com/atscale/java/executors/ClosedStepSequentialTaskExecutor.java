package com.atscale.java.executors;

import com.atscale.java.injectionsteps.ClosedStep;
import com.atscale.java.utils.AdditionalPropertiesLoader;
import com.atscale.java.utils.MavenTaskJsonUtil;
import com.atscale.java.utils.MavenTaskUtil;
import com.atscale.java.utils.MavenTaskYamlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ClosedStepSequentialTaskExecutor extends SequentialSimulationExecutor<ClosedStep> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClosedStepSequentialTaskExecutor.class);
    private List<MavenTaskDto<ClosedStep>> simulationTasks;

    public static void main(String[] args) {
        LOGGER.info("SequentialTaskExecutor started.");

        if(args.length != 1) {
            LOGGER.error("Missing TaskFile.  Expected the TaskFile name to be passed as the only argument.");
            return;
        }

        String fileName = args[0];
        if (MavenTaskUtil.taskFileNotExists(fileName)){
            LOGGER.error("The specified TaskFile does not exist: {}", fileName);
            return;
        }

        ClosedStepSequentialTaskExecutor executor = new ClosedStepSequentialTaskExecutor();
        if(fileName.endsWith(".yaml") || fileName.endsWith(".yml")) {
            LOGGER.info("Reading tasks from YAML file: {}", fileName);
            executor.simulationTasks = MavenTaskYamlUtil.readClosedStepTasksFromFile(fileName);
        } else {
            LOGGER.info("Reading tasks from JSON file: {}", fileName);
            executor.simulationTasks = MavenTaskJsonUtil.readClosedStepTasksFromFile(fileName);
        }
        executor.execute();
        LOGGER.info("SequentialTaskExecutor completed.");
    }

    protected List<MavenTaskDto<ClosedStep>> getSimulationTasks() {
        return withAdditionalProperties(simulationTasks);
    }

    private List<MavenTaskDto<ClosedStep>> withAdditionalProperties(List<MavenTaskDto<ClosedStep>> tasks) {
        Map<String, String> additionalProperties = getAdditionalProperties();
        for(MavenTaskDto<ClosedStep> task : tasks) {
            task.getAdditionalProperties().putAll(additionalProperties);
        }
        return tasks;
    }

    protected Map<String, String> getAdditionalProperties() {
        AdditionalPropertiesLoader loader = new AdditionalPropertiesLoader();
        return loader.fetchAdditionalProperties(AdditionalPropertiesLoader.SecretsManagerType.AWS);
    }
}

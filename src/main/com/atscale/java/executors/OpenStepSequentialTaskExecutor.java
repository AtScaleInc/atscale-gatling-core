package com.atscale.java.executors;

import com.atscale.java.injectionsteps.OpenStep;
import com.atscale.java.utils.MavenTaskJsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

public class OpenStepSequentialTaskExecutor extends SequentialSimulationExecutor<OpenStep> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenStepSequentialTaskExecutor.class);
    private List<MavenTaskDto<OpenStep>> simulationTasks;

    public static void main(String[] args) {
        LOGGER.info("SequentialTaskExecutor started.");

        if(args.length != 1) {
            LOGGER.error("Missing TaskFile.  Expected the TaskFile name to be passed as the only argument.");
            return;
        }

        String fileName = args[0];
        if (! MavenTaskJsonUtil.taskFileExists(fileName)){
            LOGGER.error("The specified TaskFile does not exist: {}", fileName);
            return;
        }

        OpenStepSequentialTaskExecutor executor = new OpenStepSequentialTaskExecutor();
        executor.simulationTasks = MavenTaskJsonUtil.readOpenStepTasksFromFile(fileName);
        executor.execute();
        LOGGER.info("SequentialTaskExecutor completed.");
    }


    protected List<MavenTaskDto<OpenStep>> getSimulationTasks() {
        return simulationTasks;
    }
}

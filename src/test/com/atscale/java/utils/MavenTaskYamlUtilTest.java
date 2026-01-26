package com.atscale.java.utils;

import com.atscale.java.executors.MavenTaskDto;
import com.atscale.java.injectionsteps.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.apache.commons.lang3.builder.EqualsBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class MavenTaskYamlUtilTest {

    @Test
    public void testShouldHandleEmptyList() {
        List <MavenTaskDto<OpenStep>> tasks = new ArrayList<>();
        String yaml = MavenTaskYamlUtil.openStepTasksToYaml(tasks);

        assertNotNull(yaml);
        // YAML representation of an empty list should be present and parseable
        List<MavenTaskDto<OpenStep>> parsed = MavenTaskYamlUtil.openStepTasksFromYaml(yaml);
        assertNotNull(parsed);
        assertEquals(0, parsed.size());
    }

    @Test
    public void testShouldConvertSparselyPopulatedSingleOpenStepTaskToYaml() {
        List<OpenStep> injectionSteps = new ArrayList<>();
        injectionSteps.add(new AtOnceUsersOpenInjectionStep(1));

        List<MavenTaskDto<OpenStep>> tasks = new ArrayList<>();
        MavenTaskDto<OpenStep> task = new MavenTaskDto<>("Test Task");
        task.setMavenCommand("mvn test");
        task.setSimulationClass("com.example.TestSimulation");
        task.setInjectionSteps(injectionSteps);
        tasks.add(task);

        String yaml = MavenTaskYamlUtil.openStepTasksToYaml(tasks);
        assertNotNull(yaml);

        List<MavenTaskDto<OpenStep>> parsedTasks = MavenTaskYamlUtil.openStepTasksFromYaml(yaml);
        assertTrue(areEqual(tasks, parsedTasks));
    }

    @Test
    public void testShouldConvertSparselyPopulatedSingleClosedStepTaskToYaml() {
        List<ClosedStep> injectionSteps = new ArrayList<>();
        injectionSteps.add(new ConstantConcurrentUsersClosedInjectionStep(5, 5));

        List<MavenTaskDto<ClosedStep>> tasks = new ArrayList<>();
        MavenTaskDto<ClosedStep> task = new MavenTaskDto<>("Test Task");
        task.setMavenCommand("mvn test");
        task.setSimulationClass("com.example.TestSimulation");
        task.setInjectionSteps(injectionSteps);
        tasks.add(task);

        String yaml = MavenTaskYamlUtil.closedStepTasksToYaml(tasks);
        assertNotNull(yaml);

        List<MavenTaskDto<ClosedStep>> parsedTasks = MavenTaskYamlUtil.closedStepTasksFromYaml(yaml);
        assertTrue(areEqual(tasks, parsedTasks));
    }

    @Test
    public void testShouldConvertFullyPopulatedSingleOpenStepTaskToYaml() {
        List<OpenStep> injectionSteps = new ArrayList<>();
        injectionSteps.add(new AtOnceUsersOpenInjectionStep(1));

        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put("property1", "value1");
        additionalProperties.put("property2", "value2");

        List<MavenTaskDto<OpenStep>> tasks = new ArrayList<>();
        MavenTaskDto<OpenStep> task = new MavenTaskDto<>("Test Task");
        task.setMavenCommand("mvn test");
        task.setSimulationClass("com.example.TestSimulation");
        task.setInjectionSteps(injectionSteps);
        task.setIngestionFileName("test_ingestion.csv", true);
        task.setAlternatePropertiesFileName("systems.properties");
        task.setAdditionalProperties(additionalProperties);
        task.setMavenCommand("mvn clean test -DsomeFlag=true");
        task.setLoggingAsAppend(true);
        task.setRunId(RandomStringUtils.secure().nextAlphanumeric(12));
        task.setRunLogFileName(String.format("gatling-%s.log", task.getRunId()));
        task.setRunDescription("This is a test run description.");
        tasks.add(task);

        String yaml = MavenTaskYamlUtil.openStepTasksToYaml(tasks);
        assertNotNull(yaml);

        List<MavenTaskDto<OpenStep>> parsedTasks = MavenTaskYamlUtil.openStepTasksFromYaml(yaml);
        assertTrue(areEqual(tasks, parsedTasks));
    }

    @Test
    public void testShouldConvertFullyPopulatedSingleClosedStepTaskToYaml() {
        List<ClosedStep> injectionSteps = new ArrayList<>();
        injectionSteps.add(new ConstantConcurrentUsersClosedInjectionStep(5, 5));

        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put("property1", "value1");
        additionalProperties.put("property2", "value2");

        List<MavenTaskDto<ClosedStep>> tasks = new ArrayList<>();
        MavenTaskDto<ClosedStep> task = new MavenTaskDto<>("Test Task");
        task.setMavenCommand("mvn test");
        task.setSimulationClass("com.example.TestSimulation");
        task.setInjectionSteps(injectionSteps);
        task.setIngestionFileName("test_ingestion.csv", true);
        task.setAlternatePropertiesFileName("systems.properties");
        task.setAdditionalProperties(additionalProperties);
        task.setMavenCommand("mvn clean test -DsomeFlag=true");
        task.setLoggingAsAppend(true);
        task.setRunId(RandomStringUtils.secure().nextAlphanumeric(12));
        task.setRunLogFileName(String.format("gatling-%s.log", task.getRunId()));
        task.setRunDescription("This is a test run description.");
        tasks.add(task);

        String yaml = MavenTaskYamlUtil.closedStepTasksToYaml(tasks);
        assertNotNull(yaml);

        List<MavenTaskDto<ClosedStep>> parsedTasks = MavenTaskYamlUtil.closedStepTasksFromYaml(yaml);
        assertTrue(areEqual(tasks, parsedTasks));
    }

    @Test
    public void testOpenInjectionStepFileSaveAndLoad() {
        List<OpenStep> injectionSteps = new ArrayList<>();
        injectionSteps.add(new AtOnceUsersOpenInjectionStep(1));
        injectionSteps.add(new RampUsersOpenInjectionStep(10, 12));
        injectionSteps.add(new NothingForOpenInjectionStep(3));
        injectionSteps.add(new ConstantUsersPerSecondOpenInjectionStep(15, 17));
        injectionSteps.add(new RampUsersPerSecOpenInjectionStep(5, 10, 8));
        injectionSteps.add(new StressPeakUsersOpenInjectionStep(13, 19));

        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put("property1", "value1");
        additionalProperties.put("property2", "value2");

        List<MavenTaskDto<OpenStep>> tasks = new ArrayList<>();
        MavenTaskDto<OpenStep> task = new MavenTaskDto<>("Test Task");
        task.setMavenCommand("mvn test");
        task.setSimulationClass("com.example.TestSimulation");
        task.setInjectionSteps(injectionSteps);
        task.setIngestionFileName("test_ingestion.csv", true);
        task.setAlternatePropertiesFileName("systems.properties");
        task.setAdditionalProperties(additionalProperties);
        task.setMavenCommand("mvn clean test -DsomeFlag=true");
        task.setLoggingAsAppend(true);
        task.setRunId(RandomStringUtils.secure().nextAlphanumeric(12));
        task.setRunLogFileName(String.format("gatling-%s.log", task.getRunId()));
        task.setRunDescription("This is a test run description.");

        MavenTaskDto<OpenStep> task2 = task.copy("Test Task 2");
        MavenTaskDto<OpenStep> task3 = task.copy("Test Task 3");

        tasks.add(task);
        tasks.add(task2);
        tasks.add(task3);

        String fileName = RandomStringUtils.secure().nextAlphabetic(10) + ".yaml";
        MavenTaskYamlUtil.writeOpenStepTasksToFile(fileName, tasks);

        List<MavenTaskDto<OpenStep>> loadedTasks = MavenTaskYamlUtil.readOpenStepTasksFromFile(fileName);
        assertTrue(areEqual(tasks, loadedTasks));
        assertEquals(3, loadedTasks.size());

        MavenTaskYamlUtil.deleteTaskFile(fileName);
    }

    @Test
    public void testClosedInjectionStepFileSaveAndLoad() {
        List<ClosedStep> injectionSteps = new ArrayList<>();
        injectionSteps.add(new ConstantConcurrentUsersClosedInjectionStep(5, 5));
        injectionSteps.add(new IncrementConcurrentUsersClosedInjectionStep(10, 2, 10, 5, 7));
        injectionSteps.add(new RampConcurrentUsersClosedInjectionStep(100, 5, 9));
        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put("property1", "value1");
        additionalProperties.put("property2", "value2");

        List<MavenTaskDto<ClosedStep>> tasks = new ArrayList<>();
        MavenTaskDto<ClosedStep> task = new MavenTaskDto<>("Test Task");
        task.setMavenCommand("mvn test");
        task.setSimulationClass("com.example.TestSimulation");
        task.setInjectionSteps(injectionSteps);
        task.setIngestionFileName("test_ingestion.csv", true);
        task.setAlternatePropertiesFileName("systems.properties");
        task.setAdditionalProperties(additionalProperties);
        task.setMavenCommand("mvn clean test -DsomeFlag=true");
        task.setLoggingAsAppend(true);
        task.setRunId(RandomStringUtils.secure().nextAlphanumeric(12));
        task.setRunLogFileName(String.format("gatling-%s.log", task.getRunId()));
        task.setRunDescription("This is a test run description.");
        MavenTaskDto<ClosedStep> task2 = task.copy("Test Task 2");
        MavenTaskDto<ClosedStep> task3 = task.copy("Test Task 3");

        tasks.add(task);
        tasks.add(task2);
        tasks.add(task3);

        String fileName = RandomStringUtils.secure().nextAlphabetic(10) + ".yaml";
        MavenTaskYamlUtil.writeClosedStepTasksToFile(fileName, tasks);

        List<MavenTaskDto<ClosedStep>> loadedTasks = MavenTaskYamlUtil.readClosedStepTasksFromFile(fileName);
        assertTrue(areEqual(tasks, loadedTasks));
        assertEquals(3, loadedTasks.size());

        MavenTaskYamlUtil.deleteTaskFile(fileName);
    }

    private boolean areEqual(List<? extends MavenTaskDto<?>> expected,
                             List<? extends MavenTaskDto<?>> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }
        for (int i = 0; i < expected.size(); i++) {
            MavenTaskDto<?> expectedTask = expected.get(i);
            MavenTaskDto<?> actualTask = actual.get(i);
            if (!EqualsBuilder.reflectionEquals(expectedTask, actualTask)) {
                return false;
            }
        }
        return true;
    }

}

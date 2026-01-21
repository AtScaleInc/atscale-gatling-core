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

public class MavenTaskJsonUtilTest {

    @Test
    public void testShouldHandleEmptyList() {
        List <MavenTaskDto<OpenStep>> tasks = new ArrayList<>();
        String json = MavenTaskJsonUtil.openStepTasksToJson(tasks);

        assertNotNull(json);
        assertEquals("[ ]", json);
    }

    @Test
    public void testShouldConvertSparselyPopulatedSingleOpenStepTaskToJson() {
        List<OpenStep> injectionSteps = new ArrayList<>();
        injectionSteps.add(new AtOnceUsersOpenInjectionStep(1));

        List<MavenTaskDto<OpenStep>> tasks = new ArrayList<>();
        MavenTaskDto<OpenStep> task = new MavenTaskDto<>("Test Task");
        task.setMavenCommand("mvn test");
        task.setSimulationClass("com.example.TestSimulation");
        task.setInjectionSteps(injectionSteps);
        tasks.add(task);

        String json = MavenTaskJsonUtil.openStepTasksToJson(tasks);
        assertNotNull(json);

        List<MavenTaskDto<OpenStep>> parsedTasks = MavenTaskJsonUtil.openStepTasksFromJson(json);
        assertTrue(areEqual(tasks, parsedTasks));
    }

    @Test
    public void testShouldConvertSparselyPopulatedSingleClosedStepTaskToJson() {
        List<ClosedStep> injectionSteps = new ArrayList<>();
        injectionSteps.add(new ConstantConcurrentUsersClosedInjectionStep(5, 5));

        List<MavenTaskDto<ClosedStep>> tasks = new ArrayList<>();
        MavenTaskDto<ClosedStep> task = new MavenTaskDto<>("Test Task");
        task.setMavenCommand("mvn test");
        task.setSimulationClass("com.example.TestSimulation");
        task.setInjectionSteps(injectionSteps);
        tasks.add(task);

        String json = MavenTaskJsonUtil.closedStepTasksToJson(tasks);
        assertNotNull(json);

        List<MavenTaskDto<ClosedStep>> parsedTasks = MavenTaskJsonUtil.closedStepTasksFromJson(json);
        assertTrue(areEqual(tasks, parsedTasks));
    }

    @Test
    public void testShouldConvertFullyPopulatedSingleOpenStepTaskToJson() {
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

        String json = MavenTaskJsonUtil.openStepTasksToJson(tasks);
        assertNotNull(json);

        List<MavenTaskDto<OpenStep>> parsedTasks = MavenTaskJsonUtil.openStepTasksFromJson(json);
        assertTrue(areEqual(tasks, parsedTasks));
    }

    @Test
    public void testShouldConvertFullyPopulatedSingleClosedStepTaskToJson() {
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

        String json = MavenTaskJsonUtil.closedStepTasksToJson(tasks);
        assertNotNull(json);

        List<MavenTaskDto<ClosedStep>> parsedTasks = MavenTaskJsonUtil.closedStepTasksFromJson(json);
        assertTrue(areEqual(tasks, parsedTasks));
    }

    @Test
    public void testShouldConvertFullyPopulatedAllOpenStepTaskToJson() {
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
        tasks.add(task);

        String json = MavenTaskJsonUtil.openStepTasksToJson(tasks);
        assertNotNull(json);
        assertTrue(json.contains("AtOnceUsersOpenInjectionStep"));
        assertTrue(json.contains("RampUsersOpenInjectionStep"));
        assertTrue(json.contains("NothingForOpenInjectionStep"));
        assertTrue(json.contains("ConstantUsersPerSecondOpenInjectionStep"));
        assertTrue(json.contains("RampUsersPerSecOpenInjectionStep"));
        assertTrue(json.contains("StressPeakUsersOpenInjectionStep"));
        assertTrue(json.contains("property1"));
        assertTrue(json.contains("value1"));
        assertTrue(json.contains("property2"));
        assertTrue(json.contains("value2"));

        List<MavenTaskDto<OpenStep>> parsedTasks = MavenTaskJsonUtil.openStepTasksFromJson(json);
        assertTrue(areEqual(tasks, parsedTasks));
    }

    @Test
    public void testShouldConvertFullyPopulatedAllClosedStepTaskToJson() {
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
        tasks.add(task);

        String json = MavenTaskJsonUtil.closedStepTasksToJson(tasks);
        assertNotNull(json);
        assertTrue(json.contains("IncrementConcurrentUsersClosedInjectionStep"));
        assertTrue(json.contains("RampConcurrentUsersClosedInjectionStep"));
        assertTrue(json.contains("ConstantConcurrentUsersClosedInjectionStep"));
        assertTrue(json.contains("property1"));
        assertTrue(json.contains("value1"));
        assertTrue(json.contains("property2"));
        assertTrue(json.contains("value2"));

        List<MavenTaskDto<ClosedStep>> parsedTasks = MavenTaskJsonUtil.closedStepTasksFromJson(json);
        assertTrue(areEqual(tasks, parsedTasks));
    }

    @Test
    public void testCorrectlyHandlesInjectionStepListLengths() {
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
        tasks.add(task);

        String json = MavenTaskJsonUtil.closedStepTasksToJson(tasks);
        List<MavenTaskDto<ClosedStep>> parsedTasks = MavenTaskJsonUtil.closedStepTasksFromJson(json);

        injectionSteps.remove(0);
        assertFalse(areEqual(tasks, parsedTasks));
    }

    @Test
    public void testCorrectlyHandlesAdditionalPropertyListLengths() {
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
        tasks.add(task);

        String json = MavenTaskJsonUtil.closedStepTasksToJson(tasks);
        List<MavenTaskDto<ClosedStep>> parsedTasks = MavenTaskJsonUtil.closedStepTasksFromJson(json);

        Map<String, String> additionalProp = new HashMap<>();
        additionalProp.put("property3", "value3");
        task.setAdditionalProperties(additionalProp);
        assertFalse(areEqual(tasks, parsedTasks));
    }

    @Test
    public void testMarshallsProperties() {
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
        tasks.add(task);

        String json = MavenTaskJsonUtil.closedStepTasksToJson(tasks);
        List<MavenTaskDto<ClosedStep>> parsedTasks = MavenTaskJsonUtil.closedStepTasksFromJson(json);

        Map<String, String> additionalProp = new HashMap<>();
        additionalProp.put("property2", "breaksThings");
        task.setAdditionalProperties(additionalProp);

        assertFalse(areEqual(tasks, parsedTasks));
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

        String fileName = RandomStringUtils.secure().nextAlphabetic(10) + ".json";
        MavenTaskJsonUtil.writeOpenStepTasksToFile(fileName, tasks);

        List<MavenTaskDto<OpenStep>> loadedTasks = MavenTaskJsonUtil.readOpenStepTasksFromFile(fileName);
        assertTrue(areEqual(tasks, loadedTasks));
        assertEquals(3, loadedTasks.size());

        MavenTaskJsonUtil.deleteTaskFile(fileName);
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

        String fileName = RandomStringUtils.secure().nextAlphabetic(10) + ".json";
        MavenTaskJsonUtil.writeClosedStepTasksToFile(fileName, tasks);

        List<MavenTaskDto<ClosedStep>> loadedTasks = MavenTaskJsonUtil.readClosedStepTasksFromFile(fileName);
        assertTrue(areEqual(tasks, loadedTasks));
        assertEquals(3, loadedTasks.size());

        MavenTaskJsonUtil.deleteTaskFile(fileName);
    }

    @Test
    public void testOpenInjectionStepShouldSaveAndLoadEmptyLists() {
        List<OpenStep> injectionSteps = new ArrayList<>();
        Map<String, String> additionalProperties = new HashMap<>();

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

        String fileName = RandomStringUtils.secure().nextAlphabetic(10) + ".json";
        MavenTaskJsonUtil.writeOpenStepTasksToFile(fileName, tasks);

        List<MavenTaskDto<OpenStep>> loadedTasks = MavenTaskJsonUtil.readOpenStepTasksFromFile(fileName);
        assertTrue(areEqual(tasks, loadedTasks));
        assertEquals(3, loadedTasks.size());

        MavenTaskJsonUtil.deleteTaskFile(fileName);
    }

    @Test
    public void testClosedInjectionStepShouldSaveAndLoadEmptyLists() {
        List<ClosedStep> injectionSteps = new ArrayList<>();
        Map<String, String> additionalProperties = new HashMap<>();


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

        String fileName = RandomStringUtils.secure().nextAlphabetic(10) + ".json";
        MavenTaskJsonUtil.writeClosedStepTasksToFile(fileName, tasks);

        List<MavenTaskDto<ClosedStep>> loadedTasks = MavenTaskJsonUtil.readClosedStepTasksFromFile(fileName);
        assertTrue(areEqual(tasks, loadedTasks));
        assertEquals(3, loadedTasks.size());

        MavenTaskJsonUtil.deleteTaskFile(fileName);
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

package com.atscale.java.executors;

import com.atscale.java.utils.SecretsManager;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimulationExecutorTest {

    @Test
    public void testAdditionalPropertiesWithMockedSecretsManager() {
        SecretsManager mockUtil = Mockito.mock(SecretsManager.class);
        String testKey = "test-key";
        String testRegion = "us-west-2";
        Map<String, String> expected = Map.of("foo", "bar");

        Mockito.when(mockUtil.loadSecrets(Mockito.eq(testRegion), Mockito.eq(testKey))).thenReturn(expected);

        SimulationExecutor exec = new TestSimulationExecutor(mockUtil);
        Map<String, String> result = exec.additionalProperties(testRegion, testKey);

        assertEquals(expected, result);
        Mockito.verify(mockUtil, Mockito.times(1)).loadSecrets(Mockito.eq(testRegion), Mockito.eq(testKey));
    }

    public static class TestSimulationExecutor extends SimulationExecutor {
        private final SecretsManager util;

        public TestSimulationExecutor(SecretsManager util) {
            this.util = util;
        }

        @Override
        protected SecretsManager createSecretsManager() { return util; }
    }
}

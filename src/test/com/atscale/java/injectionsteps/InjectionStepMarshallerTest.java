package com.atscale.java.injectionsteps;

import io.gatling.javaapi.core.OpenInjectionStep;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import com.atscale.java.utils.InjectionStepJsonUtil;
import java.util.List;

public class InjectionStepMarshallerTest{

    @Test
    public void testOpenInjectionStepsAsJson() {
        // Create a sample OpenStep object
        OpenStep step = new AtOnceUsersOpenInjectionStep(5);

        // Convert to JSON
        String json = InjectionStepJsonUtil.openInjectionStepsAsJson(List.of(step));

        // Verify the JSON output
        assertNotNull(json);
        assertTrue(json.contains("\"users\":5"));
    }

    @Test
    public void testTwoOpenInjectionStepsFromJson() {
        OpenStep step = new AtOnceUsersOpenInjectionStep(5);
        OpenStep step2 = new RampUsersPerSecOpenInjectionStep(1, 5, 60);

        // Convert to JSON
        String json = InjectionStepJsonUtil.openInjectionStepsAsJson(List.of(step, step2));

        // Deserialize from JSON
        List<OpenStep> steps = InjectionStepJsonUtil.openInjectionStepsFromJson(json);
        assertNotNull(steps);
        assertEquals(2, steps.size());
        assertInstanceOf(AtOnceUsersOpenInjectionStep.class, steps.get(0));
        assertEquals(5, ((AtOnceUsersOpenInjectionStep) steps.get(0)).getUsers());
        assertInstanceOf(OpenInjectionStep.class, steps.get(0).toGatlingStep());
        assertInstanceOf(OpenInjectionStep.class, steps.get(1).toGatlingStep());
    }

    @Test
    public void testOpenInjectionStepsFromJson() {
        OpenStep step = new AtOnceUsersOpenInjectionStep(5);
        OpenStep step1 = new ConstantUsersPerSecondOpenInjectionStep(8, 10);
        OpenStep step2 = new NothingForOpenInjectionStep(7);
        OpenStep step3 = new RampUsersOpenInjectionStep(100, 15);
        OpenStep step4 = new RampUsersPerSecOpenInjectionStep(1, 5, 60);
        OpenStep step5 = new StressPeakUsersOpenInjectionStep(3, 15);

        // Convert to JSON
        String json = InjectionStepJsonUtil.openInjectionStepsAsJson(List.of(step, step1, step2, step3, step4, step5));

        // Deserialize from JSON
        List<OpenStep> steps = InjectionStepJsonUtil.openInjectionStepsFromJson(json);
        assertNotNull(steps);
        assertEquals(6, steps.size());
        assertInstanceOf(AtOnceUsersOpenInjectionStep.class, steps.get(0));
        assertEquals(5, ((AtOnceUsersOpenInjectionStep) steps.get(0)).getUsers());
        assertInstanceOf(OpenInjectionStep.class, steps.get(0).toGatlingStep());
        assertInstanceOf(OpenInjectionStep.class, steps.get(1).toGatlingStep());
        assertInstanceOf(OpenInjectionStep.class, steps.get(2).toGatlingStep());
        assertInstanceOf(OpenInjectionStep.class, steps.get(3).toGatlingStep());
        assertInstanceOf(OpenInjectionStep.class, steps.get(4).toGatlingStep());
        assertInstanceOf(OpenInjectionStep.class, steps.get(5).toGatlingStep());
    }

    @Test
    public void testClosedInjectionStepsAsJson() {
        // Create a sample ClosedStep object
        ClosedStep step1 = new IncrementConcurrentUsersClosedInjectionStep(3, 3, 5, 1, 1);
        ClosedStep step2 = new ConstantConcurrentUsersClosedInjectionStep(10, 2);
        ClosedStep step3 = new RampConcurrentUsersClosedInjectionStep(5, 10, 2);

        // Convert to JSON
        String json = InjectionStepJsonUtil.closedInjectionStepsAsJson(List.of(step1, step2, step3));

        // Deserialize from JSON
        List<ClosedStep> steps = InjectionStepJsonUtil.closedInjectionStepsFromJson(json);
        assertNotNull(steps);
        assertEquals(3, steps.size());
        assertInstanceOf(IncrementConcurrentUsersClosedInjectionStep.class, steps.get(0));
        assertInstanceOf(ConstantConcurrentUsersClosedInjectionStep.class, steps.get(1));
        assertInstanceOf(RampConcurrentUsersClosedInjectionStep.class, steps.get(2));
    }

}

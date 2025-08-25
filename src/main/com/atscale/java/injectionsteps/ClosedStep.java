package com.atscale.java.injectionsteps;

import io.gatling.javaapi.core.ClosedInjectionStep;
import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonSubTypes;

@SuppressWarnings("unused")
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = RampConcurrentUsersClosedInjectionStep.class, name = "RampConcurrentUsersClosedInjectionStep"),
        @JsonSubTypes.Type(value = ConstantConcurrentUsersClosedInjectionStep.class, name = "ConstantConcurrentUsersClosedInjectionStep"),
        @JsonSubTypes.Type(value = IncrementConcurrentUsersClosedInjectionStep.class, name = "IncrementConcurrentUsersClosedInjectionStep")
})

public interface ClosedStep extends Serializable {
    ClosedInjectionStep toGatlingStep();

    default String getType() {
        return this.getClass().getSimpleName();
    }
}

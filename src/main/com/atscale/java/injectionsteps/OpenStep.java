package com.atscale.java.injectionsteps;

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
        @JsonSubTypes.Type(value = AtOnceUsersOpenInjectionStep.class, name = "AtOnceUsersOpenInjectionStep"),
        @JsonSubTypes.Type(value = ConstantUsersPerSecondOpenInjectionStep.class, name = "ConstantUsersPerSecondOpenInjectionStep"),
        @JsonSubTypes.Type(value = NothingForOpenInjectionStep.class, name = "NothingForOpenInjectionStep"),
        @JsonSubTypes.Type(value = RampUsersOpenInjectionStep.class, name = "RampUsersOpenInjectionStep"),
        @JsonSubTypes.Type(value = RampUsersPerSecOpenInjectionStep.class, name = "RampUsersPerSecOpenInjectionStep"),
        @JsonSubTypes.Type(value = StressPeakUsersOpenInjectionStep.class, name = "StressPeakUsersOpenInjectionStep")
})
public interface OpenStep extends Serializable {
    io.gatling.javaapi.core.OpenInjectionStep toGatlingStep();

    default String getType() {
        return this.getClass().getSimpleName();
    }
}
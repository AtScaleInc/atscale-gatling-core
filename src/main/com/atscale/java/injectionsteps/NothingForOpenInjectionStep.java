package com.atscale.java.injectionsteps;

import io.gatling.javaapi.core.OpenInjectionStep;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import static io.gatling.javaapi.core.CoreDsl.nothingFor;

@SuppressWarnings("unused")
public class NothingForOpenInjectionStep implements OpenStep {
    private long durationMinutes;

    @SuppressWarnings("unused")
    public NothingForOpenInjectionStep() {
        this.durationMinutes = 1;
    }

    public NothingForOpenInjectionStep(long durationMinutes) {
        this.durationMinutes = durationMinutes;
    }

    public long getDurationMinutes() {
        return durationMinutes;
    }

    public void setDurationMinutes(long durationMinutes) {
        this.durationMinutes = durationMinutes;
    }

    @Override
    public OpenInjectionStep toGatlingStep() {
        return nothingFor(java.time.Duration.ofMinutes(durationMinutes));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }
}

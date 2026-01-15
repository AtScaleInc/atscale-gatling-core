package com.atscale.java.injectionsteps;

import io.gatling.javaapi.core.ClosedInjectionStep;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import static io.gatling.javaapi.core.CoreDsl.rampConcurrentUsers;
import java.time.Duration;

@SuppressWarnings("unused")
public class RampConcurrentUsersClosedInjectionStep implements ClosedStep {
    private int from;
    private int to;
    private long durationMinutes;

    @SuppressWarnings("unused")
    public RampConcurrentUsersClosedInjectionStep() {
        this.from = 1;
        this.to = 2;
        this.durationMinutes = 1;
    }

    public RampConcurrentUsersClosedInjectionStep(int from, int to, long durationMinutes) {
        this.from = from;
        this.to = to;
        this.durationMinutes = durationMinutes;
    }

    public int getFrom() { return from; }
    public void setFrom(int from) { this.from = from; }

    public int getTo() { return to; }
    public void setTo(int to) { this.to = to; }

    public long getDurationMinutes() { return durationMinutes; }
    public void setDurationMinutes(long durationMinutes) { this.durationMinutes = durationMinutes; }

    @Override
    public ClosedInjectionStep toGatlingStep() {
        return rampConcurrentUsers(from).to(to).during(Duration.ofMinutes(durationMinutes));
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
package com.atscale.java.injectionsteps;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.time.Duration;

import static io.gatling.javaapi.core.CoreDsl.rampUsersPerSec;

@SuppressWarnings("unused")
public class RampUsersPerSecOpenInjectionStep implements OpenStep {
    private int fromUsers;
    private int toUsers;
    private long durationMinutes;

    @SuppressWarnings("unused")
    public RampUsersPerSecOpenInjectionStep() {
        this.fromUsers = 1; // Default to 1 user if not specified
        this.toUsers = 5; // Default to ramping up to 5 users
        this.durationMinutes = 1; // Default duration of 1 minute
    }

    public RampUsersPerSecOpenInjectionStep(int fromUsers, int toUsers, long durationMinutes) {
        this.fromUsers = fromUsers;
        this.toUsers = toUsers;
        this.durationMinutes = durationMinutes;
    }

    public int getFromUsers() { return fromUsers; }
    public void setFromUsers(int users) { this.fromUsers = users; }

    public int getToUsers() { return toUsers; }
    public void setToUsers(int users) { this.toUsers = users; }

    public long getDurationMinutes() { return durationMinutes; }
    public void setDurationMinutes(long durationMinutes) { this.durationMinutes = durationMinutes; }

    @Override
    public io.gatling.javaapi.core.OpenInjectionStep toGatlingStep() {
        return rampUsersPerSec(fromUsers).to(toUsers).during(Duration.ofMinutes(durationMinutes));
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
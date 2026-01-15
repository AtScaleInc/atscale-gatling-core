package com.atscale.java.injectionsteps;

import io.gatling.javaapi.core.ClosedInjectionStep;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import static io.gatling.javaapi.core.CoreDsl.incrementConcurrentUsers;
import java.time.Duration;

@SuppressWarnings("unused")
public class IncrementConcurrentUsersClosedInjectionStep implements ClosedStep {
    private int initialUsers;
    private int additionalUsers;
    private int times;
    private long levelDurationMinutes;
    private long rampDurationMinutes;

    @SuppressWarnings("unused")
    public IncrementConcurrentUsersClosedInjectionStep() {
        this.initialUsers = 1;
        this.additionalUsers = 1;
        this.times = 1;
        this.levelDurationMinutes = 1;
        this.rampDurationMinutes = 1;
    }

    public IncrementConcurrentUsersClosedInjectionStep(int initialUsers, int additionalUsers, int times, long levelDurationMinutes, long rampDurationMinutes) {
        this.initialUsers = initialUsers;
        this.additionalUsers = additionalUsers;
        this.times = times;
        this.levelDurationMinutes = levelDurationMinutes;
        this.rampDurationMinutes = rampDurationMinutes;
    }

    public int getInitialUsers() { return initialUsers; }
    public void setInitialUsers(int initialUsers) { this.initialUsers = initialUsers; }

    public int getAdditionalUsers() { return additionalUsers; }
    public void setAdditionalUsers(int additionalUsers) { this.additionalUsers = additionalUsers; }

    public int getTimes() { return times; }
    public void setTimes(int times) { this.times = times; }

    public long getLevelDurationMinutes() { return levelDurationMinutes; }
    public void setLevelDurationMinutes(long levelDurationMinutes) { this.levelDurationMinutes = levelDurationMinutes; }

    public long getRampDurationMinutes() { return rampDurationMinutes; }
    public void setRampDurationMinutes(long rampDurationMinutes) { this.rampDurationMinutes = rampDurationMinutes; }

    @Override
    public ClosedInjectionStep toGatlingStep() {
        return incrementConcurrentUsers(additionalUsers)
                .times(times)
                .eachLevelLasting(Duration.ofMinutes(levelDurationMinutes))
                .separatedByRampsLasting(Duration.ofMinutes(rampDurationMinutes))
                .startingFrom(initialUsers);
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
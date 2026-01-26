package com.atscale.java.injectionsteps;

import io.gatling.javaapi.core.OpenInjectionStep;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import static io.gatling.javaapi.core.CoreDsl.rampUsers;
import java.time.Duration;

@SuppressWarnings("unused")
public class RampUsersOpenInjectionStep implements OpenStep {
    private int users;
    private long durationMinutes;

    @SuppressWarnings("unused")
    public RampUsersOpenInjectionStep() {
        this.users = 1;
        this.durationMinutes = 1;
    }

    public RampUsersOpenInjectionStep(int users, long durationMinutes) {
        this.users = users;
        this.durationMinutes = durationMinutes;
    }

    public int getUsers() { return users; }
    public void setUsers(int users) { this.users = users; }

    public long getDurationMinutes() { return durationMinutes; }
    public void setDurationMinutes(long durationMinutes) { this.durationMinutes = durationMinutes; }

    @Override
    public OpenInjectionStep toGatlingStep() {
        return rampUsers(users).during(Duration.ofMinutes(durationMinutes));
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

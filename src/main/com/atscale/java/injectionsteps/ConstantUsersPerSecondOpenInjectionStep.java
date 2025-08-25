package com.atscale.java.injectionsteps;

import io.gatling.javaapi.core.OpenInjectionStep;
import static io.gatling.javaapi.core.CoreDsl.constantUsersPerSec;

@SuppressWarnings("unused")
public class ConstantUsersPerSecondOpenInjectionStep implements OpenStep{
    private int users;
    private long durationMinutes;

    @SuppressWarnings("unused")
    public ConstantUsersPerSecondOpenInjectionStep() {
        this.users = 1;
        this.durationMinutes = 1;
    }
    public ConstantUsersPerSecondOpenInjectionStep(int users, long durationMinutes) {
        this.users = users; // Default to 1 user if not specified
        this.durationMinutes = durationMinutes;
    }

    public int getUsers() {
        return users;
    }

    public void setUsers(int users) {
        this.users = users;
    }

    public long getDurationMinutes() {
        return durationMinutes;
    }

    public void setDurationMinutes(long durationMinutes) {
        this.durationMinutes = durationMinutes;
    }

    @Override
    public OpenInjectionStep toGatlingStep() {
        return constantUsersPerSec(users).during(java.time.Duration.ofMinutes(durationMinutes));
    }
}

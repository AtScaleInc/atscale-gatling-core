package com.atscale.java.injectionsteps;

import io.gatling.javaapi.core.ClosedInjectionStep;
import static io.gatling.javaapi.core.CoreDsl.constantConcurrentUsers;
import java.time.Duration;

@SuppressWarnings("unused")
public class ConstantConcurrentUsersClosedInjectionStep implements ClosedStep {
    private int users;
    private long durationMinutes;

    @SuppressWarnings("unused")
    public ConstantConcurrentUsersClosedInjectionStep() {
        this.users = 1;
        this.durationMinutes = 1;
    }

    public ConstantConcurrentUsersClosedInjectionStep(int users, long durationMinutes) {
        this.users = users;
        this.durationMinutes = durationMinutes;
    }

    public int getUsers() { return users; }
    public void setUsers(int users) { this.users = users; }

    public long getDurationMinutes() { return durationMinutes; }
    public void setDurationMinutes(long durationMinutes) { this.durationMinutes = durationMinutes; }

    @Override
    public ClosedInjectionStep toGatlingStep() {
        return constantConcurrentUsers(users).during(Duration.ofMinutes(durationMinutes));
    }
}

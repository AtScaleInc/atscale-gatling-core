package com.atscale.java.injectionsteps;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

@SuppressWarnings("unused")
public class AtOnceUsersOpenInjectionStep implements OpenStep {
    private int users;

    @SuppressWarnings("unused")
    public AtOnceUsersOpenInjectionStep() {
        this.users = 1; // Default to 1 user if not specified
    }

    public AtOnceUsersOpenInjectionStep(int users) {
        this.users = users;
    }

    public int getUsers() { return users; }
    public void setUsers(int users) { this.users = users; }

    @Override
    public io.gatling.javaapi.core.OpenInjectionStep toGatlingStep() {
        return io.gatling.javaapi.core.OpenInjectionStep.atOnceUsers(users);
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

package com.atscale.java.utils;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Log4jShutdown {
    private static final AtomicBoolean SHUTDOWN_CALLED = new AtomicBoolean(false);
    private static final AtomicBoolean HOOK_INSTALLED = new AtomicBoolean(false);
    private static final Thread HOOK = new Thread(Log4jShutdown::shutdownNow, "log4j-shutdown-hook");

    private Log4jShutdown() { /* utility */ }

    /**
     * Install the JVM shutdown hook once per classloader. Returns true if installation occurred.
     * This uses CAS to claim installation, and rolls back the claim if addShutdownHook throws,
     * so concurrent callers won't both add hooks and failed installs can be retried.
     */
    public static void installHook() {
        if (!HOOK_INSTALLED.compareAndSet(false, true)) {
            return; // already installed or another thread is installing
        }
        try {
            Runtime.getRuntime().addShutdownHook(HOOK);
        } catch (IllegalStateException | SecurityException e) {
            // rollback so callers can retry later
            HOOK_INSTALLED.set(false);
            System.err.println("Failed to install log4j shutdown hook: " + e.getClass().getName() + ": " + e.getMessage());
        }
    }

    public static void shutdownNow() {
        if (!SHUTDOWN_CALLED.compareAndSet(false, true)) {
            return;
        }
        try {
            Object ctx = org.apache.logging.log4j.LogManager.getContext(false);
            if (ctx != null) {
                try {
                    Method stop = ctx.getClass().getMethod("stop");
                    stop.invoke(ctx);
                    return;
                } catch (NoClassDefFoundError | NoSuchMethodException ignored) {
                    // fall through to LogManager.shutdown()
                } catch (Throwable t) {
                    System.err.println("Exception invoking ctx.stop(): " + t.getClass().getName() + ": " + t.getMessage());
                }
            }
            org.apache.logging.log4j.LogManager.shutdown();
        } catch (NoClassDefFoundError e) {
            // log4j-core not present in this classloader — nothing to do
        } catch (Throwable t) {
            System.err.println("Suppressed exception in Log4j shutdown: " + t.getClass().getName() + ": " + t.getMessage());
        }
    }
}

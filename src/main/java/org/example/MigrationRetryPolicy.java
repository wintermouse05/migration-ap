package org.example;

import java.util.Locale;

public class MigrationRetryPolicy {
    private final boolean retryEnabled;
    private final int maxAttempts;
    private final long initialDelayMs;
    private final double backoffMultiplier;
    private final boolean resumeEnabled;
    private final String resumeStateFile;
    private final boolean resetResumeState;

    public MigrationRetryPolicy(
            boolean retryEnabled,
            int maxAttempts,
            long initialDelayMs,
            double backoffMultiplier,
            boolean resumeEnabled,
            String resumeStateFile,
            boolean resetResumeState
    ) {
        this.retryEnabled = retryEnabled;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.initialDelayMs = Math.max(0L, initialDelayMs);
        this.backoffMultiplier = backoffMultiplier > 0 ? backoffMultiplier : 1.0;
        this.resumeEnabled = resumeEnabled;
        this.resumeStateFile = (resumeStateFile == null || resumeStateFile.isBlank())
                ? ".migration-resume.properties"
                : resumeStateFile;
        this.resetResumeState = resetResumeState;
    }

    public static MigrationRetryPolicy fromEnvironment() {
        return new MigrationRetryPolicy(
                getEnvAsBoolean("MIGRATION_ENABLE_RETRY", true),
                getEnvAsInt("MIGRATION_MAX_ATTEMPTS", 3),
                getEnvAsLong("MIGRATION_RETRY_DELAY_MS", 2000L),
                getEnvAsDouble("MIGRATION_RETRY_BACKOFF_MULTIPLIER", 2.0),
                getEnvAsBoolean("MIGRATION_ENABLE_RESUME", true),
                getEnv("MIGRATION_RESUME_STATE_FILE", ".migration-resume.properties"),
                getEnvAsBoolean("MIGRATION_RESET_RESUME_STATE", false)
        );
    }

    public int resolveAttempts() {
        return retryEnabled ? maxAttempts : 1;
    }

    public long delayForAttempt(int attempt) {
        if (attempt <= 1 || initialDelayMs <= 0L) {
            return initialDelayMs;
        }

        double multiplier = Math.pow(backoffMultiplier, Math.max(0, attempt - 1));
        double value = initialDelayMs * multiplier;
        if (value > Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        return (long) value;
    }

    public boolean isRetryEnabled() {
        return retryEnabled;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public long getInitialDelayMs() {
        return initialDelayMs;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public boolean isResumeEnabled() {
        return resumeEnabled;
    }

    public String getResumeStateFile() {
        return resumeStateFile;
    }

    public boolean isResetResumeState() {
        return resetResumeState;
    }

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    private static int getEnvAsInt(String name, int defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException ex) {
            System.err.println("Bien moi truong " + name + " khong hop le, dung mac dinh " + defaultValue);
            return defaultValue;
        }
    }

    private static long getEnvAsLong(String name, long defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException ex) {
            System.err.println("Bien moi truong " + name + " khong hop le, dung mac dinh " + defaultValue);
            return defaultValue;
        }
    }

    private static double getEnvAsDouble(String name, double defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException ex) {
            System.err.println("Bien moi truong " + name + " khong hop le, dung mac dinh " + defaultValue);
            return defaultValue;
        }
    }

    private static boolean getEnvAsBoolean(String name, boolean defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        Boolean parsed = parseBooleanValue(value);
        if (parsed != null) {
            return parsed;
        }

        System.err.println("Bien moi truong " + name + " khong hop le, dung mac dinh " + defaultValue);
        return defaultValue;
    }

    private static Boolean parseBooleanValue(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        String normalized = value.trim().toLowerCase(Locale.ROOT);
        if ("true".equals(normalized)
                || "1".equals(normalized)
                || "yes".equals(normalized)
                || "y".equals(normalized)
                || "on".equals(normalized)) {
            return true;
        }

        if ("false".equals(normalized)
                || "0".equals(normalized)
                || "no".equals(normalized)
                || "n".equals(normalized)
                || "off".equals(normalized)) {
            return false;
        }

        return null;
    }
}

package org.example;

import java.util.Locale;
import java.util.TimeZone;

public class MigrationApp {

    public static void main(String[] args) {
        configureAppTimezone();

        Boolean metadataTestOverride = resolveMetadataTestOverrideFromArgs(args);
        String direction = resolveDirection(args);

        if ("POSTGRES_TO_ORACLE".equals(direction)) {
            System.out.println("Migration direction: POSTGRES_TO_ORACLE");
            new PostgresToOracleMigration(metadataTestOverride).run();
            return;
        }

        System.out.println("Migration direction: ORACLE_TO_POSTGRES");
        new OracleToPostgresMigration(metadataTestOverride).run();
    }

    private static void configureAppTimezone() {
        String timezone = getEnv("APP_TIMEZONE", "UTC");
        TimeZone.setDefault(TimeZone.getTimeZone(timezone));
        System.setProperty("user.timezone", TimeZone.getDefault().getID());
        System.out.println("Timezone hien tai cua app: " + TimeZone.getDefault().getID());
    }

    private static String resolveDirection(String[] args) {
        String fromArgs = findArgValue(args, "--direction=");
        String rawDirection = (fromArgs == null || fromArgs.isBlank())
                ? getEnv("MIGRATION_DIRECTION", "ORACLE_TO_POSTGRES")
                : fromArgs;

        String normalized = normalizeDirection(rawDirection);
        if (normalized != null) {
            return normalized;
        }

        System.err.println(
                "MIGRATION_DIRECTION khong hop le: " + rawDirection
                        + ". Dung ORACLE_TO_POSTGRES hoac POSTGRES_TO_ORACLE."
        );
        return "ORACLE_TO_POSTGRES";
    }

    private static String normalizeDirection(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        String normalized = value.trim().toUpperCase(Locale.ROOT)
                .replace('-', '_')
                .replace(' ', '_');

        if ("ORACLE_TO_POSTGRES".equals(normalized) || "O2P".equals(normalized)) {
            return "ORACLE_TO_POSTGRES";
        }

        if ("POSTGRES_TO_ORACLE".equals(normalized) || "P2O".equals(normalized)) {
            return "POSTGRES_TO_ORACLE";
        }

        return null;
    }

    private static Boolean resolveMetadataTestOverrideFromArgs(String[] args) {
        if (args == null || args.length == 0) {
            return null;
        }

        Boolean override = null;
        for (String arg : args) {
            if ("--enable-metadata-test".equalsIgnoreCase(arg)) {
                override = true;
                continue;
            }

            if ("--disable-metadata-test".equalsIgnoreCase(arg)) {
                override = false;
                continue;
            }

            if (arg.toLowerCase(Locale.ROOT).startsWith("--metadata-test=")) {
                String value = arg.substring("--metadata-test=".length());
                Boolean parsed = parseBooleanValue(value);
                if (parsed != null) {
                    override = parsed;
                } else {
                    System.err.println(
                            "Tham so --metadata-test=" + value + " khong hop le. "
                                    + "Dung true/false, on/off, 1/0, yes/no."
                    );
                }
            }
        }

        return override;
    }

    private static String findArgValue(String[] args, String prefix) {
        if (args == null || args.length == 0) {
            return null;
        }

        for (String arg : args) {
            if (arg != null && arg.startsWith(prefix)) {
                return arg.substring(prefix.length());
            }
        }

        return null;
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

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }
}

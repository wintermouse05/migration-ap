package org.example;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Properties;

public class MigrationCheckpointStore {
    private static final String OFFSET_SUFFIX = ".offset";
    private static final String DONE_SUFFIX = ".done";

    private final Path statePath;
    private final Properties properties;

    public MigrationCheckpointStore(String stateFile) {
        this.statePath = Paths.get(stateFile).toAbsolutePath().normalize();
        this.properties = new Properties();
        load();
    }

    public synchronized boolean isTableCompleted(String tableName) {
        return Boolean.parseBoolean(properties.getProperty(doneKey(tableName), "false"));
    }

    public synchronized int getTableOffset(String tableName) {
        String value = properties.getProperty(offsetKey(tableName), "0");
        try {
            return Math.max(0, Integer.parseInt(value));
        } catch (NumberFormatException ex) {
            return 0;
        }
    }

    public synchronized void updateTableOffset(String tableName, int offset) {
        int safeOffset = Math.max(0, offset);
        properties.setProperty(offsetKey(tableName), Integer.toString(safeOffset));
        properties.setProperty(doneKey(tableName), "false");
        save();
    }

    public synchronized void markTableCompleted(String tableName, int finalOffset) {
        int safeOffset = Math.max(0, finalOffset);
        properties.setProperty(offsetKey(tableName), Integer.toString(safeOffset));
        properties.setProperty(doneKey(tableName), "true");
        save();
    }

    public synchronized void clearTableState(String tableName) {
        properties.remove(offsetKey(tableName));
        properties.remove(doneKey(tableName));
        save();
    }

    public synchronized void clear() {
        properties.clear();
        save();
    }

    public String getStateFilePath() {
        return statePath.toString();
    }

    private String offsetKey(String tableName) {
        return normalizeTableName(tableName) + OFFSET_SUFFIX;
    }

    private String doneKey(String tableName) {
        return normalizeTableName(tableName) + DONE_SUFFIX;
    }

    private static String normalizeTableName(String tableName) {
        if (tableName == null) {
            return "UNKNOWN";
        }
        return tableName.trim().toUpperCase(Locale.ROOT);
    }

    private void load() {
        if (!Files.exists(statePath)) {
            return;
        }

        try (InputStream inputStream = Files.newInputStream(statePath)) {
            properties.load(inputStream);
        } catch (IOException e) {
            throw new IllegalStateException("Khong the doc file resume state: " + statePath, e);
        }
    }

    private void save() {
        try {
            Path parent = statePath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            try (OutputStream outputStream = Files.newOutputStream(statePath)) {
                properties.store(outputStream, "Migration resume state");
            }
        } catch (IOException e) {
            throw new IllegalStateException("Khong the ghi file resume state: " + statePath, e);
        }
    }
}
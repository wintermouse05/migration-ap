package org.example;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class OracleToPostgresMigration extends DirectionalMigration {

    public OracleToPostgresMigration(Boolean metadataTestOverride) {
        super(metadataTestOverride);
    }

    @Override
    protected DatabaseConfig buildSourceConfig() {
        return new DatabaseConfig(
                DatabaseType.ORACLE,
                getEnv("ORACLE_HOST", "localhost"),
                getEnvAsInt("ORACLE_PORT", 1521),
                getEnv("ORACLE_SERVICE", "ORCL"),
                getEnv("ORACLE_USER", "scott"),
                getEnv("ORACLE_USER_PASSWORD", "tiger")
        );
    }

    @Override
    protected DatabaseConfig buildTargetConfig() {
        return new DatabaseConfig(
                DatabaseType.POSTGRESQL,
                getEnv("POSTGRES_HOST", "localhost"),
                getEnvAsInt("POSTGRES_PORT", 5432),
                getEnv("POSTGRES_DB", "migration_db"),
                getEnv("POSTGRES_USER", "postgres"),
                getEnv("POSTGRES_PASSWORD", "admin123")
        );
    }

    @Override
    protected String defaultSourceSchema() {
        return getEnv("SOURCE_SCHEMA", getEnv("ORACLE_USER", "scott").toUpperCase(Locale.ROOT));
    }

    @Override
    protected String defaultTargetSchema() {
        return getEnv("TARGET_SCHEMA", "public");
    }
}

abstract class DirectionalMigration {
    private static final int POOL_INIT_MAX_RETRIES = 20;
    private static final long POOL_INIT_RETRY_DELAY_MS = 3000;

    // Migration flags are controlled in code.
    protected static final boolean MIGRATION_DRY_RUN = false;
    protected static final boolean MIGRATION_SKIP_EXISTING_TABLES = true;
    protected static final boolean MIGRATION_SKIP_EXISTING_FKS = true;
    protected static final boolean MIGRATION_RECREATE_EXISTING_TABLES = false;

    private final Boolean metadataTestOverride;

    protected DirectionalMigration(Boolean metadataTestOverride) {
        this.metadataTestOverride = metadataTestOverride;
    }

    public final void run() {
        ConnectionManager manager = ConnectionManager.getInstance();
        DatabaseConfig sourceConfig = buildSourceConfig();
        DatabaseConfig targetConfig = buildTargetConfig();

        try {
            createPoolWithRetry(manager, "SOURCE_DB", sourceConfig);
            createPoolWithRetry(manager, "TARGET_DB", targetConfig);

            if (!manager.testConnection("SOURCE_DB") || !manager.testConnection("TARGET_DB")) {
                System.err.println("Khong the thiet lap ket noi, huy bo migration.");
                return;
            }

            System.out.println("Bat dau tien trinh migration...");

            List<TableDefinition> allTables;
            try (Connection sourceConn = manager.getConnection("SOURCE_DB");
                 Connection targetConn = manager.getConnection("TARGET_DB")) {
                System.out.println("Source URL: " + sourceConn.getMetaData().getURL());
                System.out.println("Target URL: " + targetConn.getMetaData().getURL());

                MetadataExtractor metadataExtractor = new MetadataExtractor();
                String sourceSchema = defaultSourceSchema();
                String targetSchema = defaultTargetSchema();
                int metadataMaxTables = getEnvAsInt("METADATA_MAX_TABLES", 5);
                boolean metadataTestEnabled = getEnvAsBoolean("ENABLE_METADATA_TEST", false);
                if (metadataTestOverride != null) {
                    metadataTestEnabled = Boolean.TRUE.equals(metadataTestOverride);
                }
                String metadataOutputFormat = getEnv("METADATA_OUTPUT_FORMAT", "json");

                if (metadataTestOverride != null) {
                    System.out.println(
                            "Metadata test override tu startup args: "
                                    + (metadataTestEnabled ? "BAT" : "TAT")
                    );
                }

                if (metadataTestEnabled) {
                    runMetadataExtraction(
                            metadataExtractor,
                            sourceConn,
                            sourceSchema,
                            "SOURCE_DB",
                            metadataMaxTables,
                            metadataOutputFormat
                    );
                    runMetadataExtraction(
                            metadataExtractor,
                            targetConn,
                            targetSchema,
                            "TARGET_DB",
                            metadataMaxTables,
                            metadataOutputFormat
                    );
                } else {
                    System.out.println(
                            "Metadata test dang tat. Bat ENABLE_METADATA_TEST=true hoac --enable-metadata-test."
                    );
                }

                int migrationMaxTables = getEnvAsInt("MIGRATION_MAX_TABLES", 0);
                Set<String> includeTables = parseEnvTableFilter("MIGRATION_INCLUDE_TABLES");
                Set<String> excludeTables = parseEnvTableFilter("MIGRATION_EXCLUDE_TABLES");
                allTables = loadTableDefinitionsForMigration(
                        metadataExtractor,
                        sourceConn,
                        sourceSchema,
                    migrationMaxTables,
                    includeTables,
                    excludeTables
                );
            }

            if (allTables.isEmpty()) {
                System.out.println("Khong co bang nao de migrate. Ket thuc tien trinh.");
                return;
            }

            SqlDialect sourceDialect = DialectFactory.getDialect(sourceConfig.getType());
            SqlDialect targetDialect = DialectFactory.getDialect(targetConfig.getType());

            if (MIGRATION_DRY_RUN) {
                System.out.println("MIGRATION_DRY_RUN=true => chi in SQL, khong ghi du lieu.");
                executeMigration(allTables, targetDialect);
            } else {
                executeMigration(allTables, sourceDialect, targetDialect);
            }

        } catch (SQLException | RuntimeException e) {
            System.err.println("Migration that bai: " + e.getMessage());
        } finally {
            manager.closeAllPools();
        }
    }

    protected abstract DatabaseConfig buildSourceConfig();

    protected abstract DatabaseConfig buildTargetConfig();

    protected abstract String defaultSourceSchema();

    protected abstract String defaultTargetSchema();

    private static void createPoolWithRetry(ConnectionManager manager, String poolId, DatabaseConfig config) {
        for (int attempt = 1; attempt <= POOL_INIT_MAX_RETRIES; attempt++) {
            try {
                manager.createPool(poolId, config);
                return;
            } catch (RuntimeException ex) {
                if (attempt == POOL_INIT_MAX_RETRIES) {
                    throw ex;
                }

                System.err.println(
                        "Chua tao duoc pool [" + poolId + "] lan " + attempt + "/" + POOL_INIT_MAX_RETRIES
                                + ", thu lai sau " + (POOL_INIT_RETRY_DELAY_MS / 1000) + " giay. Ly do: "
                                + ex.getMessage()
                );
                sleep(POOL_INIT_RETRY_DELAY_MS);
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Bi gian doan khi cho ket noi DB.", e);
        }
    }

    private static void runMetadataExtraction(
            MetadataExtractor metadataExtractor,
            Connection connection,
            String schema,
            String connectionLabel,
            int maxTables,
            String outputFormat
    ) throws SQLException {
        int safeMaxTables = Math.max(1, maxTables);
        boolean jsonOutput = "json".equalsIgnoreCase(outputFormat);
        List<String> tableNames = metadataExtractor.getTableNames(connection, schema);

        int displayCount = Math.min(safeMaxTables, tableNames.size());
        List<TableDefinition> tableDefinitions = new ArrayList<>();
        for (int i = 0; i < displayCount; i++) {
            String tableName = tableNames.get(i);
            TableDefinition tableDefinition = metadataExtractor.extractTableDefinition(connection, schema, tableName);
            tableDefinitions.add(tableDefinition);
        }

        if (jsonOutput) {
            System.out.println(buildMetadataJson(connectionLabel, schema, tableNames.size(), displayCount, tableDefinitions));
            return;
        }

        System.out.println("\n=== Extract metadata tu " + connectionLabel + " | schema=" + schema + " ===");
        if (tableNames.isEmpty()) {
            System.out.println("Khong tim thay bang nao trong schema " + schema + ".");
            return;
        }

        System.out.println("Tim thay " + tableNames.size() + " bang, hien thi " + displayCount + " bang dau tien.");

        for (TableDefinition tableDefinition : tableDefinitions) {
            printTableDefinition(tableDefinition);
        }

        if (tableNames.size() > displayCount) {
            System.out.println("... con " + (tableNames.size() - displayCount) + " bang chua hien thi.");
        }
    }

    private static void printTableDefinition(TableDefinition tableDefinition) {
        System.out.println("\nTable: " + tableDefinition.getTableName());

        List<ColumnDefinition> columns = tableDefinition.getColumns();
        if (columns.isEmpty()) {
            System.out.println("Columns: (none)");
        } else {
            System.out.println("Columns:");
            for (ColumnDefinition column : columns) {
                System.out.println(
                        " - " + column.getName() + " | " + column.getTypeName() + "(" + column.getSize() + ")"
                                + " | nullable=" + column.isNullable()
                                + " | autoIncrement=" + column.isAutoIncrement()
                );
            }
        }

        List<String> primaryKeys = tableDefinition.getPrimaryKeys();
        if (primaryKeys.isEmpty()) {
            System.out.println("Primary Keys: (none)");
        } else {
            System.out.println("Primary Keys: " + String.join(", ", primaryKeys));
        }

        List<ForeignKeyDefinition> foreignKeys = tableDefinition.getForeignKeys();
        if (foreignKeys.isEmpty()) {
            System.out.println("Foreign Keys: (none)");
        } else {
            System.out.println("Foreign Keys:");
            for (ForeignKeyDefinition fk : foreignKeys) {
                String fkName = (fk.getFkName() == null || fk.getFkName().isBlank()) ? "(unnamed)" : fk.getFkName();
                System.out.println(
                        " - " + fkName + ": " + fk.getFkColumnName()
                                + " -> " + fk.getTargetTableName() + "." + fk.getTargetColumnName()
                );
            }
        }
    }

    private static String buildMetadataJson(
            String connectionLabel,
            String schema,
            int totalTables,
            int displayedTables,
            List<TableDefinition> tableDefinitions
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"connection\": ").append(toJsonString(connectionLabel)).append(",\n");
        sb.append("  \"schema\": ").append(toJsonString(schema)).append(",\n");
        sb.append("  \"totalTables\": ").append(totalTables).append(",\n");
        sb.append("  \"displayedTables\": ").append(displayedTables).append(",\n");
        sb.append("  \"remainingTables\": ").append(Math.max(0, totalTables - displayedTables)).append(",\n");
        sb.append("  \"tables\": [");
        if (!tableDefinitions.isEmpty()) {
            sb.append("\n");
            for (int i = 0; i < tableDefinitions.size(); i++) {
                appendTableDefinitionJson(sb, tableDefinitions.get(i), "    ");
                if (i < tableDefinitions.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            sb.append("  ");
        }
        sb.append("]\n");
        sb.append("}");
        return sb.toString();
    }

    private static void appendTableDefinitionJson(StringBuilder sb, TableDefinition tableDefinition, String indent) {
        sb.append(indent).append("{\n");
        sb.append(indent).append("  \"tableName\": ").append(toJsonString(tableDefinition.getTableName())).append(",\n");
        sb.append(indent).append("  \"columns\": [");

        List<ColumnDefinition> columns = tableDefinition.getColumns();
        if (!columns.isEmpty()) {
            sb.append("\n");
            for (int i = 0; i < columns.size(); i++) {
                ColumnDefinition column = columns.get(i);
                sb.append(indent).append("    {\n");
                sb.append(indent).append("      \"name\": ").append(toJsonString(column.getName())).append(",\n");
                sb.append(indent).append("      \"jdbcType\": ").append(column.getJdbcType()).append(",\n");
                sb.append(indent).append("      \"typeName\": ").append(toJsonString(column.getTypeName())).append(",\n");
                sb.append(indent).append("      \"size\": ").append(column.getSize()).append(",\n");
                sb.append(indent).append("      \"nullable\": ").append(column.isNullable()).append(",\n");
                sb.append(indent).append("      \"autoIncrement\": ").append(column.isAutoIncrement()).append("\n");
                sb.append(indent).append("    }");
                if (i < columns.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            sb.append(indent).append("  ");
        }
        sb.append("],\n");

        List<String> primaryKeys = tableDefinition.getPrimaryKeys();
        sb.append(indent).append("  \"primaryKeys\": [");
        for (int i = 0; i < primaryKeys.size(); i++) {
            sb.append(toJsonString(primaryKeys.get(i)));
            if (i < primaryKeys.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append("],\n");

        List<ForeignKeyDefinition> foreignKeys = tableDefinition.getForeignKeys();
        sb.append(indent).append("  \"foreignKeys\": [");
        if (!foreignKeys.isEmpty()) {
            sb.append("\n");
            for (int i = 0; i < foreignKeys.size(); i++) {
                ForeignKeyDefinition fk = foreignKeys.get(i);
                sb.append(indent).append("    {\n");
                sb.append(indent).append("      \"name\": ").append(toJsonString(fk.getFkName())).append(",\n");
                sb.append(indent).append("      \"column\": ").append(toJsonString(fk.getFkColumnName())).append(",\n");
                sb.append(indent).append("      \"targetTable\": ").append(toJsonString(fk.getTargetTableName())).append(",\n");
                sb.append(indent).append("      \"targetColumn\": ").append(toJsonString(fk.getTargetColumnName())).append("\n");
                sb.append(indent).append("    }");
                if (i < foreignKeys.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            sb.append(indent).append("  ");
        }
        sb.append("]\n");
        sb.append(indent).append("}");
    }

    private static String toJsonString(String value) {
        if (value == null) {
            return "null";
        }
        return "\"" + escapeJson(value) + "\"";
    }

    private static String escapeJson(String value) {
        StringBuilder escaped = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\\' -> escaped.append("\\\\");
                case '"' -> escaped.append("\\\"");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> escaped.append(ch);
            }
        }
        return escaped.toString();
    }

    private static List<TableDefinition> loadTableDefinitionsForMigration(
            MetadataExtractor metadataExtractor,
            Connection sourceConn,
            String sourceSchema,
            int migrationMaxTables,
            Set<String> includeTables,
            Set<String> excludeTables
    ) throws SQLException {
        List<String> discoveredTableNames = metadataExtractor.getTableNames(sourceConn, sourceSchema);
        if (discoveredTableNames.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> tableNames = applyTableFilters(discoveredTableNames, includeTables, excludeTables);
        if (tableNames.isEmpty()) {
            System.out.println("Khong co bang nao phu hop MIGRATION_INCLUDE_TABLES/MIGRATION_EXCLUDE_TABLES.");
            return new ArrayList<>();
        }

        int safeLimit = migrationMaxTables > 0 ? Math.min(migrationMaxTables, tableNames.size()) : tableNames.size();
        if (safeLimit < tableNames.size()) {
            System.out.println("Gioi han migrate " + safeLimit + "/" + tableNames.size()
                    + " bang do MIGRATION_MAX_TABLES=" + migrationMaxTables);
        }

        List<TableDefinition> allTables = new ArrayList<>();
        for (int i = 0; i < safeLimit; i++) {
            String tableName = tableNames.get(i);
            allTables.add(metadataExtractor.extractTableDefinition(sourceConn, sourceSchema, tableName));
        }

        return allTables;
    }

    private static List<String> applyTableFilters(
            List<String> tableNames,
            Set<String> includeTables,
            Set<String> excludeTables
    ) {
        List<String> filtered = new ArrayList<>();
        for (String tableName : tableNames) {
            String normalized = tableName == null ? "" : tableName.trim().toUpperCase(Locale.ROOT);
            if (normalized.isEmpty()) {
                continue;
            }

            if (!includeTables.isEmpty() && !includeTables.contains(normalized)) {
                continue;
            }

            if (excludeTables.contains(normalized)) {
                continue;
            }

            filtered.add(tableName);
        }

        return filtered;
    }

    private static Set<String> parseEnvTableFilter(String envName) {
        String raw = getEnv(envName, "");
        Set<String> values = new LinkedHashSet<>();
        if (raw.isBlank()) {
            return values;
        }

        String[] parts = raw.split(",");
        for (String part : parts) {
            String value = part == null ? "" : part.trim();
            if (value.isEmpty()) {
                continue;
            }
            values.add(value.toUpperCase(Locale.ROOT));
        }

        return values;
    }

    private static void executeMigration(List<TableDefinition> allTables, SqlDialect targetDialect) {
        System.out.println("\n--- PHASE 1: CREATING TABLES ---");
        for (TableDefinition table : allTables) {
            String createSql = normalizeSqlForJdbc(targetDialect.buildCreateTableSql(table));
            System.out.println(createSql);
        }

        System.out.println("\n--- PHASE 2: MIGRATING DATA (Skipped in this dry-run) ---");

        System.out.println("\n--- PHASE 3: ADDING FOREIGN KEYS ---");
        for (TableDefinition table : allTables) {
            List<String> fkSqls = targetDialect.buildAddForeignKeySql(table);
            for (String fkSql : fkSqls) {
                System.out.println(normalizeSqlForJdbc(fkSql));
            }
        }
    }

    private static void executeMigration(
            List<TableDefinition> allTables,
            SqlDialect sourceDialect,
            SqlDialect targetDialect
    ) {
        ConnectionManager manager = ConnectionManager.getInstance();
        MigrationRetryPolicy retryPolicy = MigrationRetryPolicy.fromEnvironment();
        MigrationCheckpointStore checkpointStore = retryPolicy.isResumeEnabled()
                ? new MigrationCheckpointStore(retryPolicy.getResumeStateFile())
                : null;

        try (Connection targetConn = manager.getConnection("TARGET_DB")) {

            int batchSize = Math.max(1, getEnvAsInt("MIGRATION_BATCH_SIZE", 1000));
                boolean skipExistingTables = getEnvAsBoolean("MIGRATION_SKIP_EXISTING_TABLES", MIGRATION_SKIP_EXISTING_TABLES);
                boolean skipExistingForeignKeys = getEnvAsBoolean("MIGRATION_SKIP_EXISTING_FKS", MIGRATION_SKIP_EXISTING_FKS);
                boolean recreateExistingTables = getEnvAsBoolean(
                    "MIGRATION_RECREATE_EXISTING_TABLES",
                    MIGRATION_RECREATE_EXISTING_TABLES
                );

            printRetryResumeConfig(retryPolicy, checkpointStore);

            if (recreateExistingTables) {
                dropTargetTablesPhase(targetConn, allTables, targetDialect);
                if (checkpointStore != null) {
                    checkpointStore.clear();
                    System.out.println("Resume state da duoc reset do MIGRATION_RECREATE_EXISTING_TABLES=true.");
                }
            } else if (retryPolicy.isResetResumeState() && checkpointStore != null) {
                checkpointStore.clear();
                System.out.println("Resume state da duoc reset do MIGRATION_RESET_RESUME_STATE=true.");
            }

            runCreateTablesPhase(targetConn, allTables, targetDialect, skipExistingTables);

            System.out.println("\n--- PHASE 2: MIGRATING DATA ---");
            SqlGenerator sqlGenerator = new SqlGenerator(sourceDialect, targetDialect);
            DataTransferService transferService = new DataTransferService(sqlGenerator);
            for (TableDefinition table : allTables) {
                if (checkpointStore != null && checkpointStore.isTableCompleted(table.getTableName())) {
                    System.out.println("Bo qua bang da migrate truoc do (resume): " + table.getTableName());
                    continue;
                }

                boolean success = transferTableWithRetry(
                        manager,
                        transferService,
                        table,
                        batchSize,
                        retryPolicy,
                        checkpointStore
                );

                if (!success) {
                    System.err.println("Bo qua bang " + table.getTableName() + " sau khi da retry het so lan cho phep.");
                }
            }

            runAddForeignKeysPhase(targetConn, allTables, targetDialect, skipExistingForeignKeys);

        } catch (Exception e) {
            System.err.println("Migration that bai: " + e.getMessage());
        }
    }

    private static boolean transferTableWithRetry(
            ConnectionManager manager,
            DataTransferService transferService,
            TableDefinition table,
            int batchSize,
            MigrationRetryPolicy retryPolicy,
            MigrationCheckpointStore checkpointStore
    ) {
        int attempts = retryPolicy.resolveAttempts();
        boolean hasPrimaryKey = !table.getPrimaryKeys().isEmpty();
        boolean enableResumeByPk = retryPolicy.isResumeEnabled() && hasPrimaryKey;
        int startOffset = 0;

        if (checkpointStore != null) {
            startOffset = checkpointStore.getTableOffset(table.getTableName());
            if (startOffset > 0) {
                System.out.println("Resume bang " + table.getTableName() + " tu offset " + startOffset + ".");
            }
        }

        for (int attempt = 1; attempt <= attempts; attempt++) {
            try (Connection sourceConn = manager.getConnection("SOURCE_DB");
                 Connection targetConn = manager.getConnection("TARGET_DB")) {

                if (attempt > 1) {
                    System.out.println("Retry bang " + table.getTableName() + " lan " + attempt + "/" + attempts + "...");
                }

                DataTransferService.TransferResult result = transferService.transferTableData(
                        sourceConn,
                        targetConn,
                        table,
                        batchSize,
                        null,
                        enableResumeByPk,
                        startOffset,
                        (tableName, justTransferred, totalTransferred, totalSkipped) -> {
                            if (checkpointStore != null) {
                                checkpointStore.updateTableOffset(tableName, totalTransferred);
                            }
                        }
                );

                if (checkpointStore != null) {
                    checkpointStore.markTableCompleted(table.getTableName(), result.getTransferredRows());
                }

                return true;
            } catch (SQLException e) {
                if (!isRetryableException(e) || attempt == attempts) {
                    System.err.println("Loi migrate bang " + table.getTableName() + ": " + e.getMessage());
                    return false;
                }

                if (checkpointStore != null) {
                    startOffset = checkpointStore.getTableOffset(table.getTableName());
                }

                long delayMs = retryPolicy.delayForAttempt(attempt);
                System.err.println(
                        "Loi tam thoi khi migrate bang " + table.getTableName()
                                + " (lan " + attempt + "/" + attempts + "), thu lai sau " + delayMs + " ms. Ly do: "
                                + e.getMessage()
                );
                sleep(delayMs);
            }
        }

        return false;
    }

    private static void printRetryResumeConfig(MigrationRetryPolicy retryPolicy, MigrationCheckpointStore checkpointStore) {
        System.out.println("Retry policy: enabled=" + retryPolicy.isRetryEnabled()
                + ", maxAttempts=" + retryPolicy.getMaxAttempts()
                + ", delayMs=" + retryPolicy.getInitialDelayMs()
                + ", backoff=" + retryPolicy.getBackoffMultiplier());

        if (!retryPolicy.isResumeEnabled()) {
            System.out.println("Resume policy: disabled");
            return;
        }

        if (checkpointStore == null) {
            System.out.println("Resume policy: enabled (khong su dung checkpoint file)");
            return;
        }

        System.out.println("Resume policy: enabled, stateFile=" + checkpointStore.getStateFilePath());
    }

    private static boolean isRetryableException(SQLException e) {
        if (e == null) {
            return false;
        }

        String sqlState = e.getSQLState();
        if (sqlState != null) {
            if (sqlState.startsWith("08")
                    || "40001".equals(sqlState)
                    || "40P01".equals(sqlState)
                    || "HYT00".equals(sqlState)
                    || "HYT01".equals(sqlState)) {
                return true;
            }
        }

        String message = e.getMessage();
        if (message == null) {
            return false;
        }

        String lowered = message.toLowerCase(Locale.ROOT);
        return lowered.contains("connection")
                || lowered.contains("socket")
                || lowered.contains("timed out")
                || lowered.contains("timeout")
                || lowered.contains("broken pipe")
                || lowered.contains("i/o error")
                || lowered.contains("communications link failure");
    }

    private static void runCreateTablesPhase(
            Connection targetConn,
            List<TableDefinition> allTables,
            SqlDialect targetDialect,
            boolean skipExistingTables
    ) throws SQLException {
        System.out.println("\n--- PHASE 1: CREATING TABLES ---");

        try (Statement statement = targetConn.createStatement()) {
            for (TableDefinition table : allTables) {
                String createSql = normalizeSqlForJdbc(targetDialect.buildCreateTableSql(table));
                try {
                    statement.execute(createSql);
                    System.out.println("Created table: " + table.getTableName());
                } catch (SQLException e) {
                    if (skipExistingTables && isTableAlreadyExistsError(e)) {
                        System.out.println("Skipped existing table: " + table.getTableName());
                        continue;
                    }
                    throw e;
                }
            }
        }
    }

    private static void dropTargetTablesPhase(
            Connection targetConn,
            List<TableDefinition> allTables,
            SqlDialect targetDialect
    ) throws SQLException {
        System.out.println("\n--- PRE-PHASE: DROPPING EXISTING TABLES ---");

        try (Statement statement = targetConn.createStatement()) {
            if (targetDialect instanceof PostgresDialect) {
                for (int i = allTables.size() - 1; i >= 0; i--) {
                    TableDefinition table = allTables.get(i);
                    String dropSql = "DROP TABLE IF EXISTS "
                            + targetDialect.quoteIdentifier(table.getTableName())
                            + " CASCADE";
                    statement.execute(dropSql);
                    System.out.println("Dropped table if exists: " + table.getTableName());
                }
                return;
            }

            if (targetDialect instanceof OracleDialect) {
                for (int i = allTables.size() - 1; i >= 0; i--) {
                    TableDefinition table = allTables.get(i);
                    String dropSql = "DROP TABLE "
                            + targetDialect.quoteIdentifier(table.getTableName().toUpperCase(Locale.ROOT))
                            + " CASCADE CONSTRAINTS PURGE";
                    try {
                        statement.execute(dropSql);
                        System.out.println("Dropped table if exists: " + table.getTableName());
                    } catch (SQLException e) {
                        if (isTableNotExistsError(e)) {
                            System.out.println("Skipped missing table: " + table.getTableName());
                            continue;
                        }
                        throw e;
                    }
                }
                return;
            }

            System.out.println("MIGRATION_RECREATE_EXISTING_TABLES chua ho tro target hien tai.");
        }
    }

    private static void runAddForeignKeysPhase(
            Connection targetConn,
            List<TableDefinition> allTables,
            SqlDialect targetDialect,
            boolean skipExistingForeignKeys
    ) throws SQLException {
        System.out.println("\n--- PHASE 3: ADDING FOREIGN KEYS ---");

        try (Statement statement = targetConn.createStatement()) {
            for (TableDefinition table : allTables) {
                List<String> fkSqls = targetDialect.buildAddForeignKeySql(table);
                for (String fkSql : fkSqls) {
                    String normalizedFkSql = normalizeSqlForJdbc(fkSql);
                    try {
                        statement.execute(normalizedFkSql);
                        System.out.println("Added FK for table: " + table.getTableName());
                    } catch (SQLException e) {
                        if (skipExistingForeignKeys && isConstraintAlreadyExistsError(e)) {
                            System.out.println("Skipped existing FK on table: " + table.getTableName());
                            continue;
                        }

                        if (skipExistingForeignKeys && isIncompatibleForeignKeyError(e)) {
                            System.err.println(
                                    "Skipped incompatible FK on table " + table.getTableName()
                                            + ". Goi y: dat MIGRATION_RECREATE_EXISTING_TABLES=true trong code de dong bo lai schema target."
                            );
                            continue;
                        }
                        throw e;
                    }
                }
            }
        }
    }

    private static String normalizeSqlForJdbc(String sql) {
        String normalized = sql.trim();
        if (normalized.endsWith(";")) {
            return normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static boolean isTableAlreadyExistsError(SQLException e) {
        String sqlState = e.getSQLState();
        String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        return "42P07".equals(sqlState)
                || message.contains("already exists")
                || message.contains("ora-00955");
    }

    private static boolean isTableNotExistsError(SQLException e) {
        String sqlState = e.getSQLState();
        String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        return "42P01".equals(sqlState)
                || message.contains("does not exist")
                || message.contains("ora-00942");
    }

    private static boolean isConstraintAlreadyExistsError(SQLException e) {
        String sqlState = e.getSQLState();
        String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        return "42710".equals(sqlState)
                || message.contains("constraint") && message.contains("already exists")
                || message.contains("ora-02275");
    }

    private static boolean isIncompatibleForeignKeyError(SQLException e) {
        String sqlState = e.getSQLState();
        String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        return "42804".equals(sqlState)
                || message.contains("ora-02267")
                || (message.contains("foreign key") && message.contains("incompatible"));
    }

    protected static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    protected static boolean getEnvAsBoolean(String name, boolean defaultValue) {
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

    protected static Boolean parseBooleanValue(String value) {
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

    protected static int getEnvAsInt(String name, int defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            System.err.println("Bien moi truong " + name + " khong hop le, dung mac dinh " + defaultValue);
            return defaultValue;
        }
    }
}

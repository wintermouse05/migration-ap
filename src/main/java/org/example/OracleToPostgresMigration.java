package org.example;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

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
    protected static final boolean MIGRATION_RECREATE_EXISTING_TABLES = true;

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
                boolean metadataTestEnabled = metadataTestOverride != null
                        ? metadataTestOverride
                        : getEnvAsBoolean("ENABLE_METADATA_TEST", false);
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
                allTables = loadTableDefinitionsForMigration(
                        metadataExtractor,
                        sourceConn,
                        sourceSchema,
                        migrationMaxTables
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
            int migrationMaxTables
    ) throws SQLException {
        List<String> tableNames = metadataExtractor.getTableNames(sourceConn, sourceSchema);
        if (tableNames.isEmpty()) {
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

        try (Connection sourceConn = manager.getConnection("SOURCE_DB");
             Connection targetConn = manager.getConnection("TARGET_DB")) {

            int batchSize = Math.max(1, getEnvAsInt("MIGRATION_BATCH_SIZE", 1000));
            boolean skipExistingTables = MIGRATION_SKIP_EXISTING_TABLES;
            boolean skipExistingForeignKeys = MIGRATION_SKIP_EXISTING_FKS;
            boolean recreateExistingTables = MIGRATION_RECREATE_EXISTING_TABLES;

            if (recreateExistingTables) {
                dropTargetTablesPhase(targetConn, allTables, targetDialect);
            }

            runCreateTablesPhase(targetConn, allTables, targetDialect, skipExistingTables);

            System.out.println("\n--- PHASE 2: MIGRATING DATA ---");
            SqlGenerator sqlGenerator = new SqlGenerator(sourceDialect, targetDialect);
            DataTransferService transferService = new DataTransferService(sqlGenerator);
            for (TableDefinition table : allTables) {
                try {
                    transferService.transferTableData(sourceConn, targetConn, table, batchSize);
                } catch (SQLException e) {
                    System.err.println("Bo qua bang " + table.getTableName() + " do loi du lieu. Ly do: " + e.getMessage());
                }
            }

            runAddForeignKeysPhase(targetConn, allTables, targetDialect, skipExistingForeignKeys);

        } catch (Exception e) {
            System.err.println("Migration that bai: " + e.getMessage());
        }
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

package org.example;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

public class MigrationApp {
    private static final int POOL_INIT_MAX_RETRIES = 20;
    private static final long POOL_INIT_RETRY_DELAY_MS = 3000;

    public static void main(String[] args) {
        configureAppTimezone();

        ConnectionManager manager = ConnectionManager.getInstance();

        // 1. Cấu hình Source DB (Oracle)
        DatabaseConfig oracleSourceConfig = new DatabaseConfig(
                DatabaseType.ORACLE,
                getEnv("ORACLE_HOST", "localhost"),
                getEnvAsInt("ORACLE_PORT", 1521),
                getEnv("ORACLE_SERVICE", "ORCL"),
                getEnv("ORACLE_USER", "scott"),
                getEnv("ORACLE_USER_PASSWORD", "tiger")
        );

        // 2. Cấu hình Target DB (PostgreSQL)
        DatabaseConfig pgTargetConfig = new DatabaseConfig(
                DatabaseType.POSTGRESQL,
                getEnv("POSTGRES_HOST", "localhost"),
                getEnvAsInt("POSTGRES_PORT", 5432),
                getEnv("POSTGRES_DB", "migration_db"),
                getEnv("POSTGRES_USER", "postgres"),
                getEnv("POSTGRES_PASSWORD", "admin123")
        );

        try {
            // Khởi tạo Pools
            createPoolWithRetry(manager, "SOURCE_DB", oracleSourceConfig);
            createPoolWithRetry(manager, "TARGET_DB", pgTargetConfig);

            // Kiểm tra kết nối trước khi chạy migration
            if (manager.testConnection("SOURCE_DB") && manager.testConnection("TARGET_DB")) {
                System.out.println("Bắt đầu tiến trình Migration...");

                // Lấy connection để làm việc
                try (Connection sourceConn = manager.getConnection("SOURCE_DB");
                     Connection targetConn = manager.getConnection("TARGET_DB")) {
                    System.out.println("Source URL: " + sourceConn.getMetaData().getURL());
                    System.out.println("Target URL: " + targetConn.getMetaData().getURL());

                    MetadataExtractor metadataExtractor = new MetadataExtractor();
                    String sourceSchema = getEnv(
                        "SOURCE_SCHEMA",
                        getEnv("ORACLE_USER", "scott").toUpperCase(Locale.ROOT)
                    );
                    String targetSchema = getEnv("TARGET_SCHEMA", "public");
                    int metadataMaxTables = getEnvAsInt("METADATA_MAX_TABLES", 5);
                    boolean metadataTestEnabled = getEnvAsBoolean("ENABLE_METADATA_TEST", false);
                    String metadataOutputFormat = getEnv("METADATA_OUTPUT_FORMAT", "json");

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
                        System.out.println("Metadata test đang tắt. Bật ENABLE_METADATA_TEST=true để chạy.");
                    }

                    // Thực hiện các truy vấn đọc từ sourceConn và ghi vào targetConn
                    // ... [Logic Migration của bạn ở đây] ...

                } // Tự động trả connection về pool nhờ try-with-resources

            } else {
                System.err.println("Không thể thiết lập kết nối, hủy bỏ Migration.");
            }

        } catch (SQLException | RuntimeException e) {
            System.err.println("Migration thất bại: " + e.getMessage());
        } finally {
            // Đảm bảo dọn dẹp tài nguyên khi kết thúc task
            manager.closeAllPools();
        }
    }

    private static void configureAppTimezone() {
        String timezone = getEnv("APP_TIMEZONE", "UTC");
        TimeZone.setDefault(TimeZone.getTimeZone(timezone));
        System.setProperty("user.timezone", TimeZone.getDefault().getID());
        System.out.println("Timezone hiện tại của app: " + TimeZone.getDefault().getID());
    }

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
                        "Chưa tạo được pool [" + poolId + "] lần " + attempt + "/" + POOL_INIT_MAX_RETRIES
                                + ", thử lại sau " + (POOL_INIT_RETRY_DELAY_MS / 1000) + " giây. Lý do: "
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
            throw new RuntimeException("Bị gián đoạn khi chờ kết nối DB.", e);
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

        System.out.println("\n=== Extract metadata từ " + connectionLabel + " | schema=" + schema + " ===");
        if (tableNames.isEmpty()) {
            System.out.println("Không tìm thấy bảng nào trong schema " + schema + ".");
            return;
        }

        System.out.println("Tìm thấy " + tableNames.size() + " bảng, hiển thị " + displayCount + " bảng đầu tiên.");

        for (TableDefinition tableDefinition : tableDefinitions) {
            printTableDefinition(tableDefinition);
        }

        if (tableNames.size() > displayCount) {
            System.out.println("... còn " + (tableNames.size() - displayCount) + " bảng chưa hiển thị.");
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
                case '\"' -> escaped.append("\\\"");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> escaped.append(ch);
            }
        }
        return escaped.toString();
    }

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    private static boolean getEnvAsBoolean(String name, boolean defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        if ("true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value) || "y".equalsIgnoreCase(value)) {
            return true;
        }

        if ("false".equalsIgnoreCase(value) || "0".equals(value) || "no".equalsIgnoreCase(value) || "n".equalsIgnoreCase(value)) {
            return false;
        }

        System.err.println("Biến môi trường " + name + " không hợp lệ, dùng mặc định " + defaultValue);
        return defaultValue;
    }

    private static int getEnvAsInt(String name, int defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            System.err.println("Biến môi trường " + name + " không hợp lệ, dùng mặc định " + defaultValue);
            return defaultValue;
        }
    }
}

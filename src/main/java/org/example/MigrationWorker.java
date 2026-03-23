package org.example;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.swing.SwingWorker;

// SwingWorker<Kết Quả Cuối Cùng, Kiểu Dữ Liệu Cập Nhật Giữa Chừng>
public class MigrationWorker extends SwingWorker<Void, String> {

    private final MigrationAppUI ui;
    private final boolean isStructureOnly;
    private final boolean isDataOnly;
    private final DatabaseConfig sourceConfig;
    private final DatabaseConfig targetConfig;
    private final String sourceSchema;
    private final String targetSchema;
    private final int batchSize;
    private final boolean truncateTarget;
    private final boolean copyNewOnly;
    private final Integer limitRows;

    public MigrationWorker(
            MigrationAppUI ui,
            boolean isStructureOnly,
            boolean isDataOnly,
            DatabaseConfig sourceConfig,
            DatabaseConfig targetConfig,
            String sourceSchema,
            String targetSchema,
            int batchSize,
            boolean truncateTarget,
            boolean copyNewOnly,
            Integer limitRows
    ) {
        this.ui = ui;
        this.isStructureOnly = isStructureOnly;
        this.isDataOnly = isDataOnly;
        this.sourceConfig = sourceConfig;
        this.targetConfig = targetConfig;
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
        this.batchSize = Math.max(1, batchSize);
        this.truncateTarget = truncateTarget;
        this.copyNewOnly = copyNewOnly;
        this.limitRows = (limitRows != null && limitRows > 0) ? limitRows : null;
    }

    /**
     * CHẠY DƯỚI BACKGROUND THREAD (Không được thao tác UI ở đây)
     */
    @Override
    protected Void doInBackground() throws Exception {
        ConnectionManager manager = ConnectionManager.getInstance();
        String sourcePoolId = "UI_SOURCE_" + System.currentTimeMillis();
        String targetPoolId = "UI_TARGET_" + System.currentTimeMillis();

        try {
            setProgress(0);
            publish("Đang thiết lập kết nối tới cơ sở dữ liệu...");
            manager.createPool(sourcePoolId, sourceConfig);
            manager.createPool(targetPoolId, targetConfig);

            if (!manager.testConnection(sourcePoolId) || !manager.testConnection(targetPoolId)) {
                throw new SQLException("Không thể thiết lập kết nối tới Source/Target DB.");
            }

            try (Connection sourceConn = manager.getConnection(sourcePoolId);
                 Connection targetConn = manager.getConnection(targetPoolId)) {

                setProgress(5);
                publish("Source URL: " + sourceConn.getMetaData().getURL());
                publish("Target URL: " + targetConn.getMetaData().getURL());
                publish("Schema nguồn: " + sourceSchema + " | Schema đích: " + targetSchema);
                publish("Tùy chọn: truncate=" + truncateTarget
                    + ", copyNewOnly=" + copyNewOnly
                    + ", limit=" + (limitRows == null ? "ALL" : limitRows));

                MetadataExtractor metadataExtractor = new MetadataExtractor();
                publish("Đang đọc metadata từ schema nguồn: " + sourceSchema + "...");
                List<String> tableNames = metadataExtractor.getTableNames(sourceConn, sourceSchema);

                if (tableNames.isEmpty()) {
                    publish("Không tìm thấy bảng nào trong schema nguồn " + sourceSchema + ".");
                    setProgress(100);
                    return null;
                }

                publish("Tìm thấy " + tableNames.size() + " bảng. Đang trích xuất cấu trúc chi tiết...");
                List<TableDefinition> allTables = new ArrayList<>();
                int totalTables = tableNames.size();
                for (int i = 0; i < totalTables; i++) {
                    ensureNotCancelled();
                    String tableName = tableNames.get(i);
                    allTables.add(metadataExtractor.extractTableDefinition(sourceConn, sourceSchema, tableName));
                    publish("  -> Đã đọc metadata bảng " + tableName);
                    setProgress(5 + (int) (((i + 1) / (float) totalTables) * 25));
                }

                SqlDialect sourceDialect = DialectFactory.getDialect(sourceConfig.getType());
                SqlDialect targetDialect = DialectFactory.getDialect(targetConfig.getType());
                allTables = deduplicateTablesForTarget(allTables, targetDialect);
                totalTables = allTables.size();

                if (totalTables == 0) {
                    publish("Không còn bảng hợp lệ để migrate sau khi đối chiếu tên bảng trên target.");
                    setProgress(100);
                    return null;
                }

                if (!isDataOnly) {
                    publish("--- BẮT ĐẦU TẠO CẤU TRÚC BẢNG (DDL) TRÊN TARGET ---");
                    runCreateTablesPhase(targetConn, allTables, targetDialect);
                    setProgress(60);
                } else {
                    publish("Bỏ qua bước tạo cấu trúc do chọn chế độ Data Only.");
                    setProgress(60);
                }

                if (!isStructureOnly) {
                    publish("--- BẮT ĐẦU CHUYỂN DỮ LIỆU (DML) ---");
                    SqlGenerator sqlGenerator = new SqlGenerator(sourceDialect, targetDialect);
                    DataTransferService transferService = new DataTransferService(sqlGenerator);

                    if (truncateTarget) {
                        publish("--- TRUNCATE DỮ LIỆU CŨ TRÊN TARGET ---");
                        runTruncatePhase(targetConn, allTables, targetDialect);
                    }

                    for (int i = 0; i < totalTables; i++) {
                        ensureNotCancelled();
                        TableDefinition table = allTables.get(i);

                        if (copyNewOnly && table.getPrimaryKeys().isEmpty()) {
                            publish("  -> Bảng " + table.getTableName() + " không có PK, copyNewOnly không thể lọc trùng theo PK.");
                        }

                        publish("Đang sao chép dữ liệu bảng " + table.getTableName() + "...");
                        DataTransferService.TransferResult result = transferService.transferTableData(
                                sourceConn,
                                targetConn,
                                table,
                                batchSize,
                                limitRows,
                                copyNewOnly,
                                (tableName, justTransferred, totalTransferred, totalSkipped) ->
                                        publish("    -> Batch " + tableName
                                                + ": +" + justTransferred
                                                + " dòng, tổng=" + totalTransferred
                                                + ", bỏ qua=" + totalSkipped)
                        );
                        publish("  -> Hoàn tất bảng " + table.getTableName()
                                + " | copied=" + result.getTransferredRows()
                                + " | skipped=" + result.getSkippedRows()
                                + (result.isLimitReached() ? " | đạt ngưỡng limit" : ""));
                        setProgress(60 + (int) (((i + 1) / (float) totalTables) * 30));
                    }
                } else {
                    publish("Bỏ qua bước chuyển dữ liệu do chọn chế độ Structure Only.");
                    setProgress(90);
                }

                if (!isDataOnly) {
                    publish("--- BẮT ĐẦU THÊM KHÓA NGOẠI (FOREIGN KEYS) ---");
                    runAddForeignKeysPhase(targetConn, allTables, targetDialect);
                } else {
                    publish("Bỏ qua bước thêm khóa ngoại do chọn chế độ Data Only.");
                }
            }

            setProgress(100);
            publish("Toàn bộ tiến trình Migration đã hoàn tất thành công!");

        } catch (SQLException | RuntimeException e) {
            publish("[LỖI NGHIÊM TRỌNG]: " + e.getMessage());
            throw e;
        } finally {
            manager.closePool(sourcePoolId);
            manager.closePool(targetPoolId);
        }

        return null;
    }

    private void runCreateTablesPhase(
            Connection targetConn,
            List<TableDefinition> allTables,
            SqlDialect targetDialect
    ) throws SQLException {
        try (Statement statement = targetConn.createStatement()) {
            for (TableDefinition table : allTables) {
                ensureNotCancelled();
                String createSql = normalizeSqlForJdbc(targetDialect.buildCreateTableSql(table));
                try {
                    statement.execute(createSql);
                    publish("  -> Đã tạo bảng " + table.getTableName());
                } catch (SQLException e) {
                    if (isTableAlreadyExistsError(e)) {
                        publish("  -> Bỏ qua bảng đã tồn tại: " + table.getTableName());
                    } else {
                        throw e;
                    }
                }

                if (targetDialect instanceof OracleDialect) {
                    ensureOracleIdentityColumnsAllowExplicitInsert(statement, table, targetDialect);
                }
            }
        }
    }

    private void ensureOracleIdentityColumnsAllowExplicitInsert(
            Statement statement,
            TableDefinition table,
            SqlDialect targetDialect
    ) throws SQLException {
        for (ColumnDefinition column : table.getColumns()) {
            if (!column.isAutoIncrement()) {
                continue;
            }

            String alterIdentitySql = "ALTER TABLE "
                    + targetDialect.quoteIdentifier(table.getTableName())
                    + " MODIFY "
                    + targetDialect.quoteIdentifier(column.getName())
                    + " GENERATED BY DEFAULT AS IDENTITY";

            try {
                statement.execute(alterIdentitySql);
                publish("  -> Chuẩn hóa identity BY DEFAULT cho "
                        + table.getTableName() + "." + column.getName());
            } catch (SQLException e) {
                if (isIdentityAlterNotApplicableError(e)) {
                    continue;
                }
                throw e;
            }
        }
    }

    private void runAddForeignKeysPhase(
            Connection targetConn,
            List<TableDefinition> allTables,
            SqlDialect targetDialect
    ) throws SQLException {
        try (Statement statement = targetConn.createStatement()) {
            int totalTables = allTables.size();
            for (int i = 0; i < totalTables; i++) {
                ensureNotCancelled();
                TableDefinition table = allTables.get(i);
                for (String fkSql : targetDialect.buildAddForeignKeySql(table)) {
                    try {
                        statement.execute(normalizeSqlForJdbc(fkSql));
                    } catch (SQLException e) {
                        if (isConstraintAlreadyExistsError(e) || isIncompatibleForeignKeyError(e)) {
                            publish("  -> Bỏ qua FK không thể áp dụng cho bảng " + table.getTableName() + ": " + e.getMessage());
                            continue;
                        }
                        throw e;
                    }
                }
                setProgress(90 + (int) (((i + 1) / (float) totalTables) * 10));
            }
        }
    }

    private void runTruncatePhase(
            Connection targetConn,
            List<TableDefinition> allTables,
            SqlDialect targetDialect
    ) throws SQLException {
        if (targetDialect == null) {
            throw new IllegalArgumentException("Target dialect không hợp lệ.");
        }

        List<TableDefinition> oracleDeleteFallbackTables = new ArrayList<>();

        try (Statement statement = targetConn.createStatement()) {
            for (TableDefinition table : allTables) {
                ensureNotCancelled();
                String tableNameForSql = table.getTableName();
                String truncateSql;

                if (targetDialect instanceof PostgresDialect) {
                    truncateSql = "TRUNCATE TABLE "
                            + targetDialect.quoteIdentifier(tableNameForSql)
                            + " RESTART IDENTITY CASCADE";
                } else if (targetDialect instanceof OracleDialect) {
                    truncateSql = "TRUNCATE TABLE "
                            + targetDialect.quoteIdentifier(tableNameForSql.toUpperCase(Locale.ROOT));
                } else {
                    truncateSql = "TRUNCATE TABLE " + targetDialect.quoteIdentifier(tableNameForSql);
                }

                try {
                    statement.execute(truncateSql);
                    publish("  -> Đã truncate bảng " + table.getTableName());
                } catch (SQLException e) {
                    if (targetDialect instanceof OracleDialect && isTruncateBlockedByForeignKey(e)) {
                        oracleDeleteFallbackTables.add(table);
                        publish("  -> Bảng " + table.getTableName() + " bị chặn TRUNCATE bởi FK, sẽ fallback sang DELETE.");
                        continue;
                    }

                    if (isTableNotExistsError(e)) {
                        publish("  -> Bỏ qua truncate vì bảng chưa tồn tại: " + table.getTableName());
                        continue;
                    }
                    throw e;
                }
            }
        }

        if (targetDialect instanceof OracleDialect && !oracleDeleteFallbackTables.isEmpty()) {
            runOracleDeleteFallbackPhase(targetConn, oracleDeleteFallbackTables, targetDialect);
        }
    }

    private void runOracleDeleteFallbackPhase(
            Connection targetConn,
            List<TableDefinition> fallbackTables,
            SqlDialect targetDialect
    ) throws SQLException {
        List<TableDefinition> remaining = new ArrayList<>(fallbackTables);
        int pass = 1;

        while (!remaining.isEmpty()) {
            ensureNotCancelled();
            boolean deletedAnyInThisPass = false;
            List<TableDefinition> nextRemaining = new ArrayList<>();

            try (Statement statement = targetConn.createStatement()) {
                for (TableDefinition table : remaining) {
                    String deleteSql = "DELETE FROM "
                            + targetDialect.quoteIdentifier(table.getTableName().toUpperCase(Locale.ROOT));

                    try {
                        int deleted = statement.executeUpdate(deleteSql);
                        publish("  -> Fallback DELETE bảng " + table.getTableName() + ": " + deleted + " dòng.");
                        deletedAnyInThisPass = true;
                    } catch (SQLException e) {
                        if (isTableNotExistsError(e)) {
                            publish("  -> Bỏ qua DELETE vì bảng chưa tồn tại: " + table.getTableName());
                            deletedAnyInThisPass = true;
                            continue;
                        }

                        if (isDeleteBlockedByChildRows(e)) {
                            nextRemaining.add(table);
                            continue;
                        }

                        throw e;
                    }
                }
            }

            if (nextRemaining.isEmpty()) {
                return;
            }

            if (!deletedAnyInThisPass) {
                String blockedTables = nextRemaining.stream()
                        .map(TableDefinition::getTableName)
                        .reduce((a, b) -> a + ", " + b)
                        .orElse("unknown");
                throw new SQLException(
                        "Không thể dọn dữ liệu cho các bảng Oracle do ràng buộc FK vòng lặp hoặc dữ liệu tham chiếu còn tồn tại: "
                                + blockedTables
                );
            }

            publish("  -> Tiếp tục lượt DELETE fallback " + (++pass) + " cho các bảng còn phụ thuộc FK...");
            remaining = nextRemaining;
        }
    }

    private void ensureNotCancelled() {
        if (isCancelled()) {
            throw new RuntimeException("Tiến trình đã bị hủy.");
        }
    }

    private List<TableDefinition> deduplicateTablesForTarget(
            List<TableDefinition> allTables,
            SqlDialect targetDialect
    ) {
        if (!(targetDialect instanceof OracleDialect)) {
            return allTables;
        }

        Map<String, TableDefinition> uniqueTables = new LinkedHashMap<>();
        for (TableDefinition table : allTables) {
            String targetKey = table.getTableName().toUpperCase(Locale.ROOT);
            TableDefinition existing = uniqueTables.get(targetKey);
            if (existing != null) {
                publish("  -> Bỏ qua bảng " + table.getTableName()
                        + " vì trùng tên vật lý trên Oracle với bảng "
                        + existing.getTableName() + " (" + targetKey + ").");
                continue;
            }
            uniqueTables.put(targetKey, table);
        }

        return new ArrayList<>(uniqueTables.values());
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

    private static boolean isConstraintAlreadyExistsError(SQLException e) {
        String sqlState = e.getSQLState();
        String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        return "42710".equals(sqlState)
                || message.contains("constraint") && message.contains("already exists")
                || message.contains("ora-02275");
    }

    private static boolean isTableNotExistsError(SQLException e) {
        String sqlState = e.getSQLState();
        String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        return "42P01".equals(sqlState)
                || message.contains("does not exist")
                || message.contains("ora-00942");
    }

    private static boolean isTruncateBlockedByForeignKey(SQLException e) {
        String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        return message.contains("ora-02266")
                || (message.contains("foreign key") && message.contains("truncate"));
    }

    private static boolean isDeleteBlockedByChildRows(SQLException e) {
        String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        return message.contains("ora-02292")
                || (message.contains("child record") && message.contains("found"));
    }

    private static boolean isIdentityAlterNotApplicableError(SQLException e) {
        String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        return message.contains("ora-30673")
                || message.contains("ora-32793")
                || message.contains("ora-32794")
                || message.contains("ora-32799");
    }

    private static boolean isIncompatibleForeignKeyError(SQLException e) {
        String sqlState = e.getSQLState();
        String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        return "42804".equals(sqlState)
                || message.contains("ora-02267")
                || (message.contains("foreign key") && message.contains("incompatible"));
    }

    /**
     * CHẠY TRÊN UI THREAD: Nhận dữ liệu từ publish() và cập nhật giao diện
     */
    @Override
    protected void process(List<String> chunks) {
        // Có thể nhận nhiều message cùng lúc nếu loop chạy quá nhanh, nên ta dùng vòng lặp
        for (String message : chunks) {
            ui.appendLog(message);
        }
    }

    /**
     * CHẠY TRÊN UI THREAD: Gọi khi doInBackground kết thúc (thành công hoặc lỗi)
     */
    @Override
    protected void done() {
        try {
            get(); // Gọi get() để bắt các Exception nếu có bắn ra từ doInBackground
            ui.appendLog("\n=== HOÀN TẤT ===");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ui.appendLog("\n=== THẤT BẠI: Tiến trình bị gián đoạn ===");
        } catch (ExecutionException e) {
            ui.appendLog("\n=== THẤT BẠI: Quá trình bị gián đoạn ===");
        } finally {
            // Mở khóa lại nút Start Migration khi xong việc
            ui.enableStartButton(true);
        }
    }
}
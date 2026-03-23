package org.example;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;

public class DataTransferService {

    @FunctionalInterface
    public interface TransferProgressListener {
        void onBatchCommitted(String tableName, int justTransferred, int totalTransferred, int totalSkipped);
    }

    public static final class TransferResult {
        private final int transferredRows;
        private final int skippedRows;
        private final boolean limitReached;

        public TransferResult(int transferredRows, int skippedRows, boolean limitReached) {
            this.transferredRows = transferredRows;
            this.skippedRows = skippedRows;
            this.limitReached = limitReached;
        }

        public int getTransferredRows() {
            return transferredRows;
        }

        public int getSkippedRows() {
            return skippedRows;
        }

        public boolean isLimitReached() {
            return limitReached;
        }
    }

    private final SqlGenerator sqlGenerator;

    public DataTransferService(SqlGenerator sqlGenerator) {
        this.sqlGenerator = sqlGenerator;
    }

    /**
     * Chuyển dữ liệu của một bảng từ Source sang Target
     * * @param sourceConn Kết nối DB Nguồn
     * @param targetConn Kết nối DB Đích
     * @param table      Định nghĩa bảng
     * @param batchSize  Kích thước mỗi lô (ví dụ: 1000, 5000)
     */
    public void transferTableData(Connection sourceConn, Connection targetConn, TableDefinition table, int batchSize) throws SQLException {
        transferTableData(sourceConn, targetConn, table, batchSize, null, false, null);
    }

    public TransferResult transferTableData(
            Connection sourceConn,
            Connection targetConn,
            TableDefinition table,
            int batchSize,
            Integer limitRows,
            boolean copyNewOnly,
            TransferProgressListener progressListener
    ) throws SQLException {

        String selectSql = sqlGenerator.buildSelectSql(table);
        String insertSql = sqlGenerator.buildInsertSql(table);
        String existsByPkSql = null;
        int[] pkColumnIndexes = new int[0];

        if (copyNewOnly && !table.getPrimaryKeys().isEmpty()) {
            existsByPkSql = sqlGenerator.buildExistsByPrimaryKeySql(table);
            pkColumnIndexes = resolvePkIndexes(table);
        }

        int columnCount = table.getColumns().size();
        int safeBatchSize = Math.max(1, batchSize);
        Integer safeLimitRows = (limitRows != null && limitRows > 0) ? limitRows : null;

        // Tắt AutoCommit ở cả hai phía để tối ưu hiệu suất và bật Streaming
        boolean originalSourceAutoCommit = sourceConn.getAutoCommit();
        boolean originalTargetAutoCommit = targetConn.getAutoCommit();
        sourceConn.setAutoCommit(false);
        targetConn.setAutoCommit(false);

        System.out.println("Bắt đầu migrate dữ liệu bảng: " + table.getTableName());

        // Sử dụng TYPE_FORWARD_ONLY và CONCUR_READ_ONLY để tối ưu hóa bộ nhớ khi đọc
        try (Statement sourceStmt = sourceConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             PreparedStatement targetPstmt = targetConn.prepareStatement(insertSql);
             PreparedStatement existsByPkStmt = existsByPkSql == null ? null : targetConn.prepareStatement(existsByPkSql)) {

            // Cấu hình Fetch Size: Số lượng row tải về RAM mỗi lần (Tránh OOM)
            sourceStmt.setFetchSize(safeBatchSize);

            try (ResultSet rs = sourceStmt.executeQuery(selectSql)) {
                int totalTransferred = 0;
                int totalSkipped = 0;
                int currentBatchCount = 0;
                boolean limitReached = false;

                while (rs.next()) {
                    if (safeLimitRows != null && totalTransferred >= safeLimitRows) {
                        limitReached = true;
                        break;
                    }

                    if (existsByPkStmt != null && rowExistsByPrimaryKey(rs, table, existsByPkStmt, pkColumnIndexes)) {
                        totalSkipped++;
                        continue;
                    }

                    // Đọc từng cột và gán vào tham số của INSERT
                    for (int i = 1; i <= columnCount; i++) {
                        ColumnDefinition column = table.getColumns().get(i - 1);
                        int jdbcType = normalizeJdbcTypeForTarget(column.getJdbcType());

                        Object value = rs.getObject(i);
                        if (value == null) {
                            targetPstmt.setNull(i, jdbcType);
                            continue;
                        }

                        switch (jdbcType) {
                            case Types.TIMESTAMP -> targetPstmt.setTimestamp(i, rs.getTimestamp(i));
                            case Types.DATE -> targetPstmt.setDate(i, rs.getDate(i));
                            case Types.TIME -> targetPstmt.setTime(i, rs.getTime(i));
                            default ->
                                    // Truyền explicit JDBC type để tránh lỗi Oracle-specific object (vd: oracle.sql.TIMESTAMP)
                                    targetPstmt.setObject(i, value, jdbcType);
                        }
                    }

                    // Đưa lệnh INSERT đã được gán giá trị vào danh sách chờ (Batch)
                    targetPstmt.addBatch();
                    currentBatchCount++;
                    totalTransferred++;

                    // Khi Batch đầy, tiến hành gửi sang DB đích và Commit
                    if (currentBatchCount % safeBatchSize == 0) {
                        targetPstmt.executeBatch();
                        targetConn.commit(); // Lưu thay đổi xuống DB đích
                        targetPstmt.clearBatch();
                        if (progressListener != null) {
                            progressListener.onBatchCommitted(table.getTableName(), currentBatchCount, totalTransferred, totalSkipped);
                        }
                        currentBatchCount = 0;
                        System.out.println("  -> Đã copy " + totalTransferred + " rows...");
                    }
                }

                // Thực thi nốt những dòng còn dư cuối cùng (nếu số dòng không chia hết cho batchSize)
                if (currentBatchCount > 0) {
                    targetPstmt.executeBatch();
                    targetConn.commit();
                    if (progressListener != null) {
                        progressListener.onBatchCommitted(table.getTableName(), currentBatchCount, totalTransferred, totalSkipped);
                    }
                    System.out.println("  -> Đã copy " + totalTransferred + " rows...");
                }

                System.out.println("Hoàn tất! Tổng cộng: " + totalTransferred + " rows cho bảng " + table.getTableName());
                return new TransferResult(totalTransferred, totalSkipped, limitReached);
            }

        } catch (SQLException e) {
            // Rollback nếu có lỗi xảy ra để đảm bảo tính toàn vẹn dữ liệu
            targetConn.rollback();
            System.err.println("Lỗi khi transfer dữ liệu bảng " + table.getTableName() + ". Đã rollback!");
            throw e;
        } finally {
            // Khôi phục trạng thái ban đầu
            sourceConn.setAutoCommit(originalSourceAutoCommit);
            targetConn.setAutoCommit(originalTargetAutoCommit);
        }
    }

    private static boolean rowExistsByPrimaryKey(
            ResultSet sourceRs,
            TableDefinition table,
            PreparedStatement existsByPkStmt,
            int[] pkColumnIndexes
    ) throws SQLException {
        List<String> primaryKeys = table.getPrimaryKeys();
        for (int i = 0; i < primaryKeys.size(); i++) {
            int sourceColumnIndex = pkColumnIndexes[i];
            ColumnDefinition sourceColumn = table.getColumns().get(sourceColumnIndex - 1);
            int jdbcType = normalizeJdbcTypeForTarget(sourceColumn.getJdbcType());

            Object value = sourceRs.getObject(sourceColumnIndex);
            if (value == null) {
                existsByPkStmt.setNull(i + 1, jdbcType);
            } else {
                existsByPkStmt.setObject(i + 1, value, jdbcType);
            }
        }

        try (ResultSet checkRs = existsByPkStmt.executeQuery()) {
            return checkRs.next();
        }
    }

    private static int[] resolvePkIndexes(TableDefinition table) {
        List<String> primaryKeys = table.getPrimaryKeys();
        List<ColumnDefinition> columns = table.getColumns();
        int[] indexes = new int[primaryKeys.size()];

        for (int i = 0; i < primaryKeys.size(); i++) {
            String pkName = primaryKeys.get(i);
            int foundIndex = -1;
            for (int j = 0; j < columns.size(); j++) {
                if (columns.get(j).getName().equalsIgnoreCase(pkName)) {
                    foundIndex = j + 1;
                    break;
                }
            }

            if (foundIndex == -1) {
                throw new IllegalArgumentException(
                        "Không tìm thấy cột PK " + pkName + " trong bảng " + table.getTableName()
                );
            }

            indexes[i] = foundIndex;
        }

        return indexes;
    }

    private static int normalizeJdbcTypeForTarget(int jdbcType) {
        if (jdbcType == Types.TIMESTAMP_WITH_TIMEZONE) {
            return Types.TIMESTAMP;
        }
        if (jdbcType == Types.TIME_WITH_TIMEZONE) {
            return Types.TIME;
        }
        return jdbcType;
    }
}
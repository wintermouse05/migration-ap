package org.example;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.sql.Statement;
import java.sql.SQLException;

public class DataTransferService {

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

        String selectSql = sqlGenerator.buildSelectSql(table);
        String insertSql = sqlGenerator.buildInsertSql(table);
        int columnCount = table.getColumns().size();

        // Tắt AutoCommit ở cả hai phía để tối ưu hiệu suất và bật Streaming
        boolean originalSourceAutoCommit = sourceConn.getAutoCommit();
        boolean originalTargetAutoCommit = targetConn.getAutoCommit();
        sourceConn.setAutoCommit(false);
        targetConn.setAutoCommit(false);

        System.out.println("Bắt đầu migrate dữ liệu bảng: " + table.getTableName());

        // Sử dụng TYPE_FORWARD_ONLY và CONCUR_READ_ONLY để tối ưu hóa bộ nhớ khi đọc
        try (Statement sourceStmt = sourceConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             PreparedStatement targetPstmt = targetConn.prepareStatement(insertSql)) {

            // Cấu hình Fetch Size: Số lượng row tải về RAM mỗi lần (Tránh OOM)
            sourceStmt.setFetchSize(batchSize);

            try (ResultSet rs = sourceStmt.executeQuery(selectSql)) {
                int totalTransferred = 0;
                int currentBatchCount = 0;

                while (rs.next()) {
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
                            case Types.TIMESTAMP:
                                targetPstmt.setTimestamp(i, rs.getTimestamp(i));
                                break;
                            case Types.DATE:
                                targetPstmt.setDate(i, rs.getDate(i));
                                break;
                            case Types.TIME:
                                targetPstmt.setTime(i, rs.getTime(i));
                                break;
                            default:
                                // Truyền explicit JDBC type để tránh lỗi Oracle-specific object (vd: oracle.sql.TIMESTAMP)
                                targetPstmt.setObject(i, value, jdbcType);
                                break;
                        }
                    }

                    // Đưa lệnh INSERT đã được gán giá trị vào danh sách chờ (Batch)
                    targetPstmt.addBatch();
                    currentBatchCount++;
                    totalTransferred++;

                    // Khi Batch đầy, tiến hành gửi sang DB đích và Commit
                    if (currentBatchCount % batchSize == 0) {
                        targetPstmt.executeBatch();
                        targetConn.commit(); // Lưu thay đổi xuống DB đích
                        targetPstmt.clearBatch();
                        currentBatchCount = 0;
                        System.out.println("  -> Đã copy " + totalTransferred + " rows...");
                    }
                }

                // Thực thi nốt những dòng còn dư cuối cùng (nếu số dòng không chia hết cho batchSize)
                if (currentBatchCount > 0) {
                    targetPstmt.executeBatch();
                    targetConn.commit();
                    System.out.println("  -> Đã copy " + totalTransferred + " rows...");
                }

                System.out.println("Hoàn tất! Tổng cộng: " + totalTransferred + " rows cho bảng " + table.getTableName());
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
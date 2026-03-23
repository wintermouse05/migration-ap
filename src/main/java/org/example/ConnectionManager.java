package org.example;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectionManager {
    private static ConnectionManager instance;
    private final Map<String, HikariDataSource> pools ;
    private ConnectionManager(){
        pools = new ConcurrentHashMap<>();

    }
    public static synchronized ConnectionManager getInstance(){
        if(instance == null){
            instance = new ConnectionManager();
        }
        return instance;
    }

    public void createPool(String poolId, DatabaseConfig config) {
        if (pools.containsKey(poolId)) {
            throw new IllegalArgumentException("Pool already exists!");
        }

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getJdbcUrl());
        hikariConfig.setUsername(config.getUsername());
        hikariConfig.setPassword(config.getPassword());
        hikariConfig.setDriverClassName(config.getType().getDriverClassName());
        hikariConfig.setMaximumPoolSize(config.getMaximumPoolSize());

        if (config.getType() == DatabaseType.POSTGRESQL) {
            normalizeJvmTimezoneForPostgres();
            // Tránh lỗi khi JVM timezone (vd: Asia/Saigon) không được PostgreSQL nhận diện.
            hikariConfig.addDataSourceProperty("options", "-c TimeZone=UTC");
            hikariConfig.addDataSourceProperty("TimeZone", "UTC");
        }

        // Các cấu hình tối ưu cho HikariCP
        hikariConfig.setConnectionTimeout(30000); // 30s
        hikariConfig.setIdleTimeout(600000);      // 10 phút
        hikariConfig.setMaxLifetime(1800000);     // 30 phút

        HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        pools.put(poolId, dataSource);

        System.out.println("Đã khởi tạo thành công Connection Pool: [" + poolId + "] (" + config.getType() + ")");
    }

    public Connection getConnection(String poolId) throws SQLException {
        HikariDataSource dataSource = pools.get(poolId);
        if  (dataSource == null) {
            throw new IllegalArgumentException("Pool Not Found!");
        }
        return dataSource.getConnection();
    }
    public boolean testConnection(String poolId) {
        try (Connection conn = getConnection(poolId)) {
            // isValid(timeout) là phương thức chuẩn của JDBC 4.0
            boolean isValid = conn.isValid(5); // timeout 5 giây
            System.out.println("Test connection [" + poolId + "]: " + (isValid ? "THÀNH CÔNG" : "THẤT BẠI"));
            return isValid;
        } catch (SQLException e) {
            System.err.println("Lỗi khi test connection [" + poolId + "]: " + e.getMessage());
            return false;
        }
    }

    public void closePool(String poolId) {
        HikariDataSource dataSource = pools.remove(poolId);
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            System.out.println("Pool [" + poolId + "] closed");
        }
    }

    public void closeAllPools() {
        for (String poolId: pools.keySet()) {
            closePool(poolId);
        }
    }

    private static void normalizeJvmTimezoneForPostgres() {
        String timezoneId = TimeZone.getDefault().getID();
        if (timezoneId == null || timezoneId.isBlank()) {
            return;
        }

        String normalized = timezoneId.trim().toLowerCase(Locale.ROOT);
        if ("asia/saigon".equals(normalized)) {
            TimeZone safeTz = TimeZone.getTimeZone("Asia/Ho_Chi_Minh");
            TimeZone.setDefault(safeTz);
            System.setProperty("user.timezone", safeTz.getID());
        }
    }
}

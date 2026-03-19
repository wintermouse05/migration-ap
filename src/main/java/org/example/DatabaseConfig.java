package org.example;

public class DatabaseConfig {
    private DatabaseType type;
    private String host;
    private int port;
    private String databaseName;
    private String username;
    private String password;
    private int maximumPoolSize = 10;

    public DatabaseConfig(DatabaseType type, String host, int port, String databaseName, String username, String password) {
        this.type = type;
        this.host = host;
        this.port = port;
        this.databaseName = databaseName;
        this.username = username;
        this.password = password;

    }

    public String getJdbcUrl() {
        switch (type) {
            case POSTGRESQL:
                return String.format("jdbc:postgresql://%s:%d/%s", host, port, databaseName);
            case ORACLE:
                // Sử dụng định dạng Service Name của Oracle
                return String.format("jdbc:oracle:thin:@//%s:%d/%s", host, port, databaseName);
            default:
                throw new UnsupportedOperationException("Database type chưa được hỗ trợ: " + type);
        }
    }

    public DatabaseType getType() {return type;}
    public String getHost() {return host;}
    public int getPort() {return port;}
    public String getDatabaseName() {return databaseName;}
    public String getUsername() {return username;}
    public String getPassword() {return password;}
    public int getMaximumPoolSize() {return maximumPoolSize;}
    public void setMaximumPoolSize(int maximumPoolSize) {this.maximumPoolSize = maximumPoolSize;}

}

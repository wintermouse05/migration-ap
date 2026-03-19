package org.example;

public class DialectFactory {
    public static SqlDialect getDialect(DatabaseType databaseType) {
        switch (databaseType) {
            case POSTGRESQL:
                return new PostgresDialect();
            case ORACLE:
                return new OracleDialect();
            default:
                throw new IllegalArgumentException("Unknown database type: " + databaseType);
        }
    }
}

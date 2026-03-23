package org.example;

import java.util.List;
import java.util.stream.Collectors;

public class SqlGenerator {

    private final SqlDialect sourceDialect;
    private final SqlDialect targetDialect;

    public SqlGenerator(SqlDialect sourceDialect, SqlDialect targetDialect) {
        this.sourceDialect = sourceDialect;
        this.targetDialect = targetDialect;
    }

    // Tạo lệnh SELECT từ Source
    public String buildSelectSql(TableDefinition table) {
        String columns = table.getColumns().stream()
                .map(c -> sourceDialect.quoteIdentifier(c.getName()))
                .collect(Collectors.joining(", "));

        return "SELECT " + columns + " FROM " + sourceDialect.quoteIdentifier(table.getTableName());
    }

    // Tạo lệnh INSERT dạng PreparedStatement cho Target
    public String buildInsertSql(TableDefinition table) {
        List<ColumnDefinition> columns = table.getColumns();

        String colNames = columns.stream()
                .map(c -> targetDialect.quoteIdentifier(c.getName()))
                .collect(Collectors.joining(", "));

        String placeholders = columns.stream()
                .map(c -> "?")
                .collect(Collectors.joining(", "));

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ")
            .append(targetDialect.quoteIdentifier(table.getTableName()))
            .append(" (")
            .append(colNames)
            .append(") VALUES (")
            .append(placeholders)
            .append(")");

        if (targetDialect instanceof PostgresDialect && !table.getPrimaryKeys().isEmpty()) {
            String conflictColumns = table.getPrimaryKeys().stream()
                .map(targetDialect::quoteIdentifier)
                .collect(Collectors.joining(", "));
            sql.append(" ON CONFLICT (")
                .append(conflictColumns)
                .append(") DO NOTHING");
        }

        return sql.toString();
    }

    public String buildExistsByPrimaryKeySql(TableDefinition table) {
        if (table.getPrimaryKeys().isEmpty()) {
            throw new IllegalArgumentException("Table " + table.getTableName() + " không có PK để kiểm tra tồn tại.");
        }

        String whereClause = table.getPrimaryKeys().stream()
                .map(pk -> targetDialect.quoteIdentifier(pk) + " = ?")
                .collect(Collectors.joining(" AND "));

        return "SELECT 1 FROM "
                + targetDialect.quoteIdentifier(table.getTableName())
                + " WHERE "
                + whereClause;
    }
}
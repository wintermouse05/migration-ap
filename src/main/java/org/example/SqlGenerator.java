package org.example;

import org.example.SqlDialect;
import org.example.TableDefinition;

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

        return "INSERT INTO " + targetDialect.quoteIdentifier(table.getTableName()) +
                " (" + colNames + ") VALUES (" + placeholders + ")";
    }
}
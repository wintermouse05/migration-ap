package org.example;

public interface SqlDialect {

    String mapDataType(ColumnDefinition columnDefinition);

    String buildCreateTableSql(TableDefinition tableDefinition);

    default String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";

    }
}

package org.example;

import java.util.List;

public interface SqlDialect {

    String mapDataType(ColumnDefinition columnDefinition);

    String buildCreateTableSql(TableDefinition tableDefinition);
    List<String> buildAddForeignKeySql(TableDefinition table);
    default String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";

    }
}

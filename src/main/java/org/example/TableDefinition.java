package org.example;

import java.util.ArrayList;
import java.util.List;

public class TableDefinition {
    private String tableName;
    private List<ColumnDefinition> columns = new ArrayList<>();
    private List<String> primaryKeys = new ArrayList<>();
    private List<ForeignKeyDefinition> foreignKeys = new ArrayList<>();

    public TableDefinition(String tableName) {
        this.tableName = tableName;
    }

    public void addColumn(ColumnDefinition columnDefinition) {
        this.columns.add(columnDefinition);
    }
    public void addPrimaryKey(String primaryKey) {
        this.primaryKeys.add(primaryKey);
    }
    public void addForeignKey(ForeignKeyDefinition foreignKey) {
        this.foreignKeys.add(foreignKey);
    }

    public String getTableName() {
        return tableName;
    }
    public List<ColumnDefinition> getColumns() {
        return columns;
    }
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }
    public List<ForeignKeyDefinition> getForeignKeys() {
        return foreignKeys;
    }
}

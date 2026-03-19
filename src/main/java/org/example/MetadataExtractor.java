package org.example;

import javax.swing.text.TabExpander;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MetadataExtractor {
    public List<String> getTableNames(Connection conn, String schema) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        DatabaseMetaData metaData = conn.getMetaData();
        try (ResultSet rs = metaData.getTables(null, schema, "%", new String[]{"TABLE"})) {
            while (rs.next()) {
                tableNames.add(rs.getString("TABLE_NAME"));
            }
        }
        return tableNames;

    }

    public TableDefinition extractTableDefinition(Connection conn, String schema, String tableName) throws SQLException {
        TableDefinition tableDef = new TableDefinition(tableName);
        DatabaseMetaData metaData = conn.getMetaData();
        try (ResultSet rsColumns = metaData.getColumns(null, schema, tableName, "%")) {
            while (rsColumns.next()) {
                String colName = rsColumns.getString("COLUMN_NAME");
                int dataType = rsColumns.getInt("DATA_TYPE");
                String typeName = rsColumns.getString("TYPE_NAME");
                int size = rsColumns.getInt("COLUMN_SIZE");
                int nullable = rsColumns.getInt("NULLABLE");
                String isAutoIncStr = rsColumns.getString("IS_AUTOINCREMENT");
                boolean isNullable = (nullable == 1);
                boolean isAutoIncrement = "YES".equalsIgnoreCase(isAutoIncStr);
                ColumnDefinition colDef = new ColumnDefinition(colName, dataType, typeName, size, isNullable, isAutoIncrement);
                tableDef.addColumn(colDef);
            }
        }

        try (ResultSet rsPK = metaData.getPrimaryKeys(null, schema, tableName)) {
            while (rsPK.next()) {
                String pkColumnName = rsPK.getString("COLUMN_NAME");
                tableDef.addPrimaryKey(pkColumnName);
            }
        }

        try (ResultSet rsFK =  metaData.getImportedKeys(null, schema, tableName)) {
            while (rsFK.next()) {
                String fkName = rsFK.getString("FK_NAME");
                String fkColumnName = rsFK.getString("FKCOLUMN_NAME");
                String targetTableName = rsFK.getString("PKTABLE_NAME");
                String targetColumnName = rsFK.getString("PKCOLUMN_NAME");
                ForeignKeyDefinition fkDef = new ForeignKeyDefinition(fkName, fkColumnName, targetTableName, targetColumnName);
                tableDef.addForeignKey(fkDef);

            }
        }
        return tableDef;
    }

}

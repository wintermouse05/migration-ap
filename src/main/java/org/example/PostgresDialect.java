package org.example;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class PostgresDialect implements SqlDialect {
    @Override
    public String mapDataType(ColumnDefinition columnDefinition) {
        if (columnDefinition.isAutoIncrement()) {
            if (columnDefinition.getJdbcType() == Types.BIGINT) {
                return "BIGSERIAL";
            }
            return "SERIAL";

        }
        switch (columnDefinition.getJdbcType()) {
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.CHAR:
                return columnDefinition.getSize() > 0 ? "VARCHAR(" + columnDefinition.getSize() + ")" : "TEXT";
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
                return "INTEGER";
            case Types.BIGINT:
                return "BIGINT";
            case Types.DECIMAL:
            case Types.NUMERIC:
                return "NUMERIC"; // Có thể mở rộng để lấy precision/scale nếu cần
            case Types.DOUBLE:
            case Types.FLOAT:
                return "DOUBLE PRECISION";
            case Types.BOOLEAN:
            case Types.BIT:
                return "BOOLEAN";
            case Types.DATE:
                return "DATE";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
                return "BYTEA";
            case Types.CLOB:
                return "TEXT";
            default:
                // Fallback tạm thời
                return "VARCHAR(255)";
        }
    }

    @Override
    public String buildCreateTableSql(TableDefinition table) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(quoteIdentifier(table.getTableName())).append(" (\n");

        List<ColumnDefinition> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            ColumnDefinition col = columns.get(i);

            sql.append("    ").append(quoteIdentifier(col.getName())).append(" ");
            sql.append(mapDataType(col));

            if (!col.isNullable()) {
                sql.append(" NOT NULL");
            }

            if (i < columns.size() - 1) {
                sql.append(",\n");
            }
        }

        // Thêm Primary Key nếu có
        List<String> pks = table.getPrimaryKeys();
        if (!pks.isEmpty()) {
            sql.append(",\n    PRIMARY KEY (");
            for (int i = 0; i < pks.size(); i++) {
                sql.append(quoteIdentifier(pks.get(i)));
                if (i < pks.size() - 1) sql.append(", ");
            }
            sql.append(")");
        }

        sql.append("\n);");
        return sql.toString();
    }
    @Override
    public List<String> buildAddForeignKeySql(TableDefinition table) {
        List<String> alterStatements = new ArrayList<>();

        for (ForeignKeyDefinition fk : table.getForeignKeys()) {
            StringBuilder sql = new StringBuilder();

            // Xử lý trường hợp tên constraint bị trùng hoặc null
            String constraintName = fk.getFkName();
            if (constraintName == null || constraintName.isEmpty()) {
                constraintName = "fk_" + table.getTableName() + "_" + fk.getFkColumnName();
            }

            sql.append("ALTER TABLE ").append(quoteIdentifier(table.getTableName())).append("\n");
            sql.append("    ADD CONSTRAINT ").append(quoteIdentifier(constraintName)).append("\n");
            sql.append("    FOREIGN KEY (").append(quoteIdentifier(fk.getFkColumnName())).append(")\n");
            sql.append("    REFERENCES ").append(quoteIdentifier(fk.getTargetTableName()))
                    .append(" (").append(quoteIdentifier(fk.getTargetColumnName())).append(");");

            alterStatements.add(sql.toString());
        }

        return alterStatements;
    }
}

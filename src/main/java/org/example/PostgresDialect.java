package org.example;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class PostgresDialect implements SqlDialect {
    @Override
    public String mapDataType(ColumnDefinition columnDefinition) {
        if (columnDefinition.isAutoIncrement()) {
            if (columnDefinition.getJdbcType() == Types.BIGINT || isOracleNumberType(columnDefinition)) {
                return "BIGSERIAL";
            }
            return "SERIAL";

        }

        if (isOracleNumberType(columnDefinition)) {
            return mapOracleNumberForPostgres(columnDefinition);
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
                if (columnDefinition.getScale() > 0 && columnDefinition.getSize() > 0) {
                    return "NUMERIC(" + columnDefinition.getSize() + ", " + columnDefinition.getScale() + ")";
                }
                return "NUMERIC";
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

    private static boolean isOracleNumberType(ColumnDefinition columnDefinition) {
        String typeName = columnDefinition.getTypeName();
        return typeName != null && typeName.toUpperCase(Locale.ROOT).startsWith("NUMBER");
    }

    private static String mapOracleNumberForPostgres(ColumnDefinition columnDefinition) {
        int precision = columnDefinition.getSize();
        int scale = columnDefinition.getScale();

        if (scale > 0) {
            if (precision > 0) {
                return "NUMERIC(" + precision + ", " + scale + ")";
            }
            return "NUMERIC";
        }

        if (precision <= 0) {
            return "BIGINT";
        }

        if (precision > 0 && precision <= 9) {
            return "INTEGER";
        }

        // Oracle NUMBER không khai báo precision thường được JDBC report là 22.
        // Với cột khóa/ID thực tế thường nằm trong phạm vi BIGINT nên ưu tiên BIGINT để đồng bộ FK.
        if ((precision > 9 && precision <= 18) || precision == 22) {
            return "BIGINT";
        }

        if (precision > 0) {
            return "NUMERIC(" + precision + ")";
        }

        return "NUMERIC";
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

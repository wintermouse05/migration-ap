package org.example;

import org.example.ColumnDefinition;
import org.example.SqlDialect;
import org.example.TableDefinition;

import java.sql.Types;
import java.util.List;

public class OracleDialect implements SqlDialect {

    @Override
    public String mapDataType(ColumnDefinition col) {
        String baseType;

        switch (col.getJdbcType()) {
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.CHAR:
                baseType = col.getSize() > 0 ? "VARCHAR2(" + col.getSize() + ")" : "VARCHAR2(4000)";
                break;
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                baseType = "NUMBER(10)";
                break;
            case Types.BIGINT:
                baseType = "NUMBER(19)";
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
            case Types.DOUBLE:
            case Types.FLOAT:
                baseType = "NUMBER";
                break;
            case Types.BOOLEAN:
            case Types.BIT:
                baseType = "NUMBER(1)"; // Oracle truyền thống dùng NUMBER(1) cho boolean
                break;
            case Types.DATE:
                baseType = "DATE";
                break;
            case Types.TIMESTAMP:
                baseType = "TIMESTAMP";
                break;
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
                baseType = "BLOB";
                break;
            case Types.CLOB:
                baseType = "CLOB";
                break;
            default:
                baseType = "VARCHAR2(255)";
        }

        // Xử lý tự tăng cho Oracle 12c+
        if (col.isAutoIncrement()) {
            return baseType + " GENERATED ALWAYS AS IDENTITY";
        }
        return baseType;
    }

    @Override
    public String buildCreateTableSql(TableDefinition table) {
        StringBuilder sql = new StringBuilder();
        // Oracle thường yêu cầu tên đối tượng viết hoa
        sql.append("CREATE TABLE ").append(quoteIdentifier(table.getTableName().toUpperCase())).append(" (\n");

        List<ColumnDefinition> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            ColumnDefinition col = columns.get(i);

            sql.append("    ").append(quoteIdentifier(col.getName().toUpperCase())).append(" ");
            sql.append(mapDataType(col));

            if (!col.isNullable() && !col.isAutoIncrement()) {
                sql.append(" NOT NULL");
            }

            if (i < columns.size() - 1) {
                sql.append(",\n");
            }
        }

        // Primary Key
        List<String> pks = table.getPrimaryKeys();
        if (!pks.isEmpty()) {
            sql.append(",\n    CONSTRAINT PK_").append(table.getTableName().toUpperCase()).append(" PRIMARY KEY (");
            for (int i = 0; i < pks.size(); i++) {
                sql.append(quoteIdentifier(pks.get(i).toUpperCase()));
                if (i < pks.size() - 1) sql.append(", ");
            }
            sql.append(")");
        }

        sql.append("\n)"); // Oracle không cần dấu ; trong statement thực thi qua JDBC
        return sql.toString();
    }
}
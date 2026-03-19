package org.example;

import lombok.Data;

@Data
public class ColumnDefinition {
    private String name;
    private int jdbcType;
    private String typeName;
    private int size;
    private boolean isNullable;
    private boolean isAutoIncrement;

    public ColumnDefinition(String name, int jdbcType, String typeName, int size, boolean isNullable, boolean isAutoIncrement) {
        this.name = name;
        this.jdbcType = jdbcType;
        this.typeName = typeName;
        this.size = size;
        this.isNullable = isNullable;
        this.isAutoIncrement = isAutoIncrement;
    }

}

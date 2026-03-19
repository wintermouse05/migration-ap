package org.example;

import lombok.Data;

@Data
public class ForeignKeyDefinition {
    private String fkName;
    private String fkColumnName;
    private String targetTableName;
    private String targetColumnName;

    public ForeignKeyDefinition(String fkName, String fkColumnName, String targetTableName, String targetColumnName) {
        this.fkName = fkName;
        this.fkColumnName = fkColumnName;
        this.targetTableName = targetTableName;
        this.targetColumnName = targetColumnName;
    }

}

package com.telcobright.db.metadata;

import com.telcobright.db.annotation.Column;

import java.lang.reflect.Field;
import java.time.LocalDateTime;

/**
 * Metadata for a single column/field
 */
public class ColumnMetadata {
    
    private final Field field;
    private final String columnName;
    private final String sqlType;
    private final boolean primaryKey;
    private final boolean indexed;
    private final boolean nullable;
    
    public ColumnMetadata(Field field, Column annotation) {
        this.field = field;
        
        if (annotation != null) {
            this.columnName = annotation.name().isEmpty() ? field.getName() : annotation.name();
            this.sqlType = annotation.type().isEmpty() ? inferSqlType(field.getType()) : annotation.type();
            this.primaryKey = annotation.primaryKey();
            this.indexed = annotation.indexed();
            this.nullable = annotation.nullable();
        } else {
            this.columnName = field.getName();
            this.sqlType = inferSqlType(field.getType());
            this.primaryKey = false;
            this.indexed = false;
            this.nullable = true;
        }
    }
    
    private String inferSqlType(Class<?> javaType) {
        if (javaType == Long.class || javaType == long.class) {
            return "BIGINT";
        } else if (javaType == Integer.class || javaType == int.class) {
            return "INT";
        } else if (javaType == String.class) {
            return "VARCHAR(255)";
        } else if (javaType == LocalDateTime.class) {
            return "DATETIME";
        } else if (javaType == Boolean.class || javaType == boolean.class) {
            return "BOOLEAN";
        } else {
            return "TEXT";
        }
    }
    
    public String getColumnDefinition() {
        StringBuilder def = new StringBuilder();
        def.append(columnName).append(" ").append(sqlType);
        
        if (primaryKey) {
            def.append(" PRIMARY KEY");
        } else if (!nullable) {
            def.append(" NOT NULL");
        }
        
        return def.toString();
    }
    
    public Object getValue(Object entity) {
        try {
            return field.get(entity);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to get field value: " + field.getName(), e);
        }
    }
    
    public void setValue(Object entity, Object value) {
        try {
            field.set(entity, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to set field value: " + field.getName(), e);
        }
    }
    
    // Getters
    public Field getField() { return field; }
    public String getColumnName() { return columnName; }
    public String getSqlType() { return sqlType; }
    public boolean isPrimaryKey() { return primaryKey; }
    public boolean isIndexed() { return indexed; }
    public boolean isNullable() { return nullable; }
}
package com.telcobright.db.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to configure column properties
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    
    /**
     * Column name (defaults to field name if not specified)
     */
    String name() default "";
    
    /**
     * Whether this column should be indexed
     */
    boolean indexed() default false;
    
    /**
     * Whether this is the primary key
     */
    boolean primaryKey() default false;
    
    /**
     * SQL column type (e.g., "VARCHAR(255)", "BIGINT", "DATETIME")
     */
    String type() default "";
    
    /**
     * Whether column is nullable
     */
    boolean nullable() default true;
}
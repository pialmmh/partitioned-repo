package com.telcobright.db.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to specify column mapping for entity fields
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    /**
     * The name of the column in the database
     */
    String name() default "";
    
    /**
     * Whether this column can be null
     */
    boolean nullable() default true;
    
    /**
     * Whether this column should be included in insert statements
     */
    boolean insertable() default true;
    
    /**
     * Whether this column should be included in update statements
     */
    boolean updatable() default true;
}
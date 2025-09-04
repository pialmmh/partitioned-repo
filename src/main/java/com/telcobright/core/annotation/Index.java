package com.telcobright.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark fields that should have database indexes created.
 * Can be applied to entity fields to automatically generate indexes in CREATE TABLE statements.
 * 
 * Examples:
 * <pre>
 * {@code @Index}
 * {@code @Column(name = "user_id")}
 * private Long userId;  // Creates: KEY idx_user_id (user_id)
 * 
 * {@code @Index(name = "custom_user_idx")}
 * {@code @Column(name = "user_id")}
 * private Long userId;  // Creates: KEY custom_user_idx (user_id)
 * 
 * {@code @Index(unique = true)}
 * {@code @Column(name = "email")}
 * private String email; // Creates: UNIQUE KEY idx_email (email)
 * </pre>
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Index {
    
    /**
     * Custom name for the index. If not specified, generates name as "idx_{column_name}".
     * 
     * @return custom index name, or empty string to use default naming
     */
    String name() default "";
    
    /**
     * Whether this should be a unique index.
     * 
     * @return true for UNIQUE KEY, false for regular KEY (default)
     */
    boolean unique() default false;
    
    /**
     * Optional comment for the index.
     * 
     * @return comment string, or empty for no comment
     */
    String comment() default "";
}
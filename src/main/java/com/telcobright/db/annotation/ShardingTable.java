package com.telcobright.db.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark a class as a sharded table entity
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ShardingTable {
    
    /**
     * Table name (e.g., "sms", "order", "call")
     */
    String value();
    
    /**
     * Sharding mode: PARTITIONED_TABLE or MULTI_TABLE
     */
    ShardingMode mode() default ShardingMode.MULTI_TABLE;
    
    /**
     * Shard key field name (usually "created_at")
     */
    String shardKey() default "created_at";
    
    /**
     * Retention span in days
     */
    int retentionSpanDays() default 7;
    
    /**
     * Partition adjustment time (e.g., "04:00")
     */
    String partitionAdjustmentTime() default "04:00";
    
    /**
     * Auto-manage partitions/tables
     */
    boolean autoManagePartition() default true;
}
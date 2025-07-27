package com.telcobright.db.annotation;

/**
 * Sharding storage mode
 */
public enum ShardingMode {
    /**
     * Multi-table sharding by date (e.g., sms_20250727, sms_20250728)
     * Creates separate physical tables for each day
     */
    MULTI_TABLE,
    
    /**
     * Single table with date-based partitions (e.g., event table with event_20250727 partitions)
     * One logical table sharded into physical partitions using date
     */
    PARTITIONED_TABLE
}
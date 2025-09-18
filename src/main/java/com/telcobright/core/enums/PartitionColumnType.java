package com.telcobright.core.enums;

/**
 * Defines the data type of the partition column.
 * Used to determine partitioning logic and table naming conventions.
 */
public enum PartitionColumnType {

    /**
     * LocalDateTime columns for time-based partitioning
     */
    LOCAL_DATE_TIME,

    /**
     * Long columns for numeric range partitioning (e.g., sequence numbers)
     */
    LONG,

    /**
     * Integer columns for smaller numeric ranges or categories
     */
    INTEGER,

    /**
     * Double columns for measurement-based partitioning
     */
    DOUBLE,

    /**
     * String columns for hash-based partitioning (e.g., customer_id, region)
     */
    STRING
}
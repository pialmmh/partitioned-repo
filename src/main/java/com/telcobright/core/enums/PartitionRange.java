package com.telcobright.core.enums;

/**
 * Defines the partitioning range or bucket size for table creation.
 */
public enum PartitionRange {

    // Time-based ranges
    HOURLY,
    DAILY,
    MONTHLY,
    YEARLY,

    // Hash-based buckets
    HASH_4,    // 4 hash buckets
    HASH_8,    // 8 hash buckets
    HASH_16,   // 16 hash buckets
    HASH_32,   // 32 hash buckets
    HASH_64,   // 64 hash buckets
    HASH_128,  // 128 hash buckets

    // Value-based ranges (for numeric partition columns)
    VALUE_RANGE_1K,    // New table every 1,000 values
    VALUE_RANGE_10K,   // New table every 10,000 values
    VALUE_RANGE_100K,  // New table every 100,000 values
    VALUE_RANGE_1M,    // New table every 1,000,000 values
    VALUE_RANGE_10M,   // New table every 10,000,000 values
    VALUE_RANGE_100M   // New table every 100,000,000 values
}
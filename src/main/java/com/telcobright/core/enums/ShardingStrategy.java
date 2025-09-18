package com.telcobright.core.enums;

/**
 * Defines the sharding and partitioning strategy for data distribution.
 */
public enum ShardingStrategy {

    /**
     * Single key strategy - ID serves as both shard key and partition key.
     * Suitable for: User profiles, accounts, simple entities
     * Query pattern: Direct ID lookup
     */
    SINGLE_KEY_HASH,

    /**
     * Dual key with range partitioning - ID for sharding, separate column for range-based partitioning.
     * Suitable for: Time-series data (orders, logs, events)
     * Query pattern: ID + date range queries
     */
    DUAL_KEY_HASH_RANGE,

    /**
     * Dual key with hash partitioning - ID for sharding, separate column for hash-based partitioning.
     * Suitable for: Categorical data distribution
     * Query pattern: ID + category queries
     */
    DUAL_KEY_HASH_HASH
}
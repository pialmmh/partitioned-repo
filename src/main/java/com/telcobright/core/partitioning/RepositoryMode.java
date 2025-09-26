package com.telcobright.core.partitioning;

/**
 * Defines the repository storage mode
 */
public enum RepositoryMode {
    /**
     * Single table with MySQL native partitions
     * - Table is divided into partitions by date
     * - All partitions created upfront in CREATE TABLE
     * - Requires composite primary key (id, partition_column)
     * - Better for queries spanning multiple days
     */
    PARTITIONED,

    /**
     * Separate table for each time period (day/hour/month)
     * - Each period gets its own table
     * - Tables created on-demand or upfront
     * - Simple primary key (id only)
     * - Better for complete day isolation and archival
     */
    MULTI_TABLE
}
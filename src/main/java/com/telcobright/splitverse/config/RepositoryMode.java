package com.telcobright.splitverse.config;

/**
 * Defines the repository storage mode for Split-Verse
 */
public enum RepositoryMode {
    /**
     * Single table with MySQL native partitions
     * - Table is divided into partitions by date (p20250914, p20250915, etc.)
     * - All partitions created upfront in CREATE TABLE
     * - Requires composite primary key (id, created_at)
     * - Better for queries spanning multiple days
     */
    PARTITIONED,

    /**
     * Separate table for each time period (day/hour/month)
     * - Each period gets its own table (table_2025_09_14, table_2025_09_15, etc.)
     * - Tables created on-demand or upfront
     * - Simple primary key (id only)
     * - Better for complete day isolation and archival
     */
    MULTI_TABLE
}
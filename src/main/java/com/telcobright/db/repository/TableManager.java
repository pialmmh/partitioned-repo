package com.telcobright.db.repository;

import com.telcobright.db.annotation.ShardingMode;
import com.telcobright.db.metadata.EntityMetadata;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages table/partition creation/deletion for sharded entities
 * Delegates to appropriate strategy based on sharding mode
 */
public class TableManager {
    
    private final DataSource dataSource;
    private final EntityMetadata<?> metadata;
    private final String databaseName;
    private final MultiTableManager multiTableManager;
    private final PartitionedTableManager partitionedTableManager;
    
    public TableManager(DataSource dataSource, EntityMetadata<?> metadata, String databaseName) {
        this.dataSource = dataSource;
        this.metadata = metadata;
        this.databaseName = databaseName;
        this.multiTableManager = new MultiTableManager(dataSource, metadata, databaseName);
        this.partitionedTableManager = new PartitionedTableManager(dataSource, metadata, databaseName);
    }
    
    /**
     * Initialize tables/partitions for the retention window using smart management
     * Only creates missing tables/partitions and drops excess ones
     */
    public void initializeTablesForRetentionWindow() {
        try {
            if (metadata.getShardingMode() == ShardingMode.MULTI_TABLE) {
                // For multi-table mode, use smart table management
                // No logical table needed - ShardingSphere routes directly to physical tables
                multiTableManager.manageTablesAtStartup();
            } else {
                // For partitioned table mode, let PartitionedTableManager handle everything
                // It will create the main partitioned table if needed
                partitionedTableManager.managePartitionsAtStartup();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize tables/partitions for retention window", e);
        }
    }
    
    /**
     * Create a logical table reference for ShardingSphere
     * This helps ShardingSphere understand the table structure
     */
    private void createLogicalTableReference() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Create a simple logical table with the same structure as physical tables
            // This table will remain empty - it's just for ShardingSphere metadata
            String logicalTableName = metadata.getTableName();
            String createLogicalTableSql = metadata.generateCreateTableSql(logicalTableName);
            
            // Remove partitioning from logical table (only physical tables need partitioning)
            if (createLogicalTableSql.contains("PARTITION BY")) {
                createLogicalTableSql = createLogicalTableSql.substring(0, createLogicalTableSql.indexOf("PARTITION BY")).trim();
            }
            
            // Drop existing logical table if it exists
            try {
                stmt.execute("DROP TABLE IF EXISTS " + logicalTableName);
            } catch (Exception e) {
                // Ignore if table doesn't exist
            }
            
            // Create logical table for ShardingSphere metadata
            stmt.execute(createLogicalTableSql);
            
            System.out.println("Created logical table reference: " + logicalTableName);
            
        } catch (SQLException e) {
            System.err.println("Warning: Could not create logical table reference: " + e.getMessage());
            throw e; // Re-throw as this is critical for ShardingSphere
        }
    }
    
    /**
     * Create tables for a specific date range
     */
    public void createTablesForDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        LocalDateTime current = startDate.toLocalDate().atStartOfDay();
        
        while (!current.isAfter(endDate)) {
            String tableName = metadata.getTableNameForDate(current);
            createTable(tableName);
            current = current.plusDays(1);
        }
    }
    
    /**
     * Create a single table
     */
    public void createTable(String tableName) throws SQLException {
        String sql = metadata.generateCreateTableSql(tableName);
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            stmt.execute(sql);
            System.out.println("Created table: " + tableName);
        }
    }
    
    /**
     * Drop expired tables (for retention management)
     */
    public void dropExpiredTables() throws SQLException {
        LocalDateTime cutoffDate = LocalDateTime.now().minusDays(metadata.getRetentionSpanDays());
        List<String> expiredTables = getExpiredTableNames(cutoffDate);
        
        for (String tableName : expiredTables) {
            dropTable(tableName);
        }
    }
    
    /**
     * Drop a single table
     */
    public void dropTable(String tableName) throws SQLException {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            stmt.execute(sql);
            System.out.println("Dropped table: " + tableName);
        }
    }
    
    /**
     * Create future tables for the next few days
     */
    public void createFutureTables(int daysAhead) throws SQLException {
        LocalDateTime startDate = LocalDateTime.now().plusDays(1);
        LocalDateTime endDate = startDate.plusDays(daysAhead);
        
        createTablesForDateRange(startDate, endDate);
    }
    
    /**
     * Get list of expired table names
     */
    private List<String> getExpiredTableNames(LocalDateTime cutoffDate) {
        List<String> expiredTables = new ArrayList<>();
        
        // This is a simplified approach - in production, you'd query the database
        // to get actual table names and compare them with the cutoff date
        for (int i = 1; i <= metadata.getRetentionSpanDays() * 2; i++) {
            LocalDateTime checkDate = cutoffDate.minusDays(i);
            String tableName = metadata.getTableNameForDate(checkDate);
            expiredTables.add(tableName);
        }
        
        return expiredTables;
    }
    
    /**
     * Maintenance task - should be called daily at partitionAdjustmentTime
     * Delegates to appropriate strategy based on sharding mode
     */
    public void performDailyMaintenance() throws SQLException {
        if (!metadata.isAutoManagePartition()) {
            System.out.println("Auto-management disabled, skipping daily maintenance");
            return;
        }
        
        if (metadata.getShardingMode() == ShardingMode.MULTI_TABLE) {
            multiTableManager.performDailyMaintenance();
        } else {
            partitionedTableManager.performDailyMaintenance();
        }
    }
}
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
 * Manages table creation/deletion for sharded entities
 */
public class TableManager {
    
    private final DataSource dataSource;
    private final EntityMetadata<?> metadata;
    
    public TableManager(DataSource dataSource, EntityMetadata<?> metadata) {
        this.dataSource = dataSource;
        this.metadata = metadata;
    }
    
    /**
     * Initialize tables for the retention window (past and future)
     */
    public void initializeTablesForRetentionWindow() {
        try {
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime startDate = now.minusDays(metadata.getRetentionSpanDays());
            LocalDateTime endDate = now.plusDays(metadata.getRetentionSpanDays());
            
            createTablesForDateRange(startDate, endDate);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize tables for retention window", e);
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
     */
    public void performDailyMaintenance() throws SQLException {
        if (!metadata.isAutoManagePartition()) {
            return;
        }
        
        // Create future tables
        createFutureTables(7);
        
        // Drop expired tables
        dropExpiredTables();
        
        System.out.println("Daily maintenance completed for tables: " + metadata.getTableName());
    }
}
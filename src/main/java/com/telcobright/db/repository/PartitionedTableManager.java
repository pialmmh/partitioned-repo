package com.telcobright.db.repository;

import com.telcobright.db.metadata.EntityMetadata;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

/**
 * Manages partition creation/deletion for partitioned table sharding strategy
 * Uses information_schema to determine existing partitions and creates/drops only as needed
 */
public class PartitionedTableManager {
    
    private final DataSource dataSource;
    private final EntityMetadata<?> metadata;
    private final String databaseName;
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    public PartitionedTableManager(DataSource dataSource, EntityMetadata<?> metadata, String databaseName) {
        this.dataSource = dataSource;
        this.metadata = metadata;
        this.databaseName = databaseName;
    }
    
    /**
     * Perform smart partition management based on retention policy
     * Only creates missing partitions and drops excess partitions
     */
    public void managePartitionsAtStartup() throws SQLException {
        System.out.println("=== Partitioned Table Smart Management ===");
        
        // 1. Ensure the main table exists
        ensureMainTableExists();
        
        // 2. Determine what partitions should exist based on retention policy
        Set<String> partitionsShouldExist = determinePartitionsShouldExist();
        System.out.println("Partitions that should exist (" + partitionsShouldExist.size() + "): " + partitionsShouldExist);
        
        // 3. Query information_schema to find existing partitions
        Set<String> existingPartitions = queryExistingPartitions();
        System.out.println("Existing partitions (" + existingPartitions.size() + "): " + existingPartitions);
        
        // 4. Create missing partitions
        Set<String> missingPartitions = new HashSet<>(partitionsShouldExist);
        missingPartitions.removeAll(existingPartitions);
        createMissingPartitions(missingPartitions);
        
        // 5. Drop excess partitions
        Set<String> excessPartitions = new HashSet<>(existingPartitions);
        excessPartitions.removeAll(partitionsShouldExist);
        dropExcessPartitions(excessPartitions);
        
        System.out.println("Partitioned table management completed");
    }
    
    /**
     * Ensure the main partitioned table exists
     */
    private void ensureMainTableExists() throws SQLException {
        String tableName = metadata.getTableName();
        
        // Check if table exists
        String checkQuery = """
            SELECT COUNT(*) as table_count
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ? 
            AND TABLE_NAME = ?
            """;
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(checkQuery)) {
            
            stmt.setString(1, databaseName);
            stmt.setString(2, tableName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next() && rs.getInt("table_count") > 0) {
                    System.out.println("  ✓ Main table exists: " + tableName);
                    return;
                }
            }
        }
        
        // Create main table with initial partition structure
        System.out.println("  Creating main partitioned table: " + tableName);
        createMainPartitionedTable(tableName);
    }
    
    /**
     * Create the main partitioned table with initial partition structure
     */
    private void createMainPartitionedTable(String tableName) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Generate CREATE TABLE SQL for partitioned table
            String createTableSql = generatePartitionedTableSql(tableName);
            stmt.execute(createTableSql);
            System.out.println("    ✓ Created main partitioned table: " + tableName);
        }
    }
    
    /**
     * Generate CREATE TABLE SQL for partitioned table with initial partitions
     */
    private String generatePartitionedTableSql(String tableName) {
        // Start with basic table structure (without partitioning)
        String sql = metadata.generateCreateTableSql(tableName);
        
        // Remove any existing partitioning clause
        if (sql.contains("PARTITION BY")) {
            sql = sql.substring(0, sql.indexOf("PARTITION BY")).trim();
        }
        
        // Add date-based partitioning by the shard key
        sql += "\nPARTITION BY RANGE (TO_DAYS(" + metadata.getShardKey() + ")) (\n";
        
        // Add initial partition for current date
        LocalDateTime now = LocalDateTime.now();
        String partitionName = "p" + now.format(dateFormatter);
        int dayValue = getDayValue(now);
        
        sql += "    PARTITION " + partitionName + " VALUES LESS THAN (" + (dayValue + 1) + ")\n";
        sql += ")";
        
        return sql;
    }
    
    /**
     * Determine which partitions should exist based on retention span
     */
    private Set<String> determinePartitionsShouldExist() {
        Set<String> shouldExist = new HashSet<>();
        LocalDateTime now = LocalDateTime.now();
        int retentionDays = metadata.getRetentionSpanDays();
        
        // Partitions for past retention days
        LocalDateTime startDate = now.minusDays(retentionDays);
        
        // Partitions for future retention days
        LocalDateTime endDate = now.plusDays(retentionDays);
        
        LocalDateTime current = startDate.toLocalDate().atStartOfDay();
        while (!current.isAfter(endDate)) {
            String partitionName = "p" + current.format(dateFormatter);
            shouldExist.add(partitionName);
            current = current.plusDays(1);
        }
        
        return shouldExist;
    }
    
    /**
     * Query information_schema to find existing partitions
     */
    private Set<String> queryExistingPartitions() throws SQLException {
        Set<String> existingPartitions = new HashSet<>();
        
        String query = """
            SELECT PARTITION_NAME 
            FROM INFORMATION_SCHEMA.PARTITIONS 
            WHERE TABLE_SCHEMA = ? 
            AND TABLE_NAME = ? 
            AND PARTITION_NAME IS NOT NULL
            """;
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            
            stmt.setString(1, databaseName);
            stmt.setString(2, metadata.getTableName());
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String partitionName = rs.getString("PARTITION_NAME");
                    if (isValidPartitionName(partitionName)) {
                        existingPartitions.add(partitionName);
                    }
                }
            }
        }
        
        return existingPartitions;
    }
    
    /**
     * Validate that partition name follows expected pattern: pYYYYMMDD
     */
    private boolean isValidPartitionName(String partitionName) {
        if (!partitionName.startsWith("p") || partitionName.length() != 9) {
            return false;
        }
        
        String datePart = partitionName.substring(1);
        try {
            // Try to parse the date part to validate format
            LocalDateTime.parse(datePart + "0000", DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Create missing partitions
     */
    private void createMissingPartitions(Set<String> missingPartitions) throws SQLException {
        if (missingPartitions.isEmpty()) {
            System.out.println("  ✓ No missing partitions to create");
            return;
        }
        
        System.out.println("  Creating " + missingPartitions.size() + " missing partitions:");
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            for (String partitionName : missingPartitions) {
                // Extract date from partition name (pYYYYMMDD -> YYYYMMDD)
                String datePart = partitionName.substring(1);
                LocalDateTime partitionDate = LocalDateTime.parse(datePart + "0000", 
                    DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
                
                int dayValue = getDayValue(partitionDate);
                
                String addPartitionSql = String.format(
                    "ALTER TABLE %s ADD PARTITION (PARTITION %s VALUES LESS THAN (%d))",
                    metadata.getTableName(), partitionName, dayValue + 1
                );
                
                stmt.execute(addPartitionSql);
                System.out.println("    ✓ Created partition: " + partitionName);
            }
        }
    }
    
    /**
     * Drop excess partitions that shouldn't exist based on retention policy
     */
    private void dropExcessPartitions(Set<String> excessPartitions) throws SQLException {
        if (excessPartitions.isEmpty()) {
            System.out.println("  ✓ No excess partitions to drop");
            return;
        }
        
        System.out.println("  Dropping " + excessPartitions.size() + " excess partitions:");
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            for (String partitionName : excessPartitions) {
                String dropPartitionSql = String.format(
                    "ALTER TABLE %s DROP PARTITION %s",
                    metadata.getTableName(), partitionName
                );
                
                stmt.execute(dropPartitionSql);
                System.out.println("    ✓ Dropped partition: " + partitionName);
            }
        }
    }
    
    /**
     * Calculate MySQL TO_DAYS value for a given date
     */
    private int getDayValue(LocalDateTime date) {
        // MySQL TO_DAYS function: number of days since year 0
        // For simplicity, we'll use a base calculation
        // In production, you might want to query SELECT TO_DAYS('YYYY-MM-DD') directly
        
        int year = date.getYear();
        int dayOfYear = date.getDayOfYear();
        
        // Approximate calculation (MySQL's TO_DAYS is more complex)
        // This is a simplified version - for exact values, query MySQL directly
        return (year - 1) * 365 + (year - 1) / 4 - (year - 1) / 100 + (year - 1) / 400 + dayOfYear;
    }
    
    /**
     * Perform daily maintenance - same logic as startup but with logging
     */
    public void performDailyMaintenance() throws SQLException {
        if (!metadata.isAutoManagePartition()) {
            System.out.println("Auto-management disabled, skipping daily maintenance");
            return;
        }
        
        System.out.println("=== Daily Partitioned Table Maintenance ===");
        managePartitionsAtStartup();
    }
}
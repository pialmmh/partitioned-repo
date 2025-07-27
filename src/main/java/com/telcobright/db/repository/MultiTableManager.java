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
 * Manages table creation/deletion for multi-table sharding strategy
 * Uses information_schema to determine existing tables and creates/drops only as needed
 */
public class MultiTableManager {
    
    private final DataSource dataSource;
    private final EntityMetadata<?> metadata;
    private final String databaseName;
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    public MultiTableManager(DataSource dataSource, EntityMetadata<?> metadata, String databaseName) {
        this.dataSource = dataSource;
        this.metadata = metadata;
        this.databaseName = databaseName;
    }
    
    /**
     * Perform smart table management based on retention policy
     * Only creates missing tables and drops excess tables
     */
    public void manageTablesAtStartup() throws SQLException {
        System.out.println("=== Multi-Table Smart Management ===");
        
        // 1. Determine what tables should exist based on retention policy
        Set<String> tablesShouldExist = determineTablesShouldExist();
        System.out.println("Tables that should exist (" + tablesShouldExist.size() + "): " + tablesShouldExist);
        
        // 2. Query information_schema to find existing tables
        Set<String> existingTables = queryExistingTables();
        System.out.println("Existing tables (" + existingTables.size() + "): " + existingTables);
        
        // 3. Create missing tables
        Set<String> missingTables = new HashSet<>(tablesShouldExist);
        missingTables.removeAll(existingTables);
        createMissingTables(missingTables);
        
        // 4. Drop excess tables
        Set<String> excessTables = new HashSet<>(existingTables);
        excessTables.removeAll(tablesShouldExist);
        dropExcessTables(excessTables);
        
        // 5. Drop logical table if it exists (not needed for multi-table mode)
        dropLogicalTableIfExists();
        
        System.out.println("Multi-table management completed");
    }
    
    /**
     * Determine which tables should exist based on retention span
     */
    private Set<String> determineTablesShouldExist() {
        Set<String> shouldExist = new HashSet<>();
        LocalDateTime now = LocalDateTime.now();
        int retentionDays = metadata.getRetentionSpanDays();
        
        // Tables for past retention days
        LocalDateTime startDate = now.minusDays(retentionDays);
        
        // Tables for future retention days (for data that might be inserted with future dates)
        LocalDateTime endDate = now.plusDays(retentionDays);
        
        LocalDateTime current = startDate.toLocalDate().atStartOfDay();
        while (!current.isAfter(endDate)) {
            String tableName = metadata.getTableName() + "_" + current.format(dateFormatter);
            shouldExist.add(tableName);
            current = current.plusDays(1);
        }
        
        return shouldExist;
    }
    
    /**
     * Query information_schema to find existing tables matching our pattern
     */
    private Set<String> queryExistingTables() throws SQLException {
        Set<String> existingTables = new HashSet<>();
        
        String query = """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ? 
            AND TABLE_NAME LIKE ? 
            AND TABLE_TYPE = 'BASE TABLE'
            """;
        
        String tablePattern = metadata.getTableName() + "_%";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            
            stmt.setString(1, databaseName);
            stmt.setString(2, tablePattern);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    // Validate table name format (should end with YYYYMMDD)
                    if (isValidTableName(tableName)) {
                        existingTables.add(tableName);
                    }
                }
            }
        }
        
        return existingTables;
    }
    
    /**
     * Validate that table name follows expected pattern: prefix_YYYYMMDD
     */
    private boolean isValidTableName(String tableName) {
        String prefix = metadata.getTableName() + "_";
        if (!tableName.startsWith(prefix)) {
            return false;
        }
        
        String datePart = tableName.substring(prefix.length());
        if (datePart.length() != 8) {
            return false;
        }
        
        try {
            // Try to parse the date part to validate format
            LocalDateTime.parse(datePart + "0000", DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Create missing tables
     */
    private void createMissingTables(Set<String> missingTables) throws SQLException {
        if (missingTables.isEmpty()) {
            System.out.println("  ✓ No missing tables to create");
            return;
        }
        
        System.out.println("  Creating " + missingTables.size() + " missing tables:");
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            for (String tableName : missingTables) {
                String createTableSql = metadata.generateCreateTableSql(tableName);
                stmt.execute(createTableSql);
                System.out.println("    ✓ Created: " + tableName);
            }
        }
    }
    
    /**
     * Drop excess tables that shouldn't exist based on retention policy
     */
    private void dropExcessTables(Set<String> excessTables) throws SQLException {
        if (excessTables.isEmpty()) {
            System.out.println("  ✓ No excess tables to drop");
            return;
        }
        
        System.out.println("  Dropping " + excessTables.size() + " excess tables:");
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            for (String tableName : excessTables) {
                String dropTableSql = "DROP TABLE IF EXISTS " + tableName;
                stmt.execute(dropTableSql);
                System.out.println("    ✓ Dropped: " + tableName);
            }
        }
    }
    
    /**
     * Drop logical table if it exists (not needed for multi-table mode)
     */
    private void dropLogicalTableIfExists() throws SQLException {
        String logicalTableName = metadata.getTableName();
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Check if table exists first
            boolean tableExists = false;
            try (ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE '" + logicalTableName + "'")) {
                tableExists = rs.next();
            }
            
            if (tableExists) {
                stmt.execute("DROP TABLE " + logicalTableName);
                System.out.println("  ✓ Dropped logical table (not needed for multi-table mode): " + logicalTableName);
            } else {
                System.out.println("  ✓ Logical table does not exist (multi-table mode): " + logicalTableName);
            }
            
        } catch (SQLException e) {
            System.err.println("  • Warning: Could not drop logical table " + logicalTableName + ": " + e.getMessage());
        }
    }
    
    /**
     * Perform daily maintenance - same logic as startup but with logging
     */
    public void performDailyMaintenance() throws SQLException {
        if (!metadata.isAutoManagePartition()) {
            System.out.println("Auto-management disabled, skipping daily maintenance");
            return;
        }
        
        System.out.println("=== Daily Multi-Table Maintenance ===");
        manageTablesAtStartup();
    }
}
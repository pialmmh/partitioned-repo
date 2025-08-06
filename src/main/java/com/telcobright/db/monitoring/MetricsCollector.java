package com.telcobright.db.monitoring;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/**
 * Collects metrics from database for repository monitoring
 */
public class MetricsCollector {
    
    private final DataSource dataSource;
    private final String databaseName;
    
    public MetricsCollector(DataSource dataSource, String databaseName) {
        this.dataSource = dataSource;
        this.databaseName = databaseName;
    }
    
    /**
     * Count number of partitions for a partitioned table
     */
    public int countPartitions(String tableName) {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.PARTITIONS " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, databaseName);
            stmt.setString(2, tableName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            // Log error and return 0
            System.err.printf("Error counting partitions for table %s: %s%n", tableName, e.getMessage());
        }
        
        return 0;
    }
    
    /**
     * Get list of partition names for a partitioned table
     */
    public List<String> getPartitionNames(String tableName) {
        List<String> partitions = new ArrayList<>();
        String sql = "SELECT PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL " +
                    "ORDER BY PARTITION_NAME";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, databaseName);
            stmt.setString(2, tableName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    partitions.add(rs.getString("PARTITION_NAME"));
                }
            }
        } catch (SQLException e) {
            System.err.printf("Error getting partition names for table %s: %s%n", tableName, e.getMessage());
        }
        
        return partitions;
    }
    
    /**
     * Count number of tables with a specific prefix
     */
    public int countPrefixedTables(String tablePrefix) {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME LIKE ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, databaseName);
            stmt.setString(2, tablePrefix + "%");
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            System.err.printf("Error counting prefixed tables for prefix %s: %s%n", tablePrefix, e.getMessage());
        }
        
        return 0;
    }
    
    /**
     * Get list of table names with a specific prefix
     */
    public List<String> getPrefixedTableNames(String tablePrefix) {
        List<String> tables = new ArrayList<>();
        String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME LIKE ? " +
                    "ORDER BY TABLE_NAME";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, databaseName);
            stmt.setString(2, tablePrefix + "%");
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
        } catch (SQLException e) {
            System.err.printf("Error getting prefixed table names for prefix %s: %s%n", tablePrefix, e.getMessage());
        }
        
        return tables;
    }
    
    /**
     * Get CREATE TABLE statement to analyze partitions
     */
    public String getCreateTableStatement(String tableName) {
        String sql = "SHOW CREATE TABLE " + tableName;
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(2); // Second column contains CREATE TABLE statement
                }
            }
        } catch (SQLException e) {
            System.err.printf("Error getting CREATE TABLE for table %s: %s%n", tableName, e.getMessage());
        }
        
        return null;
    }
    
    /**
     * Check if table exists
     */
    public boolean tableExists(String tableName) {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, databaseName);
            stmt.setString(2, tableName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        } catch (SQLException e) {
            System.err.printf("Error checking if table %s exists: %s%n", tableName, e.getMessage());
        }
        
        return false;
    }
}
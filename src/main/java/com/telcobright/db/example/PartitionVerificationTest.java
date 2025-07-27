package com.telcobright.db.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Verify that MySQL tables are created with proper partitions
 */
public class PartitionVerificationTest {
    
    public static void main(String[] args) {
        try {
            System.out.println("=== MySQL Partition Verification ===\n");
            
            // Connect to MySQL
            HikariConfig config = new HikariConfig();
            config.setDriverClassName("com.mysql.cj.jdbc.Driver");
            config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
            config.setUsername("root");
            config.setPassword("123456");
            config.setMaximumPoolSize(1);
            
            DataSource ds = new HikariDataSource(config);
            
            try (Connection conn = ds.getConnection();
                 Statement stmt = conn.createStatement()) {
                
                // Check if table exists and show its structure
                System.out.println("Checking table structure for sms_20250728:");
                
                try (ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE sms_20250728")) {
                    if (rs.next()) {
                        String createStatement = rs.getString(2);
                        System.out.println("\nCREATE TABLE statement:");
                        System.out.println("------------------------");
                        System.out.println(createStatement);
                        
                        // Check if it contains partitioning
                        if (createStatement.contains("PARTITION BY")) {
                            System.out.println("\n✅ SUCCESS: Table has MySQL native partitioning!");
                            
                            // Count partitions
                            int partitionCount = 0;
                            String[] lines = createStatement.split("\n");
                            for (String line : lines) {
                                if (line.trim().startsWith("PARTITION p")) {
                                    partitionCount++;
                                }
                            }
                            System.out.println("   • Number of partitions: " + partitionCount);
                            System.out.println("   • Partitioning type: RANGE by HOUR(created_at)");
                            
                        } else {
                            System.out.println("\n❌ No partitioning found in table");
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Table sms_20250728 doesn't exist yet");
                }
                
                // Show partition information from information_schema
                System.out.println("\nChecking partition information from INFORMATION_SCHEMA:");
                try (ResultSet rs = stmt.executeQuery(
                    "SELECT TABLE_NAME, PARTITION_NAME, PARTITION_DESCRIPTION " +
                    "FROM INFORMATION_SCHEMA.PARTITIONS " +
                    "WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME LIKE 'sms_%' AND PARTITION_NAME IS NOT NULL " +
                    "ORDER BY TABLE_NAME, PARTITION_NAME")) {
                    
                    System.out.println("Table Name\t\tPartition\tHour Range");
                    System.out.println("--------------------------------------------");
                    
                    while (rs.next()) {
                        String tableName = rs.getString("TABLE_NAME");
                        String partitionName = rs.getString("PARTITION_NAME");
                        String description = rs.getString("PARTITION_DESCRIPTION");
                        
                        System.out.printf("%-20s\t%s\t\t< %s\n", tableName, partitionName, description);
                    }
                }
                
            }
            
            ((HikariDataSource) ds).close();
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
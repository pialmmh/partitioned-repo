package com.telcobright.db.example;

import com.telcobright.db.metadata.EntityMetadata;

/**
 * Test SQL generation for partitioned tables
 */
public class SQLGenerationTest {
    
    public static void main(String[] args) {
        try {
            System.out.println("=== SQL Generation Test ===\n");
            
            // Get metadata for SmsEntity
            EntityMetadata<SmsEntity> metadata = EntityMetadata.of(SmsEntity.class);
            
            // Show the generated CREATE TABLE SQL
            String sql = metadata.generateCreateTableSql("sms_test");
            
            System.out.println("Generated CREATE TABLE SQL:");
            System.out.println("==========================");
            System.out.println(sql);
            
            // Check if it contains partitioning
            if (sql.contains("PARTITION BY")) {
                System.out.println("\n✅ SQL contains partitioning!");
            } else {
                System.out.println("\n❌ SQL does not contain partitioning");
            }
            
            // Show entity metadata
            System.out.println("\nEntity Metadata:");
            System.out.println("- Table name: " + metadata.getTableName());
            System.out.println("- Shard key: " + metadata.getShardKey());
            System.out.println("- Sharding mode: " + metadata.getShardingMode());
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
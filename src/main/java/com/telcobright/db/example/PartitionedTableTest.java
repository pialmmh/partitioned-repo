package com.telcobright.db.example;

import com.telcobright.db.ShardingRepositoryBuilder;
import com.telcobright.db.repository.ShardingRepository;

/**
 * Test partitioned table mode with connection timeout configuration
 */
public class PartitionedTableTest {
    
    public static void main(String[] args) {
        try {
            System.out.println("=== Partitioned Table Mode Test with Connection Timeout ===\n");
            
            System.out.println("Testing partitioned table creation with 60-second timeout:");
            System.out.println("  • Uses OrderEntity with PARTITIONED_TABLE mode");
            System.out.println("  • Creates single logical table with MySQL native partitions");
            System.out.println("  • Connection timeout: 60 seconds for long DDL operations\n");
            
            // Create repository for partitioned table mode
            ShardingRepository<OrderEntity> orderRepo = ShardingRepositoryBuilder
                .partitionedTable()
                .host("127.0.0.1")
                .port(3306)
                .database("test")
                .username("root")
                .password("123456")
                .maxPoolSize(3)
                .charset("utf8mb4")
                .collation("utf8mb4_unicode_ci")
                .connectionTimeout(60000)  // 1 minute for partition operations
                .buildRepository(OrderEntity.class);
            
            System.out.println("✅ Partitioned table repository created successfully!");
            System.out.println("  • Mode: PARTITIONED_TABLE");
            System.out.println("  • Logical table: orders (with MySQL native partitions)");
            System.out.println("  • Connection timeout: 60 seconds");
            System.out.println("  • Primary key: Composite (id, created_at) for MySQL partitioning");
            
        } catch (Exception e) {
            System.err.println("❌ Partitioned table test failed:");
            e.printStackTrace();
        }
    }
}
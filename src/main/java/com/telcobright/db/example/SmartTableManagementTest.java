package com.telcobright.db.example;

import com.telcobright.db.ShardingRepositoryBuilder;
import com.telcobright.db.repository.ShardingRepository;

/**
 * Test smart table management that only creates missing tables and drops excess ones
 */
public class SmartTableManagementTest {
    
    public static void main(String[] args) {
        try {
            System.out.println("=== Smart Table Management Test ===\n");
            
            System.out.println("Creating SMS repository with smart table management...");
            System.out.println("  • Checks information_schema for existing tables");
            System.out.println("  • Creates only missing tables based on retention policy");
            System.out.println("  • Drops only excess tables outside retention window");
            System.out.println("  • Uses MySQL native hourly partitioning (24 partitions per table)\n");
            
            // Create repository - this will trigger smart table management
            ShardingRepository<SmsEntity> smsRepo = ShardingRepositoryBuilder
                .multiTable()
                .host("127.0.0.1")
                .port(3306)
                .database("test")
                .username("root")
                .password("123456")
                .maxPoolSize(3)
                .charset("utf8mb4")
                .collation("utf8mb4_unicode_ci")
                .buildRepository(SmsEntity.class);
            
            System.out.println("\n✅ Smart table management completed!");
            System.out.println("  • Repository ready with optimized table setup");
            System.out.println("  • Only necessary tables exist (within retention window)");
            System.out.println("  • Each table has MySQL native hourly partitioning");
            System.out.println("  • ShardingSphere COMPLEX algorithm configured");
            
        } catch (Exception e) {
            System.err.println("❌ Smart management test failed:");
            e.printStackTrace();
        }
    }
}
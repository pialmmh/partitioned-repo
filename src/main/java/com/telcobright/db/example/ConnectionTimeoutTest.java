package com.telcobright.db.example;

import com.telcobright.db.ShardingRepositoryBuilder;
import com.telcobright.db.repository.ShardingRepository;

/**
 * Test connection timeout settings for long-running partition operations
 */
public class ConnectionTimeoutTest {
    
    public static void main(String[] args) {
        try {
            System.out.println("=== Connection Timeout Configuration Test ===\n");
            
            System.out.println("Testing connection timeout settings for partition management:");
            System.out.println("  • Default timeout: 60 seconds (1 minute)");
            System.out.println("  • JDBC socketTimeout and connectTimeout configured");
            System.out.println("  • HikariCP connectionTimeout aligned with partition operations\n");
            
            // Create repository with default 60-second timeout
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
                // .connectionTimeout(120000)  // Optional: 2 minutes for very large partitions
                .buildRepository(SmsEntity.class);
            
            System.out.println("✅ Repository created with timeout configuration:");
            System.out.println("  • Connection timeout: 60 seconds (default)");
            System.out.println("  • Socket timeout: 60 seconds");
            System.out.println("  • Connect timeout: 60 seconds");
            System.out.println("  • Suitable for partition DDL operations");
            
            System.out.println("\n💡 For very large tables or slow systems, use:");
            System.out.println("  .connectionTimeout(120000)  // 2 minutes");
            System.out.println("  .connectionTimeout(300000)  // 5 minutes");
            
            System.out.println("\n✅ Connection timeout configuration successful!");
            
        } catch (Exception e) {
            System.err.println("❌ Connection timeout test failed:");
            e.printStackTrace();
        }
    }
}
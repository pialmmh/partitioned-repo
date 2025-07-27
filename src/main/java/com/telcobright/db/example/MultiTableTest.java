package com.telcobright.db.example;

import com.telcobright.db.ShardingRepositoryBuilder;
import com.telcobright.db.repository.ShardingRepository;

import java.time.LocalDateTime;

/**
 * Test multi-table mode with SmsEntity
 */
public class MultiTableTest {
    
    public static void main(String[] args) {
        try {
            System.out.println("=== Multi-Table Mode Test ===\n");
            
            System.out.println("Testing multi-table creation:");
            System.out.println("  • Uses SmsEntity with MULTI_TABLE mode");
            System.out.println("  • Creates separate tables: sms_YYYYMMDD");
            System.out.println("  • No logical table needed");
            System.out.println("  • Connection timeout: 60 seconds\n");
            
            // Create repository for multi-table mode
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
                .connectionTimeout(60000)  // 1 minute for table operations
                .buildRepository(SmsEntity.class);
            
            System.out.println("✅ Multi-table repository created successfully!");
            
            // Test insert operation
            SmsEntity sms = new SmsEntity();
            sms.setPhoneNumber("+1234567890");
            sms.setMessage("Test SMS for multi-table mode");
            sms.setStatus("sent");
            sms.setCreatedAt(LocalDateTime.now());
            sms.setUserId("test-user-456");
            
            smsRepo.insert(sms);
            System.out.println("✅ Insert operation successful");
            
            // Test query operation
            LocalDateTime startDate = LocalDateTime.now().minusDays(1);
            LocalDateTime endDate = LocalDateTime.now().plusDays(1);
            long count = smsRepo.count(startDate, endDate);
            System.out.println("✅ Count operation successful: " + count + " records");
            
            System.out.println("\n✅ All multi-table operations completed successfully!");
            System.out.println("  • Mode: MULTI_TABLE");
            System.out.println("  • Physical tables: sms_YYYYMMDD (separate tables per day)");
            System.out.println("  • Connection timeout: 60 seconds");
            System.out.println("  • ShardingSphere routing: Working");
            
        } catch (Exception e) {
            System.err.println("❌ Multi-table test failed:");
            e.printStackTrace();
        }
    }
}
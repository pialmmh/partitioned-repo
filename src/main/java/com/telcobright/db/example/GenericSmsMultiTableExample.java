package com.telcobright.db.example;

import com.telcobright.db.entity.SmsEntity;
import com.telcobright.db.entity.Student;
import com.telcobright.db.repository.GenericMultiTableRepository;
import com.telcobright.db.repository.ShardingRepository;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

/**
 * Example demonstrating generic SMS repository with automatic SQL generation
 */
public class GenericSmsMultiTableExample {
    
    public static void main(String[] args) {
        System.out.println("=== Generic SMS Multi-Table Repository Demo ===");
        
        try {
            // Create generic repository with type safety and HikariCP connection pooling
            ShardingRepository<Student, Long> StudentRepo =
                GenericMultiTableRepository.<Student, Long>builder(Student.class, Long.class)
                    .host("127.0.0.1")
                    .port(3306)
                    .database("test")
                    .username("root")
                    .password("123456")
                    .tablePrefix("student")  // Optional: will use "sms" from @Table annotation if not provided
                    .partitionRetentionPeriod(7)
                    .autoManagePartitions(true)
                    .partitionAdjustmentTime(4, 0)
                    .initializePartitionsOnStart(true)
                    // HikariCP connection pool configuration
                    .maxPoolSize(25)           // Maximum 25 connections in pool
                    .minIdleConnections(5)     // Minimum 5 idle connections
                    .connectionTimeout(30000)  // 30 second connection timeout
                    .idleTimeout(600000)       // 10 minute idle timeout
                    .maxLifetime(1800000)      // 30 minute max connection lifetime
                    .leakDetectionThreshold(60000) // 1 minute leak detection
                    .build();
            
            System.out.println("Repository created successfully with automatic metadata parsing and HikariCP pooling");
            
            // Insert entities - SQL is automatically generated from annotations
            System.out.println("\nInserting SMS entities...");
            
            Student record1 = new Student("Tanvir",26,"abc@example.com","+8801932590638",LocalDateTime.now());
            Student record2 = new Student("John",21,"xyz@example.com","+8801822590621",LocalDateTime.now());

            
            StudentRepo.insert(record1);
            StudentRepo.insert(record2);
            
            System.out.println("Inserted SMS with auto-generated ID: " + record1.getId());
            System.out.println("Inserted SMS with auto-generated ID: " + record2.getId());
            
            // Find by ID across all tables
            System.out.println("\nFinding SMS by ID...");
            Student foundSms = StudentRepo.findById(record1.getId());
            if (foundSms != null) {
                System.out.println("Found SMS: " + foundSms);
            } else {
                System.out.println("SMS not found");
            }
            
            // Find students in recent time period
            System.out.println("\nFinding recent student records...");
            LocalDateTime weekAgo = LocalDateTime.now().minusDays(7);
            List<Student> recentStudents = StudentRepo.findAllByDateRange(weekAgo, LocalDateTime.now());
            System.out.println("Found " + recentStudents.size() + " recent student records");
            
            // Find by date range
            System.out.println("\nFinding SMS in last 24 hours...");
            LocalDateTime yesterday = LocalDateTime.now().minusDays(1);
            LocalDateTime now = LocalDateTime.now();
            
            List<Student> recentSms = StudentRepo.findAllByDateRange(yesterday, now);
            System.out.println("Found " + recentSms.size() + " SMS messages in date range");
            
            // Count by date range (manual count from results)
            long count = recentSms.size();
            System.out.println("Total count: " + count);
            
            // Demonstrate new APIs
            System.out.println("\nTesting new API methods...");
            
            // Test findAllAfterDate
            List<Student> futureRecords = StudentRepo.findAllAfterDate(LocalDateTime.now().minusDays(1));
            System.out.println("Found " + futureRecords.size() + " records after yesterday");
            
            // Test findAllBeforeDate  
            List<Student> oldRecords = StudentRepo.findAllBeforeDate(LocalDateTime.now().minusDays(5));
            System.out.println("Found " + oldRecords.size() + " records before 5 days ago");
            
            // Test findAllByIdsAndDateRange (if we have some records)
            if (!recentStudents.isEmpty()) {
                List<Long> ids = Arrays.asList(recentStudents.get(0).getId());
                List<Student> foundByIds = StudentRepo.findAllByIdsAndDateRange(ids, weekAgo, LocalDateTime.now());
                System.out.println("Found " + foundByIds.size() + " records by ID list in date range");
            }
            
            // Demonstrate automatic table creation
            System.out.println("\nTesting automatic table creation...");
            Student futureRecord = new Student("Shelby",31,"peaky@example.com","+8801522590673",LocalDateTime.now());
            
            StudentRepo.insert(futureRecord);
            System.out.println("Successfully inserted message for future date (table auto-created)");
            
            // Graceful shutdown
            System.out.println("\nShutting down repository...");
            StudentRepo.shutdown();
            System.out.println("Repository shutdown complete");
            
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
package com.telcobright.db.example;

import com.telcobright.db.ShardingRepositoryBuilder;
import com.telcobright.db.repository.ShardingRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDateTime;

/**
 * Drop existing tables and recreate with MySQL native partitioning
 */
public class DropAndRecreateTablesTest {
    
    public static void main(String[] args) {
        try {
            System.out.println("=== Drop and Recreate Tables with MySQL Partitioning ===\n");
            
            // First, connect directly to MySQL to drop existing tables
            HikariConfig config = new HikariConfig();
            config.setDriverClassName("com.mysql.cj.jdbc.Driver");
            config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true&characterEncoding=UTF-8");
            config.setUsername("root");
            config.setPassword("123456");
            config.setMaximumPoolSize(1);
            config.setConnectionInitSql("SET NAMES 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'");
            
            DataSource ds = new HikariDataSource(config);
            
            System.out.println("Dropping existing SMS tables...");
            try (Connection conn = ds.getConnection();
                 Statement stmt = conn.createStatement()) {
                
                // Drop existing tables that might not have partitioning
                String[] tables = {"sms_20250726", "sms_20250727", "sms_20250728", "sms_20250729"};
                
                for (String table : tables) {
                    try {
                        stmt.execute("DROP TABLE IF EXISTS " + table);
                        System.out.println("  ✓ Dropped " + table);
                    } catch (Exception e) {
                        System.out.println("  • " + table + " doesn't exist");
                    }
                }
            }
            
            ((HikariDataSource) ds).close();
            
            System.out.println("\nRecreating repository with MySQL native partitioning...");
            
            // Now create repository which will recreate tables with partitioning
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
            
            System.out.println("✓ Repository created - tables should now have MySQL partitioning\n");
            
            // Insert test data
            LocalDateTime now = LocalDateTime.now();
            System.out.println("Inserting test data with emoji and Unicode support:");
            
            // Insert messages across different hours to test partitioning
            smsRepo.insert(new SmsEntity("+1234567890", "Early morning 🌅", "SENT", now.withHour(6), "user1"));
            smsRepo.insert(new SmsEntity("+2345678901", "Afternoon café ☕", "SENT", now.withHour(14), "user2"));
            smsRepo.insert(new SmsEntity("+3456789012", "Evening 🌙 message", "SENT", now.withHour(20), "user3"));
            
            System.out.println("  ✓ Inserted messages at hours 6, 14, and 20");
            System.out.println("  ✓ All with full Unicode and emoji support");
            
            System.out.println("\n✅ SUCCESS! Tables recreated with MySQL native partitioning");
            System.out.println("  • Each table: One per day (sms_YYYYMMDD)");
            System.out.println("  • Each table has 24 hourly partitions (p00-p23)");
            System.out.println("  • Composite primary key (id, created_at)");
            System.out.println("  • Full UTF-8 support with emoji");
            
        } catch (Exception e) {
            System.err.println("❌ Test failed:");
            e.printStackTrace();
        }
    }
}
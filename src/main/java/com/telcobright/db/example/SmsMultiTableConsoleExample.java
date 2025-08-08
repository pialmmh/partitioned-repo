package com.telcobright.db.example;

import com.telcobright.db.entity.SmsEntity;
import com.telcobright.db.repository.GenericMultiTableRepository;
import com.telcobright.db.monitoring.MonitoringConfig;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

/**
 * Example demonstrating SMS multi-table repository with console monitoring
 * 
 * Features shown:
 * - Multi-table repository with hourly partitioned daily tables
 * - Console monitoring output with formatted metrics
 * - High-volume SMS message handling
 * - Automatic table creation and maintenance
 * - 7-day default retention period
 */
public class SmsMultiTableConsoleExample {
    
    private static SmsEntity createDeliveredSms(String userId, String phoneNumber, String message, String status, 
                                              LocalDateTime createdAt, LocalDateTime deliveredAt, 
                                              BigDecimal cost, String provider) {
        SmsEntity sms = new SmsEntity(userId, phoneNumber, message, status, createdAt, cost, provider);
        sms.setDeliveredAt(deliveredAt);
        return sms;
    }
    
    public static void main(String[] args) {
        System.out.println("=== SMS Multi-Table Repository with Console Monitoring ===\n");
        
        // Configure console monitoring
        MonitoringConfig consoleMonitoring = new MonitoringConfig.Builder()
            .deliveryMethod(MonitoringConfig.DeliveryMethod.CONSOLE_ONLY)
            .metricFormat(MonitoringConfig.MetricFormat.FORMATTED_TEXT)
            .reportingIntervalSeconds(60)  // Report every minute for demo
            .serviceName("sms-service")
            .instanceId("sms-server-01")
            .build();
        
        // Create SMS repository with console monitoring
        GenericMultiTableRepository<SmsEntity, Long> smsRepo = null;
        try {
            smsRepo = GenericMultiTableRepository.<SmsEntity, Long>builder(SmsEntity.class, Long.class)
                .host("127.0.0.1")
                .port(3306)
                .database("messaging")
                .username("root")
                .password("123456")
                .tablePrefix("sms")
                .partitionRetentionPeriod(7)  // 7 days retention (15 tables total)
                .autoManagePartitions(true)   // Enable automatic maintenance
                .partitionAdjustmentTime(4, 0) // Maintenance at 04:00 AM
                .initializePartitionsOnStart(true)
                .monitoring(consoleMonitoring) // Enable console monitoring
                .build();
            
            System.out.println("✅ SMS Repository created successfully");
            System.out.println("   - Table Structure: Daily tables with 24 hourly partitions each");
            System.out.println("   - Total Partitions: 15 tables × 24 hours = 360 partitions");
            System.out.println("   - Monitoring: Console output every 60 seconds\n");
            
            // Insert sample SMS messages
            System.out.println("📤 Inserting SMS messages...");
            
            List<SmsEntity> sampleMessages = Arrays.asList(
                new SmsEntity("user001", "+1234567890", "Welcome to our service!", "SENT", 
                             LocalDateTime.now(), new BigDecimal("0.05"), "twilio"),
                new SmsEntity("user002", "+1234567891", "Your order has been shipped", "SENT", 
                             LocalDateTime.now().minusMinutes(30), new BigDecimal("0.05"), "aws-sns"),
                createDeliveredSms("user003", "+1234567892", "Payment confirmation", "DELIVERED", 
                             LocalDateTime.now().minusHours(2), LocalDateTime.now().minusMinutes(90), 
                             new BigDecimal("0.04"), "messagebird"),
                new SmsEntity("user001", "+1234567890", "Account verification code: 123456", "SENT", 
                             LocalDateTime.now().minusHours(6), new BigDecimal("0.06"), "twilio"),
                new SmsEntity("user004", "+1234567893", "Appointment reminder", "FAILED", 
                             LocalDateTime.now().minusHours(12), new BigDecimal("0.05"), "plivo")
            );
            
            // Insert messages individually to show auto-generation
            for (int i = 0; i < sampleMessages.size(); i++) {
                smsRepo.insert(sampleMessages.get(i));
                System.out.printf("   ✓ Inserted SMS %d with auto-generated ID\n", i + 1);
            }
            
            // Insert batch of messages
            List<SmsEntity> batchMessages = Arrays.asList(
                new SmsEntity("user005", "+1234567894", "Flash sale alert!", "SENT", 
                             LocalDateTime.now().minusDays(1), new BigDecimal("0.03"), "nexmo"),
                createDeliveredSms("user006", "+1234567895", "Password reset link", "DELIVERED", 
                             LocalDateTime.now().minusDays(2), LocalDateTime.now().minusDays(2).plusMinutes(5), 
                             new BigDecimal("0.05"), "twilio")
            );
            
            smsRepo.insertMultiple(batchMessages);
            System.out.println("   ✓ Inserted batch of 2 SMS messages\n");
            
            // Query operations to demonstrate functionality
            System.out.println("🔍 Performing query operations...");
            
            // Find messages from last 24 hours
            List<SmsEntity> recentMessages = smsRepo.findAllByDateRange(
                LocalDateTime.now().minusDays(1), 
                LocalDateTime.now()
            );
            System.out.printf("   Found %d messages in last 24 hours\n", recentMessages.size());
            
            // Find a specific message by ID (demonstrates cross-table search)
            if (!recentMessages.isEmpty()) {
                SmsEntity firstMessage = recentMessages.get(0);
                SmsEntity foundMessage = smsRepo.findById(firstMessage.getId());
                if (foundMessage != null) {
                    System.out.printf("   ✓ Found message by ID: %d (Status: %s)\n", 
                                    foundMessage.getId(), foundMessage.getStatus());
                }
            }
            
            // Update a message status
            if (!recentMessages.isEmpty()) {
                SmsEntity messageToUpdate = recentMessages.get(0);
                SmsEntity updateData = new SmsEntity();
                updateData.setStatus("DELIVERED");
                updateData.setDeliveredAt(LocalDateTime.now());
                
                smsRepo.updateById(messageToUpdate.getId(), updateData);
                System.out.printf("   ✓ Updated message ID %d to DELIVERED status\n", messageToUpdate.getId());
            }
            
            System.out.println("\n⏱️  Waiting for monitoring output (60 seconds)...");
            System.out.println("    Console monitoring will automatically display metrics");
            
            // Keep the application running to see monitoring output
            Thread.sleep(70000); // 70 seconds to see at least one monitoring report
            
        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (smsRepo != null) {
                System.out.println("\n🛑 Shutting down SMS repository...");
                smsRepo.shutdown();
                System.out.println("   ✓ Repository shut down gracefully");
            }
        }
        
        System.out.println("\n=== SMS Multi-Table Example Complete ===");
    }
}
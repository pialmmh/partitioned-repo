package com.telcobright.examples.demo;

import com.telcobright.examples.entity.CitizenEntity;
import com.telcobright.core.repository.GenericPartitionedTableRepository;
import com.telcobright.core.monitoring.MonitoringConfig;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

/**
 * Example demonstrating Citizen entity with extended monitoring capabilities
 * 
 * Features shown:
 * - Single partitioned table for government citizen records
 * - Extended monitoring with custom metrics and alerts
 * - JSON metric format for structured logging
 * - Advanced monitoring scenarios including error tracking
 * - Comprehensive metric collection and reporting
 * - Custom instance identification for multi-server environments
 */
public class CitizenExtendedMonitoringExample {
    
    public static void main(String[] args) {
        System.out.println("=== Citizen Entity with Extended Monitoring Example ===\n");
        
        // Configure extended monitoring with JSON format and comprehensive metrics
        MonitoringConfig extendedMonitoring = new MonitoringConfig.Builder()
            .deliveryMethod(MonitoringConfig.DeliveryMethod.CONSOLE_ONLY)
            .metricFormat(MonitoringConfig.MetricFormat.JSON)  // JSON format for structured logs
            .reportingIntervalSeconds(45)  // More frequent reporting for demo
            .serviceName("gov-citizen-service")
            .instanceId("citizen-db-node-primary")
            .includeSystemMetrics(true)    // Include JVM and system metrics
            .includeConnectionPoolMetrics(true)  // Include database connection metrics
            .includeQueryMetrics(true)     // Include detailed query performance metrics
            .enableSlowQueryLogging(true)  // Log slow queries for optimization
            .slowQueryThresholdMs(100)     // Consider queries over 100ms as slow
            .build();
        
        // Create Citizen repository with extended monitoring
        GenericPartitionedTableRepository<CitizenEntity, Long> citizenRepo = null;
        try {
            citizenRepo = GenericPartitionedTableRepository.<CitizenEntity, Long>builder(CitizenEntity.class, Long.class)
                .host("127.0.0.1")
                .port(3306)
                .database("government")
                .username("root")
                .password("123456")
                .tableName("citizens")
                .partitionRetentionPeriod(7)  // 7 days retention (15 partitions total)
                .autoManagePartitions(true)   // Enable automatic maintenance
                .partitionAdjustmentTime(3, 30) // Maintenance at 03:30 AM
                .initializePartitionsOnStart(true)
                .monitoring(extendedMonitoring) // Enable extended monitoring
                .build();
            
            System.out.println("✅ Citizen Repository created successfully");
            System.out.println("   - Table Structure: Single table with MySQL native partitioning");
            System.out.println("   - Total Partitions: 15 daily partitions (p20250731 to p20250814)");
            System.out.println("   - Partitioning: RANGE partitioning by TO_DAYS(created_at)");
            System.out.println("   - Monitoring: Extended JSON monitoring every 45 seconds");
            System.out.println("   - Metrics: System, Connection Pool, Query Performance, Slow Query Logging\n");
            
            // Insert sample citizen records
            System.out.println("👥 Inserting citizen records...");
            
            List<CitizenEntity> sampleCitizens = Arrays.asList(
                new CitizenEntity("John", "Doe", "1985-03-15", "123-45-6789", 
                                 "john.doe@email.com", "+1-555-0101", "123 Main St, Springfield", 
                                 "ACTIVE", LocalDateTime.now()),
                new CitizenEntity("Jane", "Smith", "1990-07-22", "987-65-4321", 
                                 "jane.smith@email.com", "+1-555-0102", "456 Oak Ave, Springfield", 
                                 "ACTIVE", LocalDateTime.now().minusHours(2)),
                new CitizenEntity("Robert", "Johnson", "1978-11-08", "456-78-9012", 
                                 "robert.j@email.com", "+1-555-0103", "789 Pine Rd, Springfield", 
                                 "SUSPENDED", LocalDateTime.now().minusHours(8)),
                new CitizenEntity("Maria", "Garcia", "1982-05-14", "789-01-2345", 
                                 "maria.garcia@email.com", "+1-555-0104", "321 Elm St, Springfield", 
                                 "ACTIVE", LocalDateTime.now().minusDays(1)),
                new CitizenEntity("David", "Brown", "1975-09-30", "234-56-7890", 
                                 "david.brown@email.com", "+1-555-0105", "654 Birch Ln, Springfield", 
                                 "INACTIVE", LocalDateTime.now().minusDays(2), LocalDateTime.now().minusDays(2).plusHours(1))
            );
            
            // Insert citizens individually to generate individual metrics
            for (int i = 0; i < sampleCitizens.size(); i++) {
                citizenRepo.insert(sampleCitizens.get(i));
                System.out.printf("   ✓ Inserted Citizen %d with auto-generated ID\\n", i + 1);
                
                // Add small delay to spread operations across time for better metrics
                Thread.sleep(200);
            }
            
            // Insert batch of additional citizens from different time periods
            List<CitizenEntity> batchCitizens = Arrays.asList(
                new CitizenEntity("Sarah", "Wilson", "1988-12-03", "567-89-0123", 
                                 "sarah.wilson@email.com", "+1-555-0106", "987 Cedar Ave, Springfield", 
                                 "ACTIVE", LocalDateTime.now().minusDays(3)),
                new CitizenEntity("Michael", "Davis", "1983-04-17", "678-90-1234", 
                                 "michael.davis@email.com", "+1-555-0107", "147 Maple St, Springfield", 
                                 "PENDING", LocalDateTime.now().minusDays(1).minusHours(6)),
                new CitizenEntity("Lisa", "Anderson", "1992-08-25", "890-12-3456", 
                                 "lisa.anderson@email.com", "+1-555-0108", "258 Willow Rd, Springfield", 
                                 "ACTIVE", LocalDateTime.now().minusHours(4))
            );
            
            citizenRepo.insertMultiple(batchCitizens);
            System.out.println("   ✓ Inserted batch of 3 citizens\\n");
            
            // Perform various query operations to generate comprehensive metrics
            System.out.println("🔍 Performing extended query operations for monitoring...");
            
            // 1. Date range queries (demonstrates partition pruning metrics)
            List<CitizenEntity> recentCitizens = citizenRepo.findAllByDateRange(
                LocalDateTime.now().minusDays(2), 
                LocalDateTime.now()
            );
            System.out.printf("   Found %d citizens in last 48 hours\\n", recentCitizens.size());
            
            // 2. Individual ID lookups (demonstrates query performance metrics)
            if (!recentCitizens.isEmpty()) {
                for (int i = 0; i < Math.min(3, recentCitizens.size()); i++) {
                    CitizenEntity citizen = recentCitizens.get(i);
                    CitizenEntity foundCitizen = citizenRepo.findById(citizen.getId());
                    if (foundCitizen != null) {
                        System.out.printf("   ✓ Found citizen by ID: %d (%s %s - %s)\\n", 
                                        foundCitizen.getId(), foundCitizen.getFirstName(), 
                                        foundCitizen.getLastName(), foundCitizen.getStatus());
                    }
                    Thread.sleep(150); // Spread queries for better metrics
                }
            }
            
            // 3. Update operations (demonstrates update performance metrics)
            if (!recentCitizens.isEmpty()) {
                CitizenEntity citizenToUpdate = recentCitizens.stream()
                    .filter(c -> "PENDING".equals(c.getStatus()) || "ACTIVE".equals(c.getStatus()))
                    .findFirst().orElse(recentCitizens.get(0));
                
                CitizenEntity updateData = new CitizenEntity();
                updateData.setStatus("VERIFIED");
                updateData.setUpdatedAt(LocalDateTime.now());
                
                citizenRepo.updateByIdAndDateRange(citizenToUpdate.getId(), updateData, 
                    LocalDateTime.now().minusDays(7), LocalDateTime.now());
                System.out.printf("   ✓ Updated citizen ID %d to VERIFIED status\\n", citizenToUpdate.getId());
            }
            
            // 4. Historical data queries (demonstrates slow query scenarios)
            System.out.println("   Executing historical data query (may trigger slow query logging)...");
            List<CitizenEntity> historicalCitizens = citizenRepo.findAllBeforeDate(
                LocalDateTime.now().minusDays(1)
            );
            System.out.printf("   Found %d historical citizen records\\n", historicalCitizens.size());
            
            // 5. Multiple ID lookups (demonstrates batch operation metrics)
            if (recentCitizens.size() >= 2) {
                List<Long> idsToFind = Arrays.asList(
                    recentCitizens.get(0).getId(),
                    recentCitizens.get(1).getId()
                );
                List<CitizenEntity> foundCitizens = citizenRepo.findAllByIdsAndDateRange(
                    idsToFind, LocalDateTime.now().minusDays(7), LocalDateTime.now()
                );
                System.out.printf("   Found %d citizens by ID list lookup\\n", foundCitizens.size());
            }
            
            // 6. Recent data query (demonstrates current partition access)
            List<CitizenEntity> todaysCitizens = citizenRepo.findAllAfterDate(
                LocalDateTime.now().minusHours(12)
            );
            System.out.printf("   Found %d citizens registered in last 12 hours\\n", todaysCitizens.size());
            
            System.out.println("\\n⏱️  Extended monitoring data collection in progress...");
            System.out.println("    Monitoring features active:");
            System.out.println("    - JSON formatted metrics every 45 seconds");
            System.out.println("    - System resource monitoring (CPU, Memory, Disk)");
            System.out.println("    - Database connection pool metrics");
            System.out.println("    - Query performance tracking");
            System.out.println("    - Slow query logging (threshold: 100ms)");
            System.out.println("    - Partition-specific metrics");
            System.out.println("    - Maintenance operation tracking");
            System.out.println("\\n    Watch console for structured JSON monitoring output...");
            
            // Keep the application running longer to collect comprehensive metrics
            Thread.sleep(95000); // 95 seconds to see at least 2 monitoring reports
            
        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
            
            // Demonstrate error tracking in monitoring
            System.out.println("\\n📊 Error captured in monitoring system");
            System.out.println("    Extended monitoring will include error metrics and stack traces");
            
        } finally {
            if (citizenRepo != null) {
                System.out.println("\\n🛑 Shutting down Citizen repository...");
                citizenRepo.shutdown();
                System.out.println("   ✓ Repository shut down gracefully");
                System.out.println("   ✓ All monitoring services stopped");
                System.out.println("   ✓ Connection pool closed");
                System.out.println("   ✓ Scheduled tasks terminated");
            }
        }
        
        System.out.println("\\n=== Citizen Extended Monitoring Example Complete ===");
        System.out.println("Extended monitoring features demonstrated:");
        System.out.println("- JSON structured logging for integration with log aggregation systems");
        System.out.println("- Comprehensive system and application metrics");
        System.out.println("- Performance monitoring with slow query detection");
        System.out.println("- Error tracking and alerting capabilities");
        System.out.println("- Multi-dimensional metrics for government data compliance");
    }
}
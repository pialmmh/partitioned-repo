package com.telcobright.db.example;

import com.telcobright.db.monitoring.*;

import java.time.LocalDateTime;
import java.util.Arrays;

/**
 * Standalone demo showing console metrics output without requiring database connection
 */
public class MetricsConsoleDemo {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== Console Metrics Demo ===");
        System.out.println("Demonstrating formatted console output (no database required)");
        System.out.println();

        demonstrateSimplifiedMetricsOutput();
    }
    
    private static void demonstrateSimplifiedMetricsOutput() throws InterruptedException {
        // Create monitoring config
        MonitoringConfig config = new MonitoringConfig.Builder()
                .deliveryMethod(MonitoringConfig.DeliveryMethod.SLF4J_LOGGING)
                .metricFormat(MonitoringConfig.MetricFormat.JSON)
                .reportingIntervalSeconds(3) // Very fast for demo
                .serviceName("demo-service")
                .instanceId("demo-instance-001")
                .build();
        
        // Create metrics and populate with demo data
        RepositoryMetrics metrics = new RepositoryMetrics("MultiTable", "order_events", "demo-host");
        
        // Simulate some realistic data
        metrics.setLastRunTimestamp(LocalDateTime.now().minusMinutes(2));
        metrics.setNextScheduledTime(LocalDateTime.now().plusHours(1));
        metrics.setLastJobDurationMs(1250);
        
        // Partition data
        metrics.setNumberOfPartitions(15);
        metrics.setNumberOfPrefixedTables(45);
        metrics.setPartitionsCreatedLastRun(2);
        metrics.setPartitionsDeletedLastRun(1);
        metrics.setTotalListOfPartitions(Arrays.asList(
            "order_events_20250801", "order_events_20250802", "order_events_20250803",
            "order_events_20250804", "order_events_20250805", "order_events_20250806",
            "order_events_20250807", "order_events_20250808"
        ));
        metrics.setNextPartitionsToDelete(Arrays.asList("order_events_20250715", "order_events_20250716"));
        
        // Success scenario
        metrics.setLastMaintenanceJobSucceeded(true);
        
        // Create monitoring service with null collector (demo mode)
        DefaultMonitoringService monitoringService = 
            new DefaultMonitoringService(config, metrics, null);
        
        System.out.println("1. ✅ HEALTHY Repository Metrics:");
        // Use reflection to call printSimplifiedMetrics for demo
        printSimplifiedMetricsDemo(monitoringService, config, metrics);
        
        System.out.println();
        Thread.sleep(2000);
        
        // Now simulate failure scenario
        System.out.println("2. ❌ FAILED Repository Metrics:");
        metrics.setLastMaintenanceJobSucceeded(false);
        metrics.setLastJobDurationMs(5500); // Longer duration for failure
        metrics.setPartitionsCreatedLastRun(0);
        metrics.setPartitionsDeletedLastRun(0);
        
        printSimplifiedMetricsDemo(monitoringService, config, metrics);
        
        System.out.println();
        Thread.sleep(2000);
        
        // Different repository type
        System.out.println("3. 📊 Partitioned Repository Metrics:");
        RepositoryMetrics partitionedMetrics = new RepositoryMetrics("Partitioned", "user_activity", "prod-server-02");
        partitionedMetrics.setLastRunTimestamp(LocalDateTime.now().minusMinutes(5));
        partitionedMetrics.setLastJobDurationMs(890);
        partitionedMetrics.setNumberOfPartitions(8);
        partitionedMetrics.setNumberOfPrefixedTables(8); // Same for partitioned tables
        partitionedMetrics.setPartitionsCreatedLastRun(1);
        partitionedMetrics.setPartitionsDeletedLastRun(0);
        partitionedMetrics.setTotalListOfPartitions(Arrays.asList(
            "p_202508_01", "p_202508_02", "p_202508_03", "p_202508_04"
        ));
        partitionedMetrics.setLastMaintenanceJobSucceeded(true);
        
        MonitoringConfig prometheusConfig = new MonitoringConfig.Builder()
                .deliveryMethod(MonitoringConfig.DeliveryMethod.HTTP_ENDPOINT)
                .metricFormat(MonitoringConfig.MetricFormat.PROMETHEUS)
                .httpEndpointPath("/metrics")
                .reportingIntervalSeconds(30)
                .serviceName("user-service")
                .instanceId("k8s-pod-xyz789")
                .build();
        
        printSimplifiedMetricsDemo(null, prometheusConfig, partitionedMetrics);
        
        System.out.println("✅ Console metrics demo completed!");
        System.out.println();
        System.out.println("In production, these metrics would appear:");
        System.out.println("  - Every reporting interval (e.g., 60 seconds)");
        System.out.println("  - PLUS the configured delivery method (HTTP/REST/Kafka)");
        System.out.println("  - Automatically when maintenance jobs run");
    }
    
    /**
     * Demo version of the console metrics printing
     */
    private static void printSimplifiedMetricsDemo(DefaultMonitoringService service, 
                                                   MonitoringConfig config, 
                                                   RepositoryMetrics metrics) {
        System.out.println();
        System.out.println("========================================");
        System.out.printf(" REPOSITORY METRICS - %s ", LocalDateTime.now().toString());
        System.out.println();
        System.out.println("========================================");
        
        // Service Info
        System.out.printf("Service: %s (%s)%n", config.getServiceName(), config.getInstanceId());
        System.out.printf("Repository: %s [%s]%n", metrics.getRepositoryName(), metrics.getRepositoryType());
        System.out.printf("Hostname: %s%n", metrics.getHostname());
        System.out.println();
        
        // Scheduler Status
        System.out.println("📅 SCHEDULER STATUS:");
        if (metrics.getLastRunTimestamp() != null) {
            System.out.printf("   Last Run: %s%n", metrics.getLastRunTimestamp().toString());
            System.out.printf("   Duration: %d ms%n", metrics.getLastJobDurationMs());
        } else {
            System.out.println("   Last Run: Never");
        }
        
        if (metrics.getNextScheduledTime() != null) {
            System.out.printf("   Next Run: %s%n", metrics.getNextScheduledTime().toString());
        }
        System.out.println();
        
        // Partition Metrics
        System.out.println("📊 PARTITION METRICS:");
        System.out.printf("   Total Partitions: %d%n", metrics.getNumberOfPartitions());
        System.out.printf("   Prefixed Tables: %d%n", metrics.getNumberOfPrefixedTables());
        System.out.printf("   Created Last Run: %d%n", metrics.getPartitionsCreatedLastRun());
        System.out.printf("   Deleted Last Run: %d%n", metrics.getPartitionsDeletedLastRun());
        
        if (metrics.getTotalListOfPartitions() != null && !metrics.getTotalListOfPartitions().isEmpty()) {
            System.out.printf("   Active Partitions: %s%n", 
                            metrics.getTotalListOfPartitions().size() > 5 
                                ? metrics.getTotalListOfPartitions().subList(0, 5) + "... (+" + (metrics.getTotalListOfPartitions().size() - 5) + " more)"
                                : metrics.getTotalListOfPartitions());
        }
        System.out.println();
        
        // Health Status
        System.out.println("🏥 HEALTH STATUS:");
        String healthStatus = metrics.isLastMaintenanceJobSucceeded() ? "✅ HEALTHY" : "❌ FAILED";
        System.out.printf("   Last Job: %s%n", healthStatus);
        System.out.printf("   Total Failures: %d%n", metrics.getTotalFailureCount());
        
        if (metrics.getNextPartitionsToDelete() != null && !metrics.getNextPartitionsToDelete().isEmpty()) {
            System.out.printf("   Next Cleanup: %d partitions%n", metrics.getNextPartitionsToDelete().size());
        }
        System.out.println();
        
        // Delivery Method
        System.out.println("📡 MONITORING CONFIG:");
        System.out.printf("   Delivery: %s%n", config.getDeliveryMethod().name());
        System.out.printf("   Format: %s%n", config.getMetricFormat().name());
        System.out.printf("   Interval: %d seconds%n", config.getReportingIntervalSeconds());
        
        switch (config.getDeliveryMethod()) {
            case HTTP_ENDPOINT:
                System.out.printf("   Endpoint: %s%n", config.getHttpEndpointPath());
                break;
            case REST_PUSH:
                System.out.printf("   Push URL: %s%n", config.getRestPushUrl());
                break;
            case KAFKA_PUSH:
                System.out.printf("   Kafka Topic: %s%n", config.getKafkaTopicName());
                break;
        }
        
        System.out.println("========================================");
        System.out.println();
    }
}
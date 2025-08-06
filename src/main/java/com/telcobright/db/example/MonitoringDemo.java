package com.telcobright.db.example;

import com.telcobright.db.annotation.*;
import com.telcobright.db.entity.ShardingEntity;
import com.telcobright.db.monitoring.*;
import com.telcobright.db.repository.GenericMultiTableRepository;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

/**
 * Comprehensive monitoring demonstration for partitioned-repo
 * Shows different monitoring delivery methods as specified in CLAUDE-MONITORING.md
 */
public class MonitoringDemo {

    @Table(name = "monitoring_test")
    public static class TestEntity implements ShardingEntity<Long> {
        @Id
        @Column(name = "test_id", insertable = false)
        private Long testId;
        
        @ShardingKey
        @Column(name = "created_at", nullable = false)
        private LocalDateTime createdAt;
        
        @Index
        @Column(name = "status", nullable = false)
        private String status;
        
        @Column(name = "message")
        private String message;
        
        public TestEntity() {}
        
        public TestEntity(String status, String message) {
            this.status = status;
            this.message = message;
            this.createdAt = LocalDateTime.now();
        }
        
        // Getters and setters
        public Long getTestId() { return testId; }
        public void setTestId(Long testId) { this.testId = testId; }
        
        public LocalDateTime getCreatedAt() { return createdAt; }
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
        
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== Repository Monitoring Demo ===");
        System.out.println("Implementing CLAUDE-MONITORING.md requirements");
        System.out.println();

        demonstrateMonitoringMethods();
        
        System.out.println("✅ Monitoring demonstration completed!");
    }
    
    private static void demonstrateMonitoringMethods() throws InterruptedException {
        System.out.println("1. SLF4J Logging Monitoring:");
        demonstrateSLF4JLogging();
        
        System.out.println("\n2. Prometheus HTTP Endpoint Monitoring:");
        demonstratePrometheusHTTP();
        
        System.out.println("\n3. REST Push Monitoring:");
        demonstrateRESTpush();
        
        System.out.println("\n4. Disabled Monitoring:");
        demonstrateDisabledMonitoring();
    }
    
    private static void demonstrateSLF4JLogging() throws InterruptedException {
        System.out.println("   Creating repository with SLF4J logging monitoring...");
        
        MonitoringConfig config = new MonitoringConfig.Builder()
                .deliveryMethod(MonitoringConfig.DeliveryMethod.SLF4J_LOGGING)
                .metricFormat(MonitoringConfig.MetricFormat.JSON)
                .reportingIntervalSeconds(5) // 5 seconds for demo
                .serviceName("test-service")
                .build();
        
        // Create repository with monitoring (would normally connect to real DB)
        try {
            GenericMultiTableRepository<TestEntity, Long> repository = 
                GenericMultiTableRepository.<TestEntity, Long>builder(TestEntity.class, Long.class)
                    .host("127.0.0.1")
                    .database("test")
                    .username("root")
                    .password("123456") 
                    .monitoring(config)
                    .autoManagePartitions(false) // Disable for demo
                    .initializePartitionsOnStart(false) // Disable for demo
                    .build();
            
            MonitoringService monitoring = repository.getMonitoringService();
            if (monitoring != null) {
                System.out.println("   ✅ SLF4J monitoring enabled");
                
                // Simulate some maintenance activity
                monitoring.recordMaintenanceJobStart();
                Thread.sleep(100); // Simulate work
                monitoring.recordMaintenanceJobSuccess(100, 2, 1);
                
                // Show JSON metrics
                System.out.println("   JSON metrics example:");
                String jsonMetrics = monitoring.getJsonMetrics();
                System.out.println("   " + jsonMetrics.substring(0, Math.min(200, jsonMetrics.length())) + "...");
                
                // Wait a bit to see logging output
                System.out.println("   Waiting for next logging cycle...");
                Thread.sleep(6000);
            }
            
            repository.shutdown();
            System.out.println("   ✅ Repository shut down");
            
        } catch (Exception e) {
            System.out.printf("   ⚠️ Expected demo error (no real DB): %s%n", e.getMessage());
        }
    }
    
    private static void demonstratePrometheusHTTP() throws InterruptedException {
        System.out.println("   Creating repository with Prometheus HTTP monitoring...");
        
        MonitoringConfig config = new MonitoringConfig.Builder()
                .deliveryMethod(MonitoringConfig.DeliveryMethod.HTTP_ENDPOINT)
                .metricFormat(MonitoringConfig.MetricFormat.PROMETHEUS)
                .httpEndpointPath("/metrics")
                .reportingIntervalSeconds(30)
                .serviceName("prometheus-service")
                .build();
        
        try {
            GenericMultiTableRepository<TestEntity, Long> repository = 
                GenericMultiTableRepository.<TestEntity, Long>builder(TestEntity.class, Long.class)
                    .host("127.0.0.1")
                    .database("test")
                    .username("root")
                    .password("123456")
                    .monitoring(config)
                    .autoManagePartitions(false)
                    .initializePartitionsOnStart(false)
                    .build();
            
            MonitoringService monitoring = repository.getMonitoringService();
            if (monitoring != null) {
                System.out.println("   ✅ Prometheus HTTP monitoring enabled");
                
                // Start HTTP metrics server
                SimpleHttpMetricsServer server = SimpleHttpMetricsServer.startFor(monitoring);
                
                // Simulate activity
                monitoring.recordMaintenanceJobStart();
                monitoring.recordMaintenanceJobSuccess(250, 3, 0);
                
                // Show Prometheus metrics
                System.out.println("   Prometheus metrics sample:");
                String prometheusMetrics = monitoring.getPrometheusMetrics();
                String[] lines = prometheusMetrics.split("\\n");
                for (int i = 0; i < Math.min(10, lines.length); i++) {
                    System.out.println("   " + lines[i]);
                }
                System.out.println("   ... (truncated)");
                
                System.out.println("   HTTP server running - check http://localhost:8080/metrics");
                Thread.sleep(3000);
                
                server.stop();
                System.out.println("   ✅ HTTP server stopped");
            }
            
            repository.shutdown();
            
        } catch (Exception e) {
            System.out.printf("   ⚠️ Expected demo error (no real DB): %s%n", e.getMessage());
        }
    }
    
    private static void demonstrateRESTpush() {
        System.out.println("   Creating repository with REST push monitoring...");
        
        MonitoringConfig config = new MonitoringConfig.Builder()
                .deliveryMethod(MonitoringConfig.DeliveryMethod.REST_PUSH)
                .metricFormat(MonitoringConfig.MetricFormat.JSON)
                .restPushUrl("http://localhost:9090/api/metrics")
                .reportingIntervalSeconds(10)
                .serviceName("rest-push-service")
                .build();
        
        try {
            GenericMultiTableRepository<TestEntity, Long> repository = 
                GenericMultiTableRepository.<TestEntity, Long>builder(TestEntity.class, Long.class)
                    .host("127.0.0.1")
                    .database("test")
                    .username("root")
                    .password("123456")
                    .monitoring(config)
                    .autoManagePartitions(false)
                    .initializePartitionsOnStart(false)
                    .build();
            
            MonitoringService monitoring = repository.getMonitoringService();
            if (monitoring != null) {
                System.out.println("   ✅ REST push monitoring enabled");
                System.out.printf("   Configured to push to: %s every %d seconds%n", 
                                config.getRestPushUrl(), config.getReportingIntervalSeconds());
                
                // Note: In real usage, this would push to the configured endpoint
                System.out.println("   (Would push JSON metrics to REST endpoint periodically)");
            }
            
            repository.shutdown();
            
        } catch (Exception e) {
            System.out.printf("   ⚠️ Expected demo error (no real DB): %s%n", e.getMessage());
        }
    }
    
    private static void demonstrateDisabledMonitoring() {
        System.out.println("   Creating repository with monitoring disabled...");
        
        try {
            GenericMultiTableRepository<TestEntity, Long> repository = 
                GenericMultiTableRepository.<TestEntity, Long>builder(TestEntity.class, Long.class)
                    .host("127.0.0.1")
                    .database("test")
                    .username("root")
                    .password("123456")
                    // No .monitoring() call - monitoring disabled by default
                    .autoManagePartitions(false)
                    .initializePartitionsOnStart(false)
                    .build();
            
            MonitoringService monitoring = repository.getMonitoringService();
            if (monitoring == null) {
                System.out.println("   ✅ Monitoring disabled - no overhead");
            } else {
                System.out.println("   ❌ Unexpected: monitoring should be disabled");
            }
            
            repository.shutdown();
            
        } catch (Exception e) {
            System.out.printf("   ⚠️ Expected demo error (no real DB): %s%n", e.getMessage());
        }
    }
}
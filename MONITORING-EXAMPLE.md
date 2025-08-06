# Monitoring System Example Configuration

## Database Setup

For testing the monitoring system, use these database credentials:

```
Host: 127.0.0.1
Database: test
Username: root
Password: 123456
```

## Quick Start with Monitoring

### 1. Basic Monitoring Setup

```java
import com.telcobright.db.monitoring.*;
import com.telcobright.db.repository.*;

// Configure monitoring
MonitoringConfig config = new MonitoringConfig.Builder()
    .deliveryMethod(MonitoringConfig.DeliveryMethod.SLF4J_LOGGING)
    .metricFormat(MonitoringConfig.MetricFormat.JSON)
    .reportingIntervalSeconds(60)  // Report every minute
    .serviceName("my-service")
    .build();

// Create repository with monitoring
GenericMultiTableRepository<Entity, Long> repository = 
    GenericMultiTableRepository.<Entity, Long>builder(Entity.class, Long.class)
        .host("127.0.0.1")
        .database("test")
        .username("root")
        .password("123456")
        .monitoring(config)  // Enable monitoring
        .build();
```

### 2. Prometheus HTTP Endpoint

```java
// Prometheus metrics endpoint
MonitoringConfig config = new MonitoringConfig.Builder()
    .deliveryMethod(MonitoringConfig.DeliveryMethod.HTTP_ENDPOINT)
    .metricFormat(MonitoringConfig.MetricFormat.PROMETHEUS)
    .httpEndpointPath("/metrics")
    .reportingIntervalSeconds(30)
    .serviceName("my-service")
    .build();

// Create repository
GenericPartitionedTableRepository<Entity, Long> repository = 
    GenericPartitionedTableRepository.<Entity, Long>builder(Entity.class, Long.class)
        .host("127.0.0.1")
        .database("test")
        .username("root")
        .password("123456")
        .monitoring(config)
        .build();

// Start HTTP server for metrics
SimpleHttpMetricsServer server = SimpleHttpMetricsServer.startFor(
    repository.getMonitoringService());

// Metrics available at http://localhost:8080/metrics
```

### 3. REST API Push

```java
// Push metrics to external monitoring system
MonitoringConfig config = new MonitoringConfig.Builder()
    .deliveryMethod(MonitoringConfig.DeliveryMethod.REST_PUSH)
    .metricFormat(MonitoringConfig.MetricFormat.JSON)
    .restPushUrl("http://monitoring-server:9090/api/metrics")
    .reportingIntervalSeconds(60)
    .serviceName("my-service")
    .build();

GenericMultiTableRepository<Entity, Long> repository = 
    GenericMultiTableRepository.<Entity, Long>builder(Entity.class, Long.class)
        .host("127.0.0.1")
        .database("test")
        .username("root")
        .password("123456")
        .monitoring(config)
        .build();
```

## Console Output Example

Every reporting interval, you'll see formatted metrics in the console:

```
========================================
 REPOSITORY METRICS - 2025-08-07T10:30:45
========================================
Service: my-service (hostname)
Repository: order_events [MultiTable]
Hostname: prod-server-01

📅 SCHEDULER STATUS:
   Last Run: 2025-08-07T10:25:45
   Duration: 1250 ms
   Next Run: 2025-08-07T10:35:45

📊 PARTITION METRICS:
   Total Partitions: 15
   Prefixed Tables: 45
   Created Last Run: 2
   Deleted Last Run: 1
   Active Partitions: [order_events_20250801, ...]

🏥 HEALTH STATUS:
   Last Job: ✅ HEALTHY
   Total Failures: 0
   Next Cleanup: 2 partitions

📡 MONITORING CONFIG:
   Delivery: SLF4J_LOGGING
   Format: JSON
   Interval: 60 seconds
========================================
```

## Running the Demo

To see monitoring in action with the test database:

```bash
# Compile the project
mvn clean compile

# Run the monitoring demo
mvn exec:java -Dexec.mainClass="com.telcobright.db.example.MonitoringDemo"

# Or run the simplified console demo (no DB required)
mvn exec:java -Dexec.mainClass="com.telcobright.db.example.MetricsConsoleDemo"
```

## Features

- **Always-On Console Metrics**: Formatted metrics printed every reporting cycle
- **Multiple Delivery Methods**: HTTP, REST, Kafka, SLF4J
- **Comprehensive Metrics**: Scheduler status, partition counts, health status
- **Low Overhead**: Runs in background threads
- **Framework Agnostic**: Works with any Java application

## Production Configuration

For production use:

```java
MonitoringConfig config = new MonitoringConfig.Builder()
    .deliveryMethod(MonitoringConfig.DeliveryMethod.HTTP_ENDPOINT)
    .metricFormat(MonitoringConfig.MetricFormat.PROMETHEUS)
    .httpEndpointPath("/metrics")
    .reportingIntervalSeconds(60)  // Every minute
    .serviceName("production-service")
    .instanceId(System.getenv("HOSTNAME"))  // Use container/pod name
    .build();
```

## Disabling Monitoring

To run without monitoring overhead:

```java
// Simply don't set monitoring config
GenericMultiTableRepository<Entity, Long> repository = 
    GenericMultiTableRepository.<Entity, Long>builder(Entity.class, Long.class)
        .host("127.0.0.1")
        .database("test")
        .username("root")
        .password("123456")
        // No .monitoring() call
        .build();
```
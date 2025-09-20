# Monitoring & Maintenance

## Overview

Split-Verse provides comprehensive monitoring and automated maintenance capabilities to ensure optimal performance and data lifecycle management. The system includes metrics collection, health checks, partition management, and performance optimization features.

## Monitoring Architecture

```
┌────────────────────────────────┐
│   Application Metrics          │
└────────────┬───────────────────┘
             │
┌────────────▼───────────────────┐
│   MonitoringService            │
│   - Start/Stop Control         │
│   - Metrics Aggregation        │
└────────────┬───────────────────┘
             │
    ┌────────┴────────┐
    ▼                 ▼
┌─────────┐    ┌──────────────┐
│ Metrics │    │ Metrics      │
│Collector│    │ HTTP Server  │
└─────────┘    └──────────────┘
    │                 │
┌───▼─────────────────▼──┐
│  RepositoryMetrics     │
│  - Counters            │
│  - Timers              │
│  - Gauges              │
└────────────────────────┘
```

## Metrics Collection

### 1. **Repository Metrics**

```java
public class RepositoryMetrics {
    // Operation counters
    private final AtomicLong insertCount = new AtomicLong();
    private final AtomicLong selectCount = new AtomicLong();
    private final AtomicLong updateCount = new AtomicLong();
    private final AtomicLong deleteCount = new AtomicLong();

    // Performance timers
    private final Timer insertTimer = new Timer();
    private final Timer queryTimer = new Timer();

    // Resource gauges
    private final AtomicLong activeConnections = new AtomicLong();
    private final AtomicLong tableCount = new AtomicLong();
    private final AtomicLong totalDataSize = new AtomicLong();

    // Error tracking
    private final AtomicLong errorCount = new AtomicLong();
    private final Map<String, AtomicLong> errorsByType = new ConcurrentHashMap<>();
}
```

### 2. **Metrics Collection Points**

**Insert Operations:**
```java
public void insert(T entity) throws SQLException {
    long startTime = System.currentTimeMillis();

    try {
        // Actual insert operation
        doInsert(entity);

        // Record success metrics
        metrics.recordInsert(System.currentTimeMillis() - startTime);
    } catch (SQLException e) {
        // Record error metrics
        metrics.recordError("INSERT", e);
        throw e;
    }
}
```

**Query Operations:**
```java
public List<T> findByDateRange(LocalDateTime start, LocalDateTime end) {
    return metrics.recordQuery(() -> {
        // Actual query operation
        return doFindByDateRange(start, end);
    });
}
```

### 3. **Metrics Aggregation**

```java
public class MetricsAggregator {
    public MetricsSummary aggregate(Duration window) {
        return new MetricsSummary()
            .withTotalOperations(getTotalOperations())
            .withAverageLatency(calculateAverageLatency(window))
            .withThroughput(calculateThroughput(window))
            .withErrorRate(calculateErrorRate(window))
            .withP99Latency(calculatePercentile(99, window));
    }

    private double calculateThroughput(Duration window) {
        long operations = insertCount.get() + selectCount.get() +
                         updateCount.get() + deleteCount.get();
        return operations / window.getSeconds();
    }
}
```

## Health Monitoring

### 1. **Health Checks**

```java
public class HealthCheck {
    public HealthStatus check() {
        HealthStatus status = new HealthStatus();

        // Database connectivity
        status.addCheck("database", checkDatabaseConnection());

        // Shard availability
        for (ShardConfig shard : shards) {
            status.addCheck("shard_" + shard.getId(), checkShard(shard));
        }

        // Table count
        status.addCheck("table_count", checkTableCount());

        // Disk space
        status.addCheck("disk_space", checkDiskSpace());

        // Query performance
        status.addCheck("query_performance", checkQueryPerformance());

        return status;
    }

    private CheckResult checkDatabaseConnection() {
        try (Connection conn = connectionProvider.getConnection()) {
            conn.createStatement().execute("SELECT 1");
            return CheckResult.healthy("Database connection OK");
        } catch (Exception e) {
            return CheckResult.unhealthy("Database connection failed: " + e);
        }
    }

    private CheckResult checkTableCount() {
        int count = getTableCount();
        if (count > MAX_TABLES_WARNING) {
            return CheckResult.warning("High table count: " + count);
        }
        return CheckResult.healthy("Table count normal: " + count);
    }
}
```

### 2. **Performance Monitoring**

```java
public class PerformanceMonitor {
    private final RollingWindow insertLatencies = new RollingWindow(1000);
    private final RollingWindow queryLatencies = new RollingWindow(1000);

    public void recordInsertLatency(long millis) {
        insertLatencies.add(millis);

        // Alert on high latency
        if (millis > LATENCY_THRESHOLD_MS) {
            alertManager.sendAlert(new LatencyAlert(
                "High insert latency", millis
            ));
        }
    }

    public PerformanceStats getStats() {
        return new PerformanceStats()
            .withAvgInsertLatency(insertLatencies.average())
            .withP95InsertLatency(insertLatencies.percentile(95))
            .withP99InsertLatency(insertLatencies.percentile(99))
            .withAvgQueryLatency(queryLatencies.average())
            .withP95QueryLatency(queryLatencies.percentile(95))
            .withP99QueryLatency(queryLatencies.percentile(99));
    }
}
```

## Automatic Partition Management

### 1. **Partition Lifecycle**

```java
public class PartitionManager {
    private final ScheduledExecutorService scheduler;
    private final int retentionDays;
    private final LocalTime maintenanceTime;

    public void initialize() {
        // Schedule daily maintenance
        scheduler.scheduleAtFixedRate(
            this::performMaintenance,
            getInitialDelay(),
            24, TimeUnit.HOURS
        );
    }

    private void performMaintenance() {
        try {
            // Create future partitions
            createFuturePartitions();

            // Drop old partitions
            dropExpiredPartitions();

            // Optimize existing partitions
            optimizePartitions();

            // Update statistics
            updateStatistics();
        } catch (Exception e) {
            logger.error("Maintenance failed", e);
            alertManager.sendAlert(new MaintenanceAlert(e));
        }
    }
}
```

### 2. **Partition Creation**

```java
private void createFuturePartitions() throws SQLException {
    LocalDateTime now = LocalDateTime.now();
    LocalDateTime futureDate = now.plusDays(lookaheadDays);

    List<String> requiredTables = getRequiredTables(now, futureDate);
    List<String> existingTables = getExistingTables();

    for (String tableName : requiredTables) {
        if (!existingTables.contains(tableName)) {
            createTable(tableName);
            logger.info("Created future partition: " + tableName);
        }
    }
}
```

### 3. **Partition Deletion**

```java
private void dropExpiredPartitions() throws SQLException {
    LocalDateTime cutoffDate = LocalDateTime.now().minusDays(retentionDays);
    List<String> tables = getExistingTables();

    for (String table : tables) {
        LocalDateTime tableDate = extractDateFromTableName(table);

        if (tableDate != null && tableDate.isBefore(cutoffDate)) {
            // Optional: Archive before deletion
            if (archiveEnabled) {
                archiveTable(table);
            }

            // Drop the table
            dropTable(table);
            logger.info("Dropped expired partition: " + table);
        }
    }
}
```

### 4. **Partition Optimization**

```java
private void optimizePartitions() throws SQLException {
    List<String> recentTables = getRecentTables(7); // Last 7 days

    for (String table : recentTables) {
        TableStats stats = getTableStats(table);

        // Analyze table if needed
        if (stats.getRowCount() > ANALYZE_THRESHOLD) {
            analyzeTable(table);
        }

        // Optimize fragmented tables
        if (stats.getFragmentation() > FRAGMENTATION_THRESHOLD) {
            optimizeTable(table);
        }

        // Rebuild indexes if needed
        if (stats.getIndexFragmentation() > INDEX_FRAGMENTATION_THRESHOLD) {
            rebuildIndexes(table);
        }
    }
}
```

## Monitoring HTTP Server

### 1. **Metrics Endpoint**

```java
public class SimpleHttpMetricsServer {
    private final HttpServer server;
    private final RepositoryMetrics metrics;

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/metrics", exchange -> {
            String response = formatMetrics();
            exchange.sendResponseHeaders(200, response.length());
            exchange.getResponseBody().write(response.getBytes());
            exchange.close();
        });

        server.createContext("/health", exchange -> {
            HealthStatus health = healthCheck.check();
            String response = formatHealth(health);
            int code = health.isHealthy() ? 200 : 503;
            exchange.sendResponseHeaders(code, response.length());
            exchange.getResponseBody().write(response.getBytes());
            exchange.close();
        });

        server.start();
    }

    private String formatMetrics() {
        // Prometheus format
        return String.format(
            "# HELP splitverse_inserts_total Total number of inserts\n" +
            "# TYPE splitverse_inserts_total counter\n" +
            "splitverse_inserts_total{repository=\"%s\"} %d\n" +
            "\n" +
            "# HELP splitverse_query_duration_seconds Query duration\n" +
            "# TYPE splitverse_query_duration_seconds histogram\n" +
            "splitverse_query_duration_seconds_sum %f\n" +
            "splitverse_query_duration_seconds_count %d\n",
            repositoryName, metrics.getInsertCount(),
            metrics.getQueryDurationSum(), metrics.getQueryCount()
        );
    }
}
```

### 2. **Dashboard Integration**

**Grafana Dashboard JSON:**
```json
{
  "dashboard": {
    "title": "Split-Verse Monitoring",
    "panels": [
      {
        "title": "Operations per Second",
        "targets": [
          {
            "expr": "rate(splitverse_inserts_total[1m])"
          }
        ]
      },
      {
        "title": "Query Latency",
        "targets": [
          {
            "expr": "histogram_quantile(0.99, splitverse_query_duration_seconds)"
          }
        ]
      }
    ]
  }
}
```

## Maintenance Tasks

### 1. **Scheduled Maintenance**

```java
public class MaintenanceScheduler {
    private final ScheduledExecutorService executor =
        Executors.newScheduledThreadPool(2);

    public void scheduleTasks() {
        // Daily partition management
        executor.scheduleAtFixedRate(
            this::managePartitions,
            getNextMidnight(),
            24, TimeUnit.HOURS
        );

        // Hourly statistics update
        executor.scheduleAtFixedRate(
            this::updateStatistics,
            0, 1, TimeUnit.HOURS
        );

        // Weekly optimization
        executor.scheduleAtFixedRate(
            this::optimizeTables,
            getNextSunday(),
            7, TimeUnit.DAYS
        );
    }
}
```

### 2. **On-Demand Maintenance**

```java
public class MaintenanceAPI {
    public void triggerMaintenance(MaintenanceType type) {
        switch (type) {
            case CREATE_PARTITIONS:
                createMissingPartitions();
                break;
            case DROP_OLD_PARTITIONS:
                dropOldPartitions();
                break;
            case OPTIMIZE_TABLES:
                optimizeAllTables();
                break;
            case REBUILD_INDEXES:
                rebuildAllIndexes();
                break;
            case ANALYZE_TABLES:
                analyzeAllTables();
                break;
        }
    }
}
```

### 3. **Maintenance Windows**

```java
public class MaintenanceWindow {
    private final LocalTime startTime;
    private final LocalTime endTime;
    private final Set<DayOfWeek> allowedDays;

    public boolean isInMaintenanceWindow() {
        LocalDateTime now = LocalDateTime.now();
        LocalTime currentTime = now.toLocalTime();
        DayOfWeek currentDay = now.getDayOfWeek();

        return allowedDays.contains(currentDay) &&
               currentTime.isAfter(startTime) &&
               currentTime.isBefore(endTime);
    }

    public void executeIfInWindow(Runnable task) {
        if (isInMaintenanceWindow()) {
            task.run();
        } else {
            scheduleForNextWindow(task);
        }
    }
}
```

## Performance Tuning

### 1. **Connection Pool Monitoring**

```java
public class ConnectionPoolMonitor {
    private final HikariDataSource dataSource;

    public ConnectionPoolStats getStats() {
        HikariPoolMXBean pool = dataSource.getHikariPoolMXBean();

        return new ConnectionPoolStats()
            .withActiveConnections(pool.getActiveConnections())
            .withIdleConnections(pool.getIdleConnections())
            .withTotalConnections(pool.getTotalConnections())
            .withThreadsAwaitingConnection(pool.getThreadsAwaitingConnection())
            .withConnectionWaitTime(getAverageWaitTime());
    }

    public void autoTune() {
        ConnectionPoolStats stats = getStats();

        // Increase pool size if needed
        if (stats.getThreadsAwaitingConnection() > 0) {
            int newSize = Math.min(
                dataSource.getMaximumPoolSize() + 5,
                MAX_POOL_SIZE
            );
            dataSource.setMaximumPoolSize(newSize);
            logger.info("Increased pool size to: " + newSize);
        }

        // Decrease pool size if over-provisioned
        if (stats.getIdleConnections() > stats.getActiveConnections() * 2) {
            int newSize = Math.max(
                dataSource.getMaximumPoolSize() - 5,
                MIN_POOL_SIZE
            );
            dataSource.setMaximumPoolSize(newSize);
            logger.info("Decreased pool size to: " + newSize);
        }
    }
}
```

### 2. **Query Optimization**

```java
public class QueryOptimizer {
    private final Map<String, QueryStats> queryStats = new ConcurrentHashMap<>();

    public void recordQuery(String query, long duration) {
        queryStats.computeIfAbsent(query, k -> new QueryStats())
                  .record(duration);
    }

    public List<String> getSlowQueries(int limit) {
        return queryStats.entrySet().stream()
            .filter(e -> e.getValue().getAvgDuration() > SLOW_QUERY_THRESHOLD)
            .sorted((a, b) -> Long.compare(
                b.getValue().getAvgDuration(),
                a.getValue().getAvgDuration()
            ))
            .limit(limit)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    }

    public void suggestOptimizations() {
        for (String slowQuery : getSlowQueries(10)) {
            QueryPlan plan = analyzeQuery(slowQuery);

            if (plan.isFullTableScan()) {
                suggest("Add index on: " + plan.getScanColumns());
            }

            if (plan.hasFileSort()) {
                suggest("Add covering index for ORDER BY");
            }

            if (plan.hasTempTable()) {
                suggest("Optimize JOIN or subquery");
            }
        }
    }
}
```

## Alert Management

### 1. **Alert Configuration**

```java
public class AlertManager {
    private final List<AlertRule> rules = new ArrayList<>();
    private final List<AlertHandler> handlers = new ArrayList<>();

    public void configure() {
        // High latency alert
        rules.add(new AlertRule()
            .withMetric("query_p99_latency")
            .withThreshold(1000)  // 1 second
            .withDuration(Duration.ofMinutes(5))
            .withSeverity(Severity.WARNING));

        // Error rate alert
        rules.add(new AlertRule()
            .withMetric("error_rate")
            .withThreshold(0.01)  // 1% error rate
            .withDuration(Duration.ofMinutes(10))
            .withSeverity(Severity.CRITICAL));

        // Table count alert
        rules.add(new AlertRule()
            .withMetric("table_count")
            .withThreshold(10000)
            .withSeverity(Severity.WARNING));
    }

    public void checkAlerts() {
        for (AlertRule rule : rules) {
            if (rule.isTriggered()) {
                Alert alert = new Alert(rule, getCurrentValue(rule.getMetric()));
                sendAlert(alert);
            }
        }
    }

    private void sendAlert(Alert alert) {
        for (AlertHandler handler : handlers) {
            handler.handle(alert);
        }
    }
}
```

### 2. **Alert Handlers**

```java
public class EmailAlertHandler implements AlertHandler {
    public void handle(Alert alert) {
        Email email = new Email()
            .to(alertRecipients)
            .subject("Split-Verse Alert: " + alert.getTitle())
            .body(formatAlert(alert));

        emailService.send(email);
    }
}

public class SlackAlertHandler implements AlertHandler {
    public void handle(Alert alert) {
        SlackMessage message = new SlackMessage()
            .channel("#alerts")
            .text(alert.getTitle())
            .attachments(formatDetails(alert));

        slackClient.send(message);
    }
}
```

## Best Practices

### 1. **Monitoring Setup**

- Enable metrics collection from day one
- Set up alerting before going to production
- Monitor both application and database metrics
- Track business metrics alongside technical metrics

### 2. **Maintenance Planning**

- Schedule maintenance during low-traffic periods
- Test maintenance procedures in staging
- Have rollback plans for all maintenance operations
- Monitor closely after maintenance

### 3. **Performance Baselines**

- Establish normal performance baselines
- Track trends over time
- Set alerts based on deviations from baseline
- Regular performance reviews

### 4. **Capacity Planning**

- Monitor growth trends
- Plan for seasonal variations
- Provision capacity ahead of demand
- Regular capacity reviews
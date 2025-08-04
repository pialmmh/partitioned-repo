# 🏊‍♂️ HikariCP Integration

This document explains the HikariCP connection pooling integration in the Generic Repository Framework.

## ✨ Overview

Both `GenericMultiTableRepository` and `GenericPartitionedTableRepository` now use **HikariCP** as the exclusive connection pooling solution, providing:

- **High Performance**: HikariCP is the fastest JDBC connection pool
- **Production Ready**: Battle-tested in high-load environments  
- **Low Overhead**: Minimal memory footprint and CPU usage
- **Connection Management**: Automatic connection lifecycle management
- **Leak Detection**: Built-in connection leak detection and reporting
- **Monitoring**: JMX metrics and health checks

## 🔧 Configuration Options

### Connection Pool Settings

```java
GenericMultiTableRepository<SmsEntity, Long> repo = 
    GenericMultiTableRepository.<SmsEntity, Long>builder(SmsEntity.class, Long.class)
        // Database connection
        .host("localhost")
        .port(3306)
        .database("messaging")
        .username("app_user")
        .password("secure_password")
        
        // HikariCP pool configuration
        .maxPoolSize(20)              // Maximum connections in pool (default: 20)
        .minIdleConnections(5)        // Minimum idle connections (default: 5)
        .connectionTimeout(30000)     // Connection timeout in ms (default: 30 seconds)
        .idleTimeout(600000)          // Idle timeout in ms (default: 10 minutes)
        .maxLifetime(1800000)         // Max connection lifetime in ms (default: 30 minutes)
        .leakDetectionThreshold(60000) // Leak detection threshold in ms (default: 1 minute)
        .build();
```

### Configuration Parameters Explained

| Parameter | Description | Default Value | Recommendation |
|-----------|-------------|---------------|----------------|
| `maxPoolSize` | Maximum number of connections in pool | 20 | Start with 10-30, tune based on load |
| `minIdleConnections` | Minimum idle connections maintained | 5 | Usually 1/4 of maxPoolSize |
| `connectionTimeout` | Max time to wait for connection (ms) | 30000 | 30 seconds is usually sufficient |
| `idleTimeout` | Max time connection can be idle (ms) | 600000 | 10+ minutes for web apps |
| `maxLifetime` | Max connection lifetime (ms) | 1800000 | Less than DB server timeout |
| `leakDetectionThreshold` | Time before leak warning (ms) | 60000 | Enable in development/staging |

## 🚀 Performance Benefits

### Automatic Optimizations

HikariCP automatically applies MySQL-specific optimizations:

```java
// Prepared statement caching
config.addDataSourceProperty("cachePrepStmts", "true");
config.addDataSourceProperty("prepStmtCacheSize", "250");
config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

// Server-side prepared statements
config.addDataSourceProperty("useServerPrepStmts", "true");

// Batch rewriting for performance
config.addDataSourceProperty("rewriteBatchedStatements", "true");

// Connection reliability
config.addDataSourceProperty("autoReconnect", "true");
config.addDataSourceProperty("maxReconnects", "3");
```

### Connection Pool Monitoring

Each repository gets a named pool for easy monitoring:

```java
// Pool names for JMX monitoring
"MultiTableRepository-sms"           // For SMS multi-table repo
"PartitionedTableRepository-orders"  // For Orders partitioned repo
```

## 📊 Usage Examples

### High-Volume SMS Processing

```java
GenericMultiTableRepository<SmsEntity, Long> smsRepo = 
    GenericMultiTableRepository.<SmsEntity, Long>builder(SmsEntity.class, Long.class)
        .database("messaging")
        .username("sms_service")
        .password(System.getenv("DB_PASSWORD"))
        .tablePrefix("sms")
        
        // Optimized for high-volume SMS processing
        .maxPoolSize(25)              // Handle burst traffic
        .minIdleConnections(5)        // Keep connections ready
        .connectionTimeout(30000)     // Quick connection acquisition
        .idleTimeout(600000)          // 10 min idle (SMS is frequent)
        .maxLifetime(1800000)         // 30 min lifetime
        .leakDetectionThreshold(60000) // Detect leaks quickly
        .build();
```

### Enterprise Order Processing

```java
GenericPartitionedTableRepository<OrderEntity, Long> orderRepo = 
    GenericPartitionedTableRepository.<OrderEntity, Long>builder(OrderEntity.class, Long.class)
        .database("ecommerce")
        .username("order_service")  
        .password(System.getenv("DB_PASSWORD"))
        .tableName("orders")
        
        // Optimized for complex order processing
        .maxPoolSize(30)              // Handle complex queries
        .minIdleConnections(10)       // More idle connections for stability
        .connectionTimeout(30000)     // Standard timeout
        .idleTimeout(900000)          // 15 min idle (longer operations)
        .maxLifetime(3600000)         // 1 hour lifetime (longer transactions)
        .leakDetectionThreshold(120000) // 2 min leak detection (complex queries)
        .build();
```

### Development/Testing Configuration

```java
GenericMultiTableRepository<SmsEntity, Long> devRepo = 
    GenericMultiTableRepository.<SmsEntity, Long>builder(SmsEntity.class, Long.class)
        .database("test_db")
        .username("test_user")
        .password("test_password")
        
        // Minimal pool for testing
        .maxPoolSize(5)               // Small pool for tests
        .minIdleConnections(1)        // Minimal idle connections
        .connectionTimeout(10000)     // Faster timeout for tests
        .idleTimeout(300000)          // 5 min idle
        .maxLifetime(900000)          // 15 min lifetime
        .leakDetectionThreshold(30000) // Aggressive leak detection
        .build();
```

## 🔒 Production Best Practices

### 1. **Connection Pool Sizing**

```java
// Rule of thumb: 
// maxPoolSize = (CPU cores * 2) + number of disks
// For 8-core server with SSD: maxPoolSize = (8 * 2) + 1 = 17

.maxPoolSize(17)
.minIdleConnections(4)  // ~25% of max
```

### 2. **Security Configuration**

```java
.username(System.getenv("DB_USERNAME"))     // From environment
.password(System.getenv("DB_PASSWORD"))     // From environment

// SSL enabled for production
config.addDataSourceProperty("useSSL", "true");
config.addDataSourceProperty("requireSSL", "true");
```

### 3. **Monitoring Setup**

```java
// Enable JMX for monitoring
config.setRegisterMbeans(true);

// Custom pool name for monitoring
config.setPoolName("MyApp-MainDB-Pool");
```

### 4. **Graceful Shutdown**

```java
// Always shutdown properly
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    repository.shutdown(); // Closes HikariCP pool and scheduler
}));
```

## 🔍 Connection Leak Detection

HikariCP automatically detects connection leaks:

```java
// Enable leak detection (development/staging)
.leakDetectionThreshold(60000)  // 1 minute warning

// Disable in production (set to 0)
.leakDetectionThreshold(0)
```

### Leak Detection Output

```
[WARN] HikariCP - Connection leak detection triggered for thread pool-1-thread-1, 
stack trace follows:
Exception in thread "pool-1-thread-1" 
    at com.yourapp.SmsService.processMessage(SmsService.java:42)
    at com.yourapp.MessageProcessor.run(MessageProcessor.java:15)
```

## 📈 Performance Tuning

### Typical Performance Settings

```java
// High-throughput application
.maxPoolSize(50)              // Large pool
.minIdleConnections(10)       // Keep connections ready
.connectionTimeout(20000)     // Shorter timeout
.idleTimeout(300000)          // Shorter idle time
.maxLifetime(1200000)         // 20 minute lifetime

// Low-latency application  
.maxPoolSize(15)              // Smaller, focused pool
.minIdleConnections(8)        // High idle ratio
.connectionTimeout(10000)     // Very fast acquisition
.idleTimeout(600000)          // Standard idle time
.maxLifetime(1800000)         // Standard lifetime
```

## 🛠️ Troubleshooting

### Common Issues

1. **Pool Exhaustion**
   ```
   ERROR: Connection pool exhausted
   Solution: Increase maxPoolSize or check for connection leaks
   ```

2. **Connection Timeouts**
   ```
   ERROR: Connection timeout after 30000ms
   Solution: Increase connectionTimeout or maxPoolSize
   ```

3. **Connection Leaks**
   ```
   WARN: Connection leak detected
   Solution: Check try-with-resources usage, enable leak detection
   ```

### Monitoring Queries

```sql
-- Check active connections
SHOW PROCESSLIST;

-- Check connection limits
SHOW VARIABLES LIKE 'max_connections';

-- Check connection usage
SHOW STATUS LIKE 'Threads_connected';
```

This HikariCP integration provides enterprise-grade connection pooling with optimal performance and reliability for your partitioned data access needs.
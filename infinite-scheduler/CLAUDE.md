# Infinite Scheduler - High-Performance Distributed Job Scheduler

## Project Overview

**Infinite Scheduler** is a high-performance, scalable job scheduling system built on Quartz Scheduler with MySQL persistence, designed to handle millions to billions of scheduled jobs efficiently. It integrates with the **partitioned-repo** library's ShardingRepository interface for optimal data management and automatic cleanup.

## Core Requirements

### 1. **Quartz Scheduler Foundation with MySQL Persistence**
- **Base Technology:** Quartz Scheduler with JDBC JobStore (MySQL)
- **Persistence:** All job data, triggers, and state stored in MySQL for durability
- **Misfire Handling:** Default misfire instruction: **MISFIRE_INSTRUCTION_FIRE_ONCE_NOW**
  - Failed jobs automatically retry once when scheduler recovers
  - Prevents job accumulation during system downtime
- **Clustering:** Support for multi-node scheduler clustering for high availability

### 2. **ShardingRepository Integration with ShardingEntity Constraint**
- **Data Source:** Uses `ShardingRepository<T extends ShardingEntity<K>, K>` interface from **partitioned-repo** library
- **Repository Types:** Support for both:
  - `GenericMultiTableRepository<T extends ShardingEntity<K>, K>` for high-volume, short-retention data
  - `GenericPartitionedTableRepository<T extends ShardingEntity<K>, K>` for structured, long-retention data
- **Entity Constraint:** **ALL entities to be scheduled MUST implement `ShardingEntity<K>` interface**
- **Required Annotations:** Guarantees presence of `@Id` and `@ShardingKey` annotated fields for scheduling
- **Flexible Naming:** Field names can be anything as long as proper annotations are applied
- **Type Safety:** Full compile-time type safety with bounded generic parameters

### 3. **Virtual Thread-Based Fetcher**
- **Fetcher Thread:** Single virtual thread running continuously
- **Fetch Interval:** Configurable interval `n` seconds (default: 25 seconds)
- **Lookahead Window:** Configurable window `n1` seconds (default: 30 seconds)
- **Constraint:** `n1 > n` (lookahead must be greater than fetch interval)
- **Data Query:** Fetches entities from ShardingRepository for time range `[now, now + n1]`

### 4. **Dynamic Job Scheduling**
- **Job Creation:** For each fetched entity, create and schedule a Quartz job
- **Execution Time:** Schedule job to execute at entity's specified execution time
- **Persistence:** All jobs persisted to MySQL immediately upon creation
- **Job Data:** Include entity data and metadata in JobDataMap
- **Failure Recovery:** Failed jobs automatically recovered from database on scheduler restart

### 5. **Massive Scale Handling**
- **Volume:** Designed to handle millions to billions of scheduled jobs
- **Performance:** Optimized for high-throughput job creation and execution
- **Memory Management:** Efficient memory usage to prevent OOM issues
- **Auto-Cleanup:** Leverage partitioned-repo's automatic partition/table cleanup
- **Backlog Prevention:** Completed jobs automatically cleaned by repository retention policies

## Technical Architecture

### **Core Components**

#### 1. **InfiniteScheduler**
```java
public class InfiniteScheduler<TEntity extends ShardingEntity<TKey>, TKey> {
    private final ShardingRepository<TEntity, TKey> repository;
    private final Scheduler quartzScheduler;
    private final SchedulerConfig config;
    private final VirtualThread fetcherThread;
}
```

#### 2. **SchedulerConfig**
```java
public class SchedulerConfig {
    private final int fetchIntervalSeconds;      // n - fetch every 25 seconds
    private final int lookaheadWindowSeconds;    // n1 - fetch for next 30 seconds
    private final String quartzDataSource;       // MySQL connection for Quartz
    private final int maxJobsPerFetch;           // Limit jobs per fetch cycle
    private final MisfireInstruction misfirePolicy;
}
```

#### 3. **EntityJobExecutor**
```java
@Component
public class EntityJobExecutor<TEntity extends ShardingEntity<TKey>, TKey> implements Job {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        // Execute the actual business logic for the entity
        TEntity entity = (TEntity) context.getJobDetail().getJobDataMap().get("entity");
        
        // Access fields through EntityMetadata (annotation-based)
        EntityMetadata<TEntity, TKey> metadata = 
            (EntityMetadata<TEntity, TKey>) context.getJobDetail().getJobDataMap().get("metadata");
        
        TKey entityId = metadata.getId(entity);
        LocalDateTime shardingKeyValue = metadata.getShardingKeyValue(entity);
        
        // Business logic execution here
    }
}
```

## ShardingEntity Interface Requirements

### **Mandatory Interface Implementation**

ALL entities used for scheduling **MUST** implement the `ShardingEntity<K>` interface:

```java
public interface ShardingEntity<K> {
    // This interface serves as a marker interface for type safety.
    // The actual ID and sharding key fields are detected via reflection
    // using @Id and @ShardingKey annotations, allowing flexible naming.
    //
    // Required annotations on entity fields:
    // - One field must be annotated with @Id (any field name allowed)
    // - One field must be annotated with @ShardingKey (any field name allowed)
    // - @Column annotations determine the database column names
}
```

### **Benefits of ShardingEntity Constraint:**

1. **Compile-Time Safety:** Prevents invalid entities from being used
2. **Guaranteed Fields:** Ensures `@Id` and `@ShardingKey` annotated fields are always available
3. **Flexible Naming:** Field names can be anything as long as proper annotations are applied
4. **Consistent API:** All scheduled entities follow the same annotation contract
5. **Type Safety:** Full generic type checking with bounded parameters
6. **Scheduling Reliability:** Guaranteed access to timing information for job scheduling

### **Data Flow**

1. **Fetcher Thread (Virtual Thread):**
   - Runs every `n` seconds (25s default)
   - Queries ShardingRepository: `findAllByDateRange(now, now + n1)`
   - Only entities implementing `ShardingEntity<K>` can be fetched
   - Uses `@ShardingKey` annotated field for time-based filtering

2. **Job Scheduling:**
   - For each entity, create JobDetail and Trigger
   - Use `@Id` annotated field for unique job identification
   - Use `@ShardingKey` annotated field for execution time calculation
   - Persist job to MySQL via Quartz JDBC JobStore
   - Apply misfire policy: FIRE_ONCE_NOW

3. **Job Execution:**
   - Quartz fires jobs at scheduled times
   - EntityJobExecutor accesses entity via reflection and annotations
   - Guaranteed access to ID and timing information via `@Id` and `@ShardingKey` fields
   - Job completion triggers cleanup

4. **Auto-Cleanup:**
   - Repository's retention policies automatically clean old data
   - Quartz's built-in cleanup removes executed jobs
   - System remains stable even with billions of jobs

## Implementation Phases

### **Phase 1: Core Scheduler Framework**
- [ ] Set up Quartz Scheduler with MySQL JDBC JobStore
- [ ] Create InfiniteScheduler generic class with ShardingRepository integration
- [ ] Implement ShardingEntity interface constraint validation
- [ ] Implement basic virtual thread fetcher
- [ ] Configure misfire handling policies
- [ ] Add basic job execution framework

### **Phase 2: Advanced Features**
- [ ] Implement configurable fetch intervals and lookahead windows
- [ ] Add batch processing for high-volume job creation
- [ ] Implement job priority and categorization
- [ ] Add monitoring and metrics collection
- [ ] Create management APIs for scheduler control

### **Phase 3: Production Optimization**
- [ ] Add clustering support for high availability
- [ ] Implement advanced error handling and retry policies
- [ ] Add comprehensive logging and observability
- [ ] Performance tuning for massive scale
- [ ] Documentation and examples

### **Phase 4: Integration & Testing**
- [ ] Create example integrations with SMS, Order, and Notification entities
- [ ] Load testing with millions of jobs
- [ ] Failover and recovery testing
- [ ] Production deployment guides

## Configuration Examples

### **SMS Schedule Entity (Must Implement ShardingEntity)**
```java
@Table(name = "sms_schedule")
public class SmsScheduleEntity implements ShardingEntity<Long> {
    @Id
    @Column(name = "sms_id", insertable = false)
    private Long smsId;                    // Flexible field name - not required to be 'id'
    
    @ShardingKey
    @Column(name = "schedule_time", nullable = false)
    private LocalDateTime scheduleTime;    // Flexible field name - not required to be 'created_at'
    
    @Column(name = "phone_number", nullable = false)
    private String phoneNumber;
    
    @Column(name = "message", nullable = false)
    private String message;
    
    // Constructors
    public SmsScheduleEntity() {}
    
    public SmsScheduleEntity(String phoneNumber, String message, LocalDateTime scheduleTime) {
        this.phoneNumber = phoneNumber;
        this.message = message;
        this.scheduleTime = scheduleTime;
    }
    
    // Getters/setters for all fields
    public Long getSmsId() { return smsId; }
    public void setSmsId(Long smsId) { this.smsId = smsId; }
    
    public LocalDateTime getScheduleTime() { return scheduleTime; }
    public void setScheduleTime(LocalDateTime scheduleTime) { this.scheduleTime = scheduleTime; }
    
    public String getPhoneNumber() { return phoneNumber; }
    public void setPhoneNumber(String phoneNumber) { this.phoneNumber = phoneNumber; }
    
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}
```

### **Basic SMS Scheduling**
```java
// Repository for SMS schedule entities (type-safe with ShardingEntity constraint)
ShardingRepository<SmsScheduleEntity, Long> smsRepo = 
    GenericMultiTableRepository.<SmsScheduleEntity, Long>builder(SmsScheduleEntity.class, Long.class)
        .database("messaging")
        .tablePrefix("sms_schedule")
        .build();

// Scheduler configuration
SchedulerConfig config = SchedulerConfig.builder()
    .fetchInterval(25)        // Fetch every 25 seconds
    .lookaheadWindow(30)      // Look 30 seconds ahead
    .quartzDataSource("jdbc:mysql://localhost:3306/scheduler")
    .maxJobsPerFetch(10000)   // Process up to 10K jobs per fetch
    .build();

// Create infinite scheduler with type safety
InfiniteScheduler<SmsScheduleEntity, Long> scheduler = 
    new InfiniteScheduler<>(smsRepo, config);

scheduler.start();
```

### **Order Schedule Entity Example**
```java
@Table(name = "order_schedule")
public class OrderScheduleEntity implements ShardingEntity<String> {
    @Id
    @Column(name = "task_uuid", insertable = false)
    private String taskId;                   // String ID with custom field name
    
    @ShardingKey
    @Column(name = "execution_time", nullable = false)
    private LocalDateTime executionTime;     // Custom sharding field name
    
    @Column(name = "order_id", nullable = false)
    private Long orderId;
    
    @Column(name = "action", nullable = false)
    private String action; // REMINDER, FOLLOW_UP, CANCEL_IF_UNPAID, etc.
    
    @Column(name = "priority")
    private Integer priority;
    
    // Constructors
    public OrderScheduleEntity() {}
    
    public OrderScheduleEntity(String taskId, Long orderId, String action, 
                             LocalDateTime executionTime, Integer priority) {
        this.taskId = taskId;
        this.orderId = orderId;
        this.action = action;
        this.executionTime = executionTime;
        this.priority = priority;
    }
    
    // Getters/setters
    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    
    public LocalDateTime getExecutionTime() { return executionTime; }
    public void setExecutionTime(LocalDateTime executionTime) { this.executionTime = executionTime; }
    
    public Long getOrderId() { return orderId; }
    public void setOrderId(Long orderId) { this.orderId = orderId; }
    
    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }
    
    public Integer getPriority() { return priority; }
    public void setPriority(Integer priority) { this.priority = priority; }
}
```

### **Production Configuration**
```java
// Partitioned repository for high-volume order scheduling (String ID type)
ShardingRepository<OrderScheduleEntity, String> orderRepo = 
    GenericPartitionedTableRepository.<OrderScheduleEntity, String>builder(OrderScheduleEntity.class, String.class)
        .database("ecommerce")
        .tableName("order_schedule")
        .partitionRetentionPeriod(90)  // 90 days retention
        .build();

SchedulerConfig productionConfig = SchedulerConfig.builder()
    .fetchInterval(15)                    // More frequent fetching
    .lookaheadWindow(45)                  // Larger lookahead window
    .quartzDataSource("jdbc:mysql://prod-db:3306/scheduler?useSSL=true")
    .maxJobsPerFetch(50000)               // Higher throughput
    .misfireInstruction(MISFIRE_INSTRUCTION_FIRE_ONCE_NOW)
    .clusteringEnabled(true)              // Enable clustering
    .threadPoolSize(20)                   // Quartz thread pool
    .batchSize(1000)                      // Batch job creation
    .build();

InfiniteScheduler<OrderScheduleEntity, String> scheduler = 
    new InfiniteScheduler<>(orderRepo, productionConfig);
```

## Compile-Time Validation

### **Valid Entity (Compiles Successfully):**
```java
// ✅ This works - entity implements ShardingEntity<UUID> with flexible field names
@Table(name = "notification_schedule")
public class NotificationScheduleEntity implements ShardingEntity<UUID> {
    @Id @Column(name = "notification_uuid") private UUID notificationId;  // Custom field name
    @ShardingKey @Column(name = "delivery_time") private LocalDateTime deliveryTime;  // Custom field name
    
    @Column(name = "recipient") private String recipient;
    @Column(name = "message") private String message;
    
    // Getters/setters (no interface methods required - annotation-based)
    public UUID getNotificationId() { return notificationId; }
    public void setNotificationId(UUID notificationId) { this.notificationId = notificationId; }
    public LocalDateTime getDeliveryTime() { return deliveryTime; }
    public void setDeliveryTime(LocalDateTime deliveryTime) { this.deliveryTime = deliveryTime; }
    // ... other getters/setters
}

// ✅ Scheduler creation works with flexible UUID type
InfiniteScheduler<NotificationScheduleEntity, UUID> scheduler = 
    new InfiniteScheduler<>(repository, config);
```

### **Invalid Entity (Compilation Error):**
```java
// ❌ This entity does NOT implement ShardingEntity<K>
public class InvalidScheduleEntity {
    private Long id;
    private LocalDateTime timestamp; // Missing annotations!
    // No ShardingEntity interface implementation!
}

// ❌ COMPILATION ERROR: InvalidScheduleEntity doesn't extend ShardingEntity<Long>
InfiniteScheduler<InvalidScheduleEntity, Long> scheduler = // Won't compile!
    new InfiniteScheduler<>(repository, config);
```

## Performance Characteristics

### **Expected Performance**
- **Job Creation Rate:** 10,000+ jobs/second
- **Job Execution Rate:** 5,000+ jobs/second per node
- **Memory Usage:** < 2GB for millions of active jobs
- **Database Storage:** Efficient with automatic cleanup
- **Scalability:** Linear scaling with additional nodes

### **Resource Requirements**
- **CPU:** Virtual threads minimize CPU overhead
- **Memory:** Bounded memory usage regardless of job count
- **Database:** MySQL with appropriate indexing and partitioning
- **Network:** Minimal network usage for job coordination

## Integration Points

### **With partitioned-repo Library**
- Import as Maven dependency: `com.telcobright:partitioned-repo:1.0.0`
- Use ShardingRepository interface for all data operations
- Leverage automatic table/partition management
- Benefit from HikariCP connection pooling
- **ALL entities MUST implement ShardingEntity<K> interface**

### **Entity Requirements Checklist**
- ✅ **Implement ShardingEntity<K>** interface (marker interface)
- ✅ **@Id annotation** on primary key field (any field name allowed)
- ✅ **@ShardingKey annotation** on date/time field for partitioning (any field name allowed)
- ✅ **@Column annotations** to specify database column names
- ✅ **@Table annotation** for table mapping
- ✅ **LocalDateTime type** for @ShardingKey field (required for partitioning)
- ✅ **Getters/setters** for all fields (standard Java bean pattern)

## Monitoring & Observability

### **Key Metrics**
- Jobs scheduled per second
- Jobs executed per second
- Jobs failed/retried
- Fetch cycle duration
- Repository query performance
- Memory and CPU utilization
- ShardingEntity constraint violations (compile-time)
- Entity annotation validation errors (startup-time)

### **Logging**
- Structured logging with correlation IDs
- Job lifecycle tracking
- Entity type and ID information
- Performance metrics
- Error reporting and alerting

## Benefits

### **Scalability**
- Handle billions of jobs without performance degradation
- Linear scaling with additional scheduler nodes
- Automatic cleanup prevents disk space issues

### **Reliability**
- MySQL persistence ensures job durability
- Misfire handling prevents job loss
- Clustering provides high availability
- **Compile-time entity validation prevents runtime errors**

### **Performance**
- Virtual threads minimize resource overhead
- Batch processing optimizes database operations
- ShardingRepository provides efficient data access
- **Guaranteed field access eliminates reflection overhead**

### **Maintainability**
- **ShardingEntity interface enforces consistent contracts**
- **Annotation-based field detection provides flexibility**
- Generic design supports any compliant entity type with any field names
- Automatic cleanup eliminates manual maintenance
- Comprehensive monitoring and logging
- **Compile-time safety prevents integration issues**
- **Runtime validation ensures proper annotation usage**

---

## Dependencies

### **Required Libraries**
- **Quartz Scheduler:** `org.quartz-scheduler:quartz:2.3.2`
- **MySQL Connector:** `mysql:mysql-connector-java:8.0.33`
- **HikariCP:** `com.zaxxer:HikariCP:5.0.1`
- **partitioned-repo:** `com.telcobright:partitioned-repo:1.0.0` (local)

### **Java Requirements**
- **Java Version:** 21+ (for Virtual Threads)
- **Memory:** Minimum 4GB heap for production
- **Database:** MySQL 8.0+ or MariaDB 10.6+

---

**Note:** This project enforces the **ShardingEntity<K> interface contract** at compile-time, ensuring all scheduled entities have the required `@Id` and `@ShardingKey` annotated fields. Field names are flexible - any names can be used as long as proper annotations are applied. This prevents runtime errors and provides guaranteed access to essential scheduling information through annotation-based reflection. The constraint is enforced through bounded generic parameters: `<T extends ShardingEntity<K>, K>`.
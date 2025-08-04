package com.telcobright.db.repository;

import com.telcobright.db.query.QueryDSL;
import com.telcobright.db.entity.OrderEntity;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.math.BigDecimal;

/**
 * Single partitioned table repository for entities with generic ID type
 * Uses MySQL native partitioning on single table
 * Partitions by date: p20250803, p20250804, etc.
 * @param <T> The type of the ID column (Long, String, UUID, etc.)
 */
public class PartitionedTableRepository<T> {
    
    private final DataSource dataSource;
    private final String database;
    private final String tableName;
    private final int partitionRetentionPeriod;
    private final boolean autoManagePartitions;
    private final boolean initializePartitionsOnStart;
    private final LocalTime partitionAdjustmentTime;
    private final ScheduledExecutorService scheduler;
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    private PartitionedTableRepository(Builder<T> builder) {
        this.dataSource = createDataSource(builder.host, builder.port, builder.database, 
                                          builder.username, builder.password);
        this.database = builder.database;
        this.tableName = builder.tableName;
        this.partitionRetentionPeriod = builder.partitionRetentionPeriod;
        this.autoManagePartitions = builder.autoManagePartitions;
        this.initializePartitionsOnStart = builder.initializePartitionsOnStart;
        this.partitionAdjustmentTime = builder.partitionAdjustmentTime;
        this.scheduler = autoManagePartitions ? Executors.newScheduledThreadPool(1) : null;
        
        if (initializePartitionsOnStart) {
            try {
                createTableIfNotExists();
                LocalDateTime now = LocalDateTime.now();
                addPartitions(
                    now.minusDays(partitionRetentionPeriod), 
                    now.plusDays(partitionRetentionPeriod)
                );
            } catch (SQLException e) {
                throw new RuntimeException("Failed to initialize partitions on start", e);
            }
        }
        
        // Start daily scheduler if auto-management is enabled
        if (autoManagePartitions && scheduler != null) {
            startDailyScheduler();
        }
    }
    
    public static <T> Builder<T> builder() {
        return new Builder<T>();
    }
    
    public static class Builder<T> {
        private String host = "localhost";
        private int port = 3306;
        private String database;
        private String username;
        private String password;
        private String tableName;
        private int partitionRetentionPeriod = 30; // Default 30 days
        private boolean autoManagePartitions = true; // Default enabled
        private boolean initializePartitionsOnStart = false; // Default disabled
        private LocalTime partitionAdjustmentTime = LocalTime.of(4, 0); // Default 04:00
        
        public Builder<T> host(String host) {
            this.host = host;
            return this;
        }
        
        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }
        
        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }
        
        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }
        
        public Builder<T> database(String database) {
            this.database = database;
            return this;
        }
        
        public Builder<T> tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }
        
        public Builder<T> partitionRetentionPeriod(int days) {
            this.partitionRetentionPeriod = days;
            return this;
        }
        
        public Builder<T> autoManagePartitions(boolean autoManage) {
            this.autoManagePartitions = autoManage;
            return this;
        }
        
        public Builder<T> initializePartitionsOnStart(boolean initialize) {
            this.initializePartitionsOnStart = initialize;
            return this;
        }
        
        public Builder<T> partitionAdjustmentTime(LocalTime adjustmentTime) {
            this.partitionAdjustmentTime = adjustmentTime;
            return this;
        }
        
        public Builder<T> partitionAdjustmentTime(int hour, int minute) {
            this.partitionAdjustmentTime = LocalTime.of(hour, minute);
            return this;
        }
        
        public PartitionedTableRepository<T> build() {
            if (database == null || database.trim().isEmpty()) {
                throw new IllegalArgumentException("Database name is required");
            }
            if (username == null || username.trim().isEmpty()) {
                throw new IllegalArgumentException("Username is required");
            }
            if (password == null) {
                throw new IllegalArgumentException("Password is required");
            }
            if (tableName == null || tableName.trim().isEmpty()) {
                throw new IllegalArgumentException("Table name is required");
            }
            if (partitionRetentionPeriod <= 0) {
                throw new IllegalArgumentException("Partition retention period must be positive");
            }
            
            return new PartitionedTableRepository<T>(this);
        }
    }
    
    /**
     * Insert order into partitioned table (MySQL handles routing)
     */
    public void insert(OrderEntity order) throws SQLException {
        // Automatically ensure partitions exist for the order date
        ensurePartitionExistsForDate(order.getCreatedAt());
        
        String sql = "INSERT INTO " + database + "." + tableName + 
            " (customer_id, order_number, total_amount, status, payment_method, " +
            "shipping_address, created_at, shipped_at, delivered_at, item_count) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            
            stmt.setString(1, order.getCustomerId());
            stmt.setString(2, order.getOrderNumber());
            stmt.setBigDecimal(3, order.getTotalAmount());
            stmt.setString(4, order.getStatus());
            stmt.setString(5, order.getPaymentMethod());
            stmt.setString(6, order.getShippingAddress());
            stmt.setTimestamp(7, Timestamp.valueOf(order.getCreatedAt()));
            stmt.setTimestamp(8, order.getShippedAt() != null ? 
                Timestamp.valueOf(order.getShippedAt()) : null);
            stmt.setTimestamp(9, order.getDeliveredAt() != null ? 
                Timestamp.valueOf(order.getDeliveredAt()) : null);
            stmt.setInt(10, order.getItemCount());
            
            stmt.executeUpdate();
            
            try (ResultSet keys = stmt.getGeneratedKeys()) {
                if (keys.next()) {
                    order.setId(keys.getLong(1));
                }
            }
        }
    }
    
    /**
     * Execute query using Query DSL (MySQL handles partition pruning)
     */
    public <R> List<R> executeQuery(QueryDSL.FromBuilder queryBuilder,
                                   Object[] parameters,
                                   ResultMapper<R> mapper) throws SQLException {
        String query = queryBuilder.build();
        return executeQuery(query, null, mapper);
    }
    
    /**
     * Find orders by date range (MySQL partition pruning)
     */
    public List<OrderEntity> findByDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        String query = QueryDSL.select()
            .column("*")
            .from(tableName)
            .where(w -> w.dateRange("created_at", startDate, endDate))
            .build();
        
        return executeQuery(query, null, this::mapOrderEntity);
    }
    
    /**
     * Find entity by ID (partition scan - MySQL will scan all partitions)
     * Warning: This may be slower than date-range queries as MySQL scans all partitions
     * @param id The ID value to search for
     * @return The found entity or null if not found
     */
    public OrderEntity findById(T id) throws SQLException {
        return findById("id", id);
    }
    
    /**
     * Find entity by ID column value (partition scan - MySQL will scan all partitions)
     * Warning: This may be slower than date-range queries as MySQL scans all partitions
     * 
     * @param idColumnName The name of the ID column to search by
     * @param value The value to search for
     * @return The found entity or null if not found
     */
    private OrderEntity findById(String idColumnName, T value) throws SQLException {
        String query = QueryDSL.select()
            .column("*")
            .from(tableName)
            .where(w -> w.equals(idColumnName, value))
            .limit(1)
            .build();
        
        List<OrderEntity> results = executeQuery(query, null, this::mapOrderEntity);
        return results.isEmpty() ? null : results.get(0);
    }
    
    /**
     * Find all entities by ID column value (partition scan - MySQL will scan all partitions)
     * Useful when ID column is not unique or when searching by non-primary key
     * @param idColumnName The name of the ID column to search by
     * @param value The value to search for
     * @return List of found entities
     */
    public List<OrderEntity> findAllById(String idColumnName, T value) throws SQLException {
        String query = QueryDSL.select()
            .column("*")
            .from(tableName)
            .where(w -> w.equals(idColumnName, value))
            .build();
        
        return executeQuery(query, null, this::mapOrderEntity);
    }
    
    /**
     * Count orders in date range
     */
    public long countByDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        String query = QueryDSL.select()
            .count("*", "total_count")
            .from(tableName)
            .where(w -> w.dateRange("created_at", startDate, endDate))
            .build();
        
        List<Long> results = executeQuery(query, null, 
            rs -> rs.getLong("total_count"));
        
        return results.isEmpty() ? 0 : results.get(0);
    }
    
    /**
     * Get customer order statistics
     */
    public List<CustomerOrderStats> getCustomerStats(LocalDateTime startDate, LocalDateTime endDate, int limit) throws SQLException {
        String query = QueryDSL.select()
            .column("customer_id")
            .count("*", "order_count")
            .sum("total_amount", "total_spent")
            .avg("total_amount", "avg_order_value")
            .sum("item_count", "total_items")
            .min("created_at", "first_order")
            .max("created_at", "last_order")
            .from(tableName)
            .where(w -> w
                .dateRange("created_at", startDate, endDate)
                .isNotNull("customer_id"))
            .groupBy("customer_id")
            .orderByDesc("total_spent")
            .limit(limit)
            .build();
        
        return executeQuery(query, null, rs -> new CustomerOrderStats(
            rs.getString("customer_id"),
            rs.getLong("order_count"),
            rs.getBigDecimal("total_spent"),
            rs.getBigDecimal("avg_order_value"),
            rs.getLong("total_items"),
            rs.getTimestamp("first_order").toLocalDateTime(),
            rs.getTimestamp("last_order").toLocalDateTime()
        ));
    }
    
    /**
     * Get daily order statistics
     */
    public List<DailyOrderStats> getDailyStats(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        String query = QueryDSL.select()
            .column("DATE(created_at)", "order_date")
            .count("*", "order_count")
            .aggregate(QueryDSL.AggregateFunction.COUNT, "customer_id", "unique_customers", true)
            .sum("total_amount", "daily_revenue")
            .avg("total_amount", "avg_order_value")
            .sum("item_count", "total_items")
            .sum("CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END", "delivered_count")
            .sum("CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END", "cancelled_count")
            .from(tableName)
            .where(w -> w.dateRange("created_at", startDate, endDate))
            .groupBy("DATE(created_at)")
            .orderByDesc("order_date")
            .build();
        
        return executeQuery(query, null, rs -> new DailyOrderStats(
            rs.getDate("order_date").toLocalDate(),
            rs.getLong("order_count"),
            rs.getLong("unique_customers"),
            rs.getBigDecimal("daily_revenue"),
            rs.getBigDecimal("avg_order_value"),
            rs.getLong("total_items"),
            rs.getLong("delivered_count"),
            rs.getLong("cancelled_count")
        ));
    }
    
    /**
     * Create partitioned table if not exists
     */
    public void createTableIfNotExists() throws SQLException {
        String createSQL = "CREATE TABLE IF NOT EXISTS " + database + "." + tableName + " (" +
            "id BIGINT AUTO_INCREMENT, " +
            "customer_id VARCHAR(50) NOT NULL, " +
            "order_number VARCHAR(100) NOT NULL, " +
            "total_amount DECIMAL(12,2) NOT NULL, " +
            "status VARCHAR(20) NOT NULL, " +
            "payment_method VARCHAR(50), " +
            "shipping_address TEXT, " +
            "created_at DATETIME NOT NULL, " +
            "shipped_at DATETIME, " +
            "delivered_at DATETIME, " +
            "item_count INT NOT NULL DEFAULT 1, " +
            "PRIMARY KEY (id, created_at), " +
            "INDEX idx_customer_id (customer_id), " +
            "INDEX idx_status (status), " +
            "INDEX idx_order_number (order_number)" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 " +
            "PARTITION BY RANGE (TO_DAYS(created_at)) (" +
            generateInitialPartitions() +
            ")";
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(createSQL);
        }
    }
    
    /**
     * Add new partitions for future dates
     */
    public void addPartitions(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        // Get list of existing partitions
        Set<String> existingPartitions = getExistingPartitions();
        
        List<String> newPartitions = new ArrayList<>();
        LocalDateTime current = startDate;
        
        while (!current.isAfter(endDate)) {
            String partitionName = "p" + current.format(DATE_FORMAT);
            
            // Only add if partition doesn't already exist
            if (!existingPartitions.contains(partitionName)) {
                LocalDateTime nextDay = current.plusDays(1);
                
                newPartitions.add(String.format(
                    "PARTITION %s VALUES LESS THAN (TO_DAYS('%s'))",
                    partitionName,
                    nextDay.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                ));
            }
            
            current = current.plusDays(1);
        }
        
        if (!newPartitions.isEmpty()) {
            String alterSQL = "ALTER TABLE " + database + "." + tableName + 
                " ADD PARTITION (" + String.join(", ", newPartitions) + ")";
            
            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute(alterSQL);
                System.out.println("Added " + newPartitions.size() + " partitions to " + tableName);
            }
        }
    }
    
    /**
     * Drop old partitions
     */
    public void dropOldPartitions(LocalDateTime cutoffDate) throws SQLException {
        if (!autoManagePartitions) {
            return; // Skip dropping if auto-management is disabled
        }
        // Get existing partitions
        List<String> oldPartitions = new ArrayList<>();
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT partition_name FROM information_schema.partitions " +
                 "WHERE table_schema = ? AND table_name = ? AND partition_name IS NOT NULL")) {
            
            stmt.setString(1, database);
            stmt.setString(2, tableName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String partitionName = rs.getString("partition_name");
                    if (isPartitionOlderThan(partitionName, cutoffDate)) {
                        oldPartitions.add(partitionName);
                    }
                }
            }
        }
        
        // Drop old partitions
        if (!oldPartitions.isEmpty()) {
            String dropSQL = "ALTER TABLE " + database + "." + tableName +
                " DROP PARTITION " + String.join(", ", oldPartitions);
            
            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute(dropSQL);
                System.out.println("Dropped " + oldPartitions.size() + " old partitions from " + tableName);
            }
        }
    }
    
    /**
     * Clean up old partitions based on retention period
     */
    public void cleanupOldPartitions() throws SQLException {
        if (autoManagePartitions) {
            LocalDateTime cutoffDate = LocalDateTime.now().minusDays(partitionRetentionPeriod);
            dropOldPartitions(cutoffDate);
        }
    }
    
    // Getter methods for configuration
    public int getPartitionRetentionPeriod() {
        return partitionRetentionPeriod;
    }
    
    public boolean isAutoManagePartitions() {
        return autoManagePartitions;
    }
    
    public boolean isInitializePartitionsOnStart() {
        return initializePartitionsOnStart;
    }
    
    public String getDatabase() {
        return database;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public LocalTime getPartitionAdjustmentTime() {
        return partitionAdjustmentTime;
    }
    
    // Private helper methods
    
    /**
     * Get existing partition names for the table
     */
    private Set<String> getExistingPartitions() throws SQLException {
        Set<String> partitions = new HashSet<>();
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT partition_name FROM information_schema.partitions " +
                 "WHERE table_schema = ? AND table_name = ? AND partition_name IS NOT NULL")) {
            
            stmt.setString(1, database);
            stmt.setString(2, tableName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    partitions.add(rs.getString("partition_name"));
                }
            }
        }
        
        return partitions;
    }
    
    /**
     * Create DataSource with MySQL configuration
     */
    private static DataSource createDataSource(String host, int port, String database, 
                                             String username, String password) {
        return new DataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                // Try MariaDB URL first, then MySQL
                String mariadbUrl = String.format("jdbc:mariadb://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC", 
                                         host, port, database);
                String mysqlUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC", 
                                         host, port, database);
                
                try {
                    return DriverManager.getConnection(mariadbUrl, username, password);
                } catch (SQLException e) {
                    // Fallback to MySQL URL
                    return DriverManager.getConnection(mysqlUrl, username, password);
                }
            }
            
            @Override
            public Connection getConnection(String user, String pass) throws SQLException {
                return getConnection();
            }
            
            // Other DataSource methods with default implementations
            @Override public java.io.PrintWriter getLogWriter() throws SQLException { return null; }
            @Override public void setLogWriter(java.io.PrintWriter out) throws SQLException { }
            @Override public void setLoginTimeout(int seconds) throws SQLException { }
            @Override public int getLoginTimeout() throws SQLException { return 0; }
            @Override public java.util.logging.Logger getParentLogger() { return null; }
            @Override public <T> T unwrap(Class<T> iface) throws SQLException { return null; }
            @Override public boolean isWrapperFor(Class<?> iface) throws SQLException { return false; }
        };
    }
    
    /**
     * Automatically ensure partition exists for the given date
     * Also performs maintenance if auto-management is enabled
     */
    private void ensurePartitionExistsForDate(LocalDateTime date) throws SQLException {
        createTableIfNotExists();
        
        // Perform automatic maintenance if enabled
        if (autoManagePartitions) {
            performAutomaticPartitionMaintenance(date);
        }
    }
    
    /**
     * Perform automatic partition maintenance tasks
     */
    private void performAutomaticPartitionMaintenance(LocalDateTime currentDate) throws SQLException {
        // Add partitions for near future dates (next few days)
        LocalDateTime futureDate = currentDate.plusDays(7);
        addPartitions(currentDate, futureDate);
        
        // Clean up old partitions based on retention period
        LocalDateTime cutoffDate = currentDate.minusDays(partitionRetentionPeriod);
        dropOldPartitions(cutoffDate);
    }
    
    /**
     * Start daily scheduler for automatic partition management
     */
    private void startDailyScheduler() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nextRun = now.toLocalDate().atTime(partitionAdjustmentTime);
        
        // If the adjustment time has already passed today, schedule for tomorrow
        if (nextRun.isBefore(now) || nextRun.isEqual(now)) {
            nextRun = nextRun.plusDays(1);
        }
        
        long initialDelayMinutes = java.time.Duration.between(now, nextRun).toMinutes();
        
        System.out.println("📅 Partitioned Scheduler: Next maintenance at " + nextRun + 
                          " (in " + initialDelayMinutes + " minutes)");
        
        scheduler.scheduleAtFixedRate(() -> {
            try {
                performScheduledMaintenance();
            } catch (Exception e) {
                System.err.println("❌ Partitioned scheduled maintenance failed: " + e.getMessage());
                e.printStackTrace();
            }
        }, initialDelayMinutes, 24 * 60, TimeUnit.MINUTES); // Run daily
    }
    
    /**
     * Perform scheduled maintenance - creates/drops partitions based on retention period
     */
    private void performScheduledMaintenance() throws SQLException {
        LocalDateTime today = LocalDateTime.now();
        
        System.out.println("🔧 Partitioned scheduled maintenance started at " + today);
        
        // Calculate the valid range: {today - retentionDays} to {today + retentionDays}
        LocalDateTime startRange = today.minusDays(partitionRetentionPeriod);
        LocalDateTime endRange = today.plusDays(partitionRetentionPeriod);
        
        System.out.println("📊 Valid partition range: " + startRange.toLocalDate() + " to " + endRange.toLocalDate());
        
        // Ensure table exists
        createTableIfNotExists();
        
        // Create missing partitions in the valid range
        addPartitions(startRange, endRange);
        
        // Drop partitions outside the valid range (older than startRange)
        dropOldPartitions(startRange);
        
        System.out.println("✅ Partitioned scheduled maintenance completed");
    }
    
    /**
     * Shutdown the scheduler (call this when application stops)
     */
    public void shutdown() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
    
    private String generateInitialPartitions() {
        List<String> partitions = new ArrayList<>();
        LocalDateTime start = LocalDateTime.now().minusDays(7);
        LocalDateTime end = LocalDateTime.now().plusDays(30);
        LocalDateTime current = start;
        
        while (!current.isAfter(end)) {
            String partitionName = "p" + current.format(DATE_FORMAT);
            LocalDateTime nextDay = current.plusDays(1);
            
            partitions.add(String.format(
                "PARTITION %s VALUES LESS THAN (TO_DAYS('%s'))",
                partitionName,
                nextDay.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
            ));
            
            current = nextDay;
        }
        
        // Add a catch-all partition for future dates
        partitions.add("PARTITION pmax VALUES LESS THAN MAXVALUE");
        
        return String.join(", ", partitions);
    }
    
    private boolean isPartitionOlderThan(String partitionName, LocalDateTime cutoffDate) {
        if (!partitionName.startsWith("p") || partitionName.equals("pmax")) {
            return false;
        }
        
        try {
            String dateStr = partitionName.substring(1);
            LocalDateTime partitionDate = LocalDateTime.parse(dateStr + "000000", 
                DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            return partitionDate.isBefore(cutoffDate);
        } catch (Exception e) {
            return false;
        }
    }
    
    private <R> List<R> executeQuery(String sql, Object[] parameters, ResultMapper<R> mapper) throws SQLException {
        List<R> results = new ArrayList<>();
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            if (parameters != null) {
                for (int i = 0; i < parameters.length; i++) {
                    setParameter(stmt, i + 1, parameters[i]);
                }
            }
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(mapper.map(rs));
                }
            }
        }
        
        return results;
    }
    
    private void setParameter(PreparedStatement stmt, int index, Object value) throws SQLException {
        if (value == null) {
            stmt.setNull(index, Types.NULL);
        } else if (value instanceof String) {
            stmt.setString(index, (String) value);
        } else if (value instanceof LocalDateTime) {
            stmt.setTimestamp(index, Timestamp.valueOf((LocalDateTime) value));
        } else if (value instanceof Integer) {
            stmt.setInt(index, (Integer) value);
        } else if (value instanceof Long) {
            stmt.setLong(index, (Long) value);
        } else if (value instanceof BigDecimal) {
            stmt.setBigDecimal(index, (BigDecimal) value);
        } else {
            stmt.setObject(index, value);
        }
    }
    
    private OrderEntity mapOrderEntity(ResultSet rs) throws SQLException {
        OrderEntity order = new OrderEntity();
        order.setId(rs.getLong("id"));
        order.setCustomerId(rs.getString("customer_id"));
        order.setOrderNumber(rs.getString("order_number"));
        order.setTotalAmount(rs.getBigDecimal("total_amount"));
        order.setStatus(rs.getString("status"));
        order.setPaymentMethod(rs.getString("payment_method"));
        order.setShippingAddress(rs.getString("shipping_address"));
        order.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
        
        Timestamp shipped = rs.getTimestamp("shipped_at");
        order.setShippedAt(shipped != null ? shipped.toLocalDateTime() : null);
        
        Timestamp delivered = rs.getTimestamp("delivered_at");
        order.setDeliveredAt(delivered != null ? delivered.toLocalDateTime() : null);
        
        order.setItemCount(rs.getInt("item_count"));
        
        return order;
    }
    
    // Result classes
    public static class CustomerOrderStats {
        public final String customerId;
        public final long orderCount;
        public final BigDecimal totalSpent;
        public final BigDecimal avgOrderValue;
        public final long totalItems;
        public final LocalDateTime firstOrder;
        public final LocalDateTime lastOrder;
        
        public CustomerOrderStats(String customerId, long orderCount, BigDecimal totalSpent,
                                 BigDecimal avgOrderValue, long totalItems, 
                                 LocalDateTime firstOrder, LocalDateTime lastOrder) {
            this.customerId = customerId;
            this.orderCount = orderCount;
            this.totalSpent = totalSpent;
            this.avgOrderValue = avgOrderValue;
            this.totalItems = totalItems;
            this.firstOrder = firstOrder;
            this.lastOrder = lastOrder;
        }
        
        @Override
        public String toString() {
            return String.format("Customer: %s | Orders: %d | Spent: $%s | Avg: $%s | Items: %d",
                customerId, orderCount, totalSpent, avgOrderValue, totalItems);
        }
    }
    
    public static class DailyOrderStats {
        public final java.time.LocalDate date;
        public final long orderCount;
        public final long uniqueCustomers;
        public final BigDecimal dailyRevenue;
        public final BigDecimal avgOrderValue;
        public final long totalItems;
        public final long deliveredCount;
        public final long cancelledCount;
        public final double fulfillmentRate;
        
        public DailyOrderStats(java.time.LocalDate date, long orderCount, long uniqueCustomers,
                              BigDecimal dailyRevenue, BigDecimal avgOrderValue, long totalItems,
                              long deliveredCount, long cancelledCount) {
            this.date = date;
            this.orderCount = orderCount;
            this.uniqueCustomers = uniqueCustomers;
            this.dailyRevenue = dailyRevenue;
            this.avgOrderValue = avgOrderValue;
            this.totalItems = totalItems;
            this.deliveredCount = deliveredCount;
            this.cancelledCount = cancelledCount;
            this.fulfillmentRate = orderCount > 0 ? (double) deliveredCount / orderCount * 100 : 0;
        }
        
        @Override
        public String toString() {
            return String.format("Date: %s | Orders: %d | Revenue: $%s | Customers: %d | Fulfillment: %.1f%%",
                date, orderCount, dailyRevenue, uniqueCustomers, fulfillmentRate);
        }
    }
    
    @FunctionalInterface
    public interface ResultMapper<R> {
        R map(ResultSet rs) throws SQLException;
    }
}
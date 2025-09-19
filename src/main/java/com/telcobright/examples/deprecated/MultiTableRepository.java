package com.telcobright.examples.deprecated;

import com.telcobright.core.query.QueryDSL;
import com.telcobright.examples.entity.SmsEntity;

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
 * Multi-table repository for entities with generic ID type
 * Creates separate tables per day: sms_20250803, sms_20250804, etc.
 * Uses 2-level queries with UNION ALL for cross-table operations
 * @param <T> The type of the ID column (Long, String, UUID, etc.)
 */
public class MultiTableRepository<T> {
    
    private final DataSource dataSource;
    private final String database;
    private final String tablePrefix;
    private final int partitionRetentionPeriod;
    private final boolean autoManagePartitions;
    private final boolean initializePartitionsOnStart;
    private final LocalTime partitionAdjustmentTime;
    private final ScheduledExecutorService scheduler;
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    private MultiTableRepository(Builder<T> builder) {
        this.dataSource = createDataSource(builder.host, builder.port, builder.database, 
                                          builder.username, builder.password);
        this.database = builder.database;
        this.tablePrefix = builder.tablePrefix;
        this.partitionRetentionPeriod = builder.partitionRetentionPeriod;
        this.autoManagePartitions = builder.autoManagePartitions;
        this.initializePartitionsOnStart = builder.initializePartitionsOnStart;
        this.partitionAdjustmentTime = builder.partitionAdjustmentTime;
        this.scheduler = autoManagePartitions ? Executors.newScheduledThreadPool(1) : null;
        
        if (initializePartitionsOnStart) {
            try {
                LocalDateTime now = LocalDateTime.now();
                createTablesForDateRange(
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
        private String tablePrefix;
        private int partitionRetentionPeriod = 7; // Default 7 days
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
        
        public Builder<T> tablePrefix(String tablePrefix) {
            this.tablePrefix = tablePrefix;
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
        
        public MultiTableRepository<T> build() {
            if (database == null || database.trim().isEmpty()) {
                throw new IllegalArgumentException("Database name is required");
            }
            if (username == null || username.trim().isEmpty()) {
                throw new IllegalArgumentException("Username is required");
            }
            if (password == null) {
                throw new IllegalArgumentException("Password is required");
            }
            if (tablePrefix == null || tablePrefix.trim().isEmpty()) {
                throw new IllegalArgumentException("Table prefix is required");
            }
            if (partitionRetentionPeriod <= 0) {
                throw new IllegalArgumentException("Partition retention period must be positive");
            }
            
            return new MultiTableRepository<T>(this);
        }
    }
    
    /**
     * Insert SMS into appropriate daily table
     */
    public void insert(SmsEntity sms) throws SQLException {
        // Automatically ensure table exists for the SMS date
        ensureTableExistsForDate(sms.getPartitionColValue());
        
        String tableName = getTableName(sms.getPartitionColValue());
        
        String sql = "INSERT INTO " + tableName + 
            " (user_id, phone_number, message, status, created_at, delivered_at, cost, provider) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            
            stmt.setString(1, sms.getUserId());
            stmt.setString(2, sms.getPhoneNumber());
            stmt.setString(3, sms.getMessage());
            stmt.setString(4, sms.getStatus());
            stmt.setTimestamp(5, Timestamp.valueOf(sms.getPartitionColValue()));
            stmt.setTimestamp(6, sms.getDeliveredAt() != null ? 
                Timestamp.valueOf(sms.getDeliveredAt()) : null);
            stmt.setBigDecimal(7, sms.getCost());
            stmt.setString(8, sms.getProvider());
            
            stmt.executeUpdate();
            
            try (ResultSet keys = stmt.getGeneratedKeys()) {
                if (keys.next()) {
                    sms.setId(String.valueOf(keys.getLong(1)));
                }
            }
        }
    }
    
    /**
     * Execute 2-level partitioned query using Query DSL
     */
    public <R> List<R> executePartitionedQuery(QueryDSL.FromBuilder queryBuilder,
                                              LocalDateTime startDate,
                                              LocalDateTime endDate,
                                              Object[] parameters,
                                              ResultMapper<R> mapper) throws SQLException {
        // Build partitioned query
        String partitionedQuery = queryBuilder.buildPartitioned(database, startDate, endDate);
        
        // Execute query - buildPartitioned() embeds parameters, so pass null
        return executeQuery(partitionedQuery, null, mapper);
    }
    
    /**
     * Find SMS by date range using 2-level query
     */
    public List<SmsEntity> findByDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        String query = QueryDSL.select()
            .column("*")
            .from(tablePrefix)
            .where(w -> w.dateRange("created_at", startDate, endDate))
            .buildPartitioned(database, startDate, endDate);
        
        return executeQuery(query, null, this::mapSmsEntity);
    }
    
    /**
     * Find entity by ID across all tables (full table scan)
     * Warning: This may be slow as it searches across all existing tables
     * @param id The ID value to search for
     * @return The found entity or null if not found
     */
    public SmsEntity findById(T id) throws SQLException {
        return findById("id", id);
    }
    
    /**
     * Find entity by ID column value across all tables (full table scan)
     * Warning: This may be slow as it searches across all existing tables
     * 
     * @param idColumnName The name of the ID column to search by
     * @param value The value to search for
     * @return The found entity or null if not found
     */
    private SmsEntity findById(String idColumnName, T value) throws SQLException {
        // Get all existing tables
        List<String> existingTables = getExistingTables();
        
        if (existingTables.isEmpty()) {
            return null;
        }
        
        // Build UNION ALL query across all tables
        StringBuilder queryBuilder = new StringBuilder();
        for (int i = 0; i < existingTables.size(); i++) {
            if (i > 0) {
                queryBuilder.append(" UNION ALL ");
            }
            queryBuilder.append("SELECT * FROM ").append(existingTables.get(i))
                       .append(" WHERE ").append(idColumnName).append(" = ");
            
            // Format value based on type
            if (value instanceof String) {
                queryBuilder.append("'").append(value).append("'");
            } else {
                queryBuilder.append(value);
            }
        }
        
        queryBuilder.append(" LIMIT 1");
        
        List<SmsEntity> results = executeQuery(queryBuilder.toString(), null, this::mapSmsEntity);
        return results.isEmpty() ? null : results.get(0);
    }
    
    /**
     * Find all entities by ID column value across all tables (full table scan)
     * Useful when ID column is not unique across tables or when searching by non-primary key
     * @param idColumnName The name of the ID column to search by
     * @param value The value to search for
     * @return List of found entities
     */
    public List<SmsEntity> findAllById(String idColumnName, T value) throws SQLException {
        // Get all existing tables
        List<String> existingTables = getExistingTables();
        
        if (existingTables.isEmpty()) {
            return new ArrayList<>();
        }
        
        // Build UNION ALL query across all tables
        StringBuilder queryBuilder = new StringBuilder();
        for (int i = 0; i < existingTables.size(); i++) {
            if (i > 0) {
                queryBuilder.append(" UNION ALL ");
            }
            queryBuilder.append("SELECT * FROM ").append(existingTables.get(i))
                       .append(" WHERE ").append(idColumnName).append(" = ");
            
            // Format value based on type
            if (value instanceof String) {
                queryBuilder.append("'").append(value).append("'");
            } else {
                queryBuilder.append(value);
            }
        }
        
        return executeQuery(queryBuilder.toString(), null, this::mapSmsEntity);
    }
    
    /**
     * Count SMS in date range
     */
    public long countByDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        String query = QueryDSL.select()
            .count("*", "total_count")
            .from(tablePrefix)
            .where(w -> w.dateRange("created_at", startDate, endDate))
            .buildPartitioned(database, startDate, endDate);
        
        List<Long> results = executeQuery(query, null, 
            rs -> rs.getLong("total_count"));
        
        return results.isEmpty() ? 0 : results.get(0);
    }
    
    /**
     * Get user SMS statistics using 2-level aggregation
     */
    public List<UserSmsStats> getUserStats(LocalDateTime startDate, LocalDateTime endDate, int limit) throws SQLException {
        String query = QueryDSL.select()
            .column("user_id")
            .count("*", "message_count")
            .min("created_at", "first_message")
            .max("created_at", "last_message")
            .sum("LENGTH(message)", "total_chars")
            .avg("LENGTH(message)", "avg_message_length")
            .sum("cost", "total_cost")
            .from(tablePrefix)
            .where(w -> w
                .dateRange("created_at", startDate, endDate)
                .isNotNull("user_id"))
            .groupBy("user_id")
            .orderByDesc("message_count")
            .limit(limit)
            .buildPartitioned(database, startDate, endDate);
        
        return executeQuery(query, null, rs -> new UserSmsStats(
            rs.getString("user_id"),
            rs.getLong("message_count"),
            rs.getTimestamp("first_message").toLocalDateTime(),
            rs.getTimestamp("last_message").toLocalDateTime(),
            rs.getLong("total_chars"),
            rs.getDouble("avg_message_length"),
            rs.getBigDecimal("total_cost")
        ));
    }
    
    /**
     * Get hourly SMS statistics across partitions
     */
    public List<HourlyStats> getHourlyStats(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        String query = QueryDSL.select()
            .column("DATE_FORMAT(created_at, '%Y-%m-%d %H:00')", "hour")
            .count("*", "message_count")
            .aggregate(QueryDSL.AggregateFunction.COUNT, "user_id", "unique_users", true)
            .sum("CASE WHEN status = 'SENT' THEN 1 ELSE 0 END", "sent_count")
            .sum("CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END", "failed_count")
            .sum("cost", "total_cost")
            .from(tablePrefix)
            .where(w -> w.dateRange("created_at", startDate, endDate))
            .groupBy("DATE_FORMAT(created_at, '%Y-%m-%d %H:00')")
            .orderByDesc("hour")
            .limit(24)
            .buildPartitioned(database, startDate, endDate);
        
        return executeQuery(query, null, rs -> new HourlyStats(
            rs.getString("hour"),
            rs.getLong("message_count"),
            rs.getLong("unique_users"),
            rs.getLong("sent_count"),  
            rs.getLong("failed_count"),
            rs.getBigDecimal("total_cost")
        ));
    }
    
    /**
     * Create tables for date range
     */
    public void createTablesForDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        LocalDateTime current = startDate;
        while (!current.isAfter(endDate)) {
            createTableIfNotExists(getTableName(current));
            current = current.plusDays(1);
        }
    }
    
    /**
     * Drop old tables outside retention window  
     */
    public void dropOldTables(LocalDateTime cutoffDate) throws SQLException {
        if (!autoManagePartitions) {
            return; // Skip dropping if auto-management is disabled
        }
        String pattern = tablePrefix + "_%";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT table_name FROM information_schema.tables " +
                 "WHERE table_schema = ? AND table_name LIKE ?")) {
            
            stmt.setString(1, database);
            stmt.setString(2, pattern);
            
            List<String> tablesToDrop = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    if (isTableOlderThan(tableName, cutoffDate)) {
                        tablesToDrop.add(tableName);
                    }
                }
            }
            
            // Drop old tables
            for (String tableName : tablesToDrop) {
                try (Statement dropStmt = conn.createStatement()) {
                    dropStmt.execute("DROP TABLE IF EXISTS " + database + "." + tableName);
                    System.out.println("Dropped old table: " + tableName);
                }
            }
        }
    }
    
    /**
     * Clean up old tables based on retention period
     */
    public void cleanupOldTables() throws SQLException {
        if (autoManagePartitions) {
            LocalDateTime cutoffDate = LocalDateTime.now().minusDays(partitionRetentionPeriod);
            dropOldTables(cutoffDate);
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
    
    public String getTablePrefix() {
        return tablePrefix;
    }
    
    public LocalTime getPartitionAdjustmentTime() {
        return partitionAdjustmentTime;
    }
    
    // Private helper methods
    
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
     * Get list of all existing tables with the configured prefix
     */
    private List<String> getExistingTables() throws SQLException {
        List<String> tables = new ArrayList<>();
        String pattern = tablePrefix + "_%";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT table_name FROM information_schema.tables " +
                 "WHERE table_schema = ? AND table_name LIKE ? ORDER BY table_name")) {
            
            stmt.setString(1, database);
            stmt.setString(2, pattern);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    tables.add(database + "." + tableName);
                }
            }
        }
        
        return tables;
    }
    
    /**
     * Automatically ensure table exists for the given date
     * Also performs maintenance if auto-management is enabled
     */
    private void ensureTableExistsForDate(LocalDateTime date) throws SQLException {
        String tableName = getTableName(date);
        createTableIfNotExists(tableName);
        
        // Perform automatic maintenance if enabled
        if (autoManagePartitions) {
            performAutomaticMaintenance(date);
        }
    }
    
    /**
     * Perform automatic maintenance tasks
     */
    private void performAutomaticMaintenance(LocalDateTime currentDate) throws SQLException {
        // Create tables for near future dates (next few days)
        LocalDateTime futureDate = currentDate.plusDays(3);
        LocalDateTime current = currentDate;
        while (!current.isAfter(futureDate)) {
            createTableIfNotExists(getTableName(current));
            current = current.plusDays(1);
        }
        
        // Clean up old tables based on retention period
        LocalDateTime cutoffDate = currentDate.minusDays(partitionRetentionPeriod);
        dropOldTables(cutoffDate);
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
        
        System.out.println(" MultiTable Scheduler: Next maintenance at " + nextRun + 
                          " (in " + initialDelayMinutes + " minutes)");
        
        scheduler.scheduleAtFixedRate(() -> {
            try {
                performScheduledMaintenance();
            } catch (Exception e) {
                System.err.println(" MultiTable scheduled maintenance failed: " + e.getMessage());
                e.printStackTrace();
            }
        }, initialDelayMinutes, 24 * 60, TimeUnit.MINUTES); // Run daily
    }
    
    /**
     * Perform scheduled maintenance - creates/drops tables based on retention period
     */
    private void performScheduledMaintenance() throws SQLException {
        LocalDateTime today = LocalDateTime.now();
        
        System.out.println(" MultiTable scheduled maintenance started at " + today);
        
        // Calculate the valid range: {today - retentionDays} to {today + retentionDays}
        LocalDateTime startRange = today.minusDays(partitionRetentionPeriod);
        LocalDateTime endRange = today.plusDays(partitionRetentionPeriod);
        
        System.out.println(" Valid table range: " + startRange.toLocalDate() + " to " + endRange.toLocalDate());
        
        // Create missing tables in the valid range
        createTablesForDateRange(startRange, endRange);
        
        // Drop tables outside the valid range (older than startRange)
        dropOldTables(startRange);
        
        System.out.println(" MultiTable scheduled maintenance completed");
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
    
    private String getTableName(LocalDateTime date) {
        return database + "." + tablePrefix + "_" + date.format(DATE_FORMAT);
    }
    
    private void createTableIfNotExists(String tableName) throws SQLException {
        String createSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "user_id VARCHAR(50), " +
            "phone_number VARCHAR(20), " +
            "message TEXT, " +
            "status VARCHAR(20), " +
            "created_at DATETIME NOT NULL, " +
            "delivered_at DATETIME, " +
            "cost DECIMAL(10,4), " +
            "provider VARCHAR(50), " +
            "INDEX idx_created_at (created_at), " +
            "INDEX idx_user_id (user_id), " +
            "INDEX idx_status (status)" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(createSQL);
        }
    }
    
    private boolean isTableOlderThan(String tableName, LocalDateTime cutoffDate) {
        String[] parts = tableName.split("_");
        if (parts.length < 2) return false;
        
        try {
            String dateStr = parts[parts.length - 1];
            LocalDateTime tableDate = LocalDateTime.parse(dateStr + "000000", 
                DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            return tableDate.isBefore(cutoffDate);
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
    
    private SmsEntity mapSmsEntity(ResultSet rs) throws SQLException {
        SmsEntity sms = new SmsEntity();
        sms.setId(String.valueOf(rs.getLong("id")));
        sms.setUserId(rs.getString("user_id"));
        sms.setPhoneNumber(rs.getString("phone_number"));
        sms.setMessage(rs.getString("message"));
        sms.setStatus(rs.getString("status"));
        sms.setPartitionColValue(rs.getTimestamp("created_at").toLocalDateTime());
        
        Timestamp delivered = rs.getTimestamp("delivered_at");
        sms.setDeliveredAt(delivered != null ? delivered.toLocalDateTime() : null);
        
        sms.setCost(rs.getBigDecimal("cost"));
        sms.setProvider(rs.getString("provider"));
        
        return sms;
    }
    
    // Result classes
    public static class UserSmsStats {
        public final String userId;
        public final long messageCount;
        public final LocalDateTime firstMessage;
        public final LocalDateTime lastMessage;
        public final long totalChars;
        public final double avgMessageLength;
        public final BigDecimal totalCost;
        
        public UserSmsStats(String userId, long messageCount, LocalDateTime firstMessage,
                           LocalDateTime lastMessage, long totalChars, double avgMessageLength,
                           BigDecimal totalCost) {
            this.userId = userId;
            this.messageCount = messageCount;
            this.firstMessage = firstMessage;
            this.lastMessage = lastMessage;
            this.totalChars = totalChars;
            this.avgMessageLength = avgMessageLength;
            this.totalCost = totalCost;
        }
        
        @Override
        public String toString() {
            return String.format("User: %s | Messages: %d | Cost: $%s | Avg Length: %.1f chars",
                userId, messageCount, totalCost, avgMessageLength);
        }
    }
    
    public static class HourlyStats {
        public final String hour;
        public final long messageCount;
        public final long uniqueUsers;
        public final long sentCount;
        public final long failedCount;
        public final BigDecimal totalCost;
        public final double successRate;
        
        public HourlyStats(String hour, long messageCount, long uniqueUsers,
                          long sentCount, long failedCount, BigDecimal totalCost) {
            this.hour = hour;
            this.messageCount = messageCount;
            this.uniqueUsers = uniqueUsers;
            this.sentCount = sentCount;
            this.failedCount = failedCount;
            this.totalCost = totalCost;
            this.successRate = messageCount > 0 ? (double) sentCount / messageCount * 100 : 0;
        }
        
        @Override
        public String toString() {
            return String.format("Hour: %s | Messages: %d | Users: %d | Success: %.1f%% | Cost: $%s",
                hour, messageCount, uniqueUsers, successRate, totalCost);
        }
    }
    
    @FunctionalInterface
    public interface ResultMapper<R> {
        R map(ResultSet rs) throws SQLException;
    }
}
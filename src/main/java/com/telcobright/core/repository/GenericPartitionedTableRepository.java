package com.telcobright.core.repository;
import com.telcobright.api.ShardingRepository;

import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.metadata.EntityMetadata;
import com.telcobright.core.monitoring.*;
import com.telcobright.core.pagination.Page;
import com.telcobright.core.pagination.PageRequest;
import com.telcobright.core.query.QueryDSL;
import com.telcobright.core.connection.ConnectionProvider;
import com.telcobright.core.connection.ConnectionProvider.MaintenanceConnection;
import com.telcobright.core.partition.PartitionType;
import com.telcobright.core.partition.PartitionStrategy;
import com.telcobright.core.partition.PartitionStrategyFactory;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.telcobright.core.logging.Logger;
import com.telcobright.core.logging.ConsoleLogger;

/**
 * Generic Partitioned Table Repository implementation
 * Uses MySQL native partitioning on a single table
 * 
 * Entities must implement ShardingEntity to ensure they have
 * required getId/setId and getCreatedAt/setCreatedAt methods.
 * 
 * @param <T> Entity type that implements ShardingEntity
 */
public class GenericPartitionedTableRepository<T extends ShardingEntity> implements ShardingRepository<T> {
    private final Logger logger;
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    private final ConnectionProvider connectionProvider;
    private final String database;
    private final String tableName;
    private final int partitionRetentionPeriod;
    private final boolean autoManagePartitions;
    private final LocalTime partitionAdjustmentTime;
    private final boolean initializePartitionsOnStart;
    private final EntityMetadata<T> metadata;
    private final Class<T> entityClass;
    private final MonitoringService monitoringService;
    private final String charset;
    private final String collation;
    
    private ScheduledExecutorService scheduler;
    
    private GenericPartitionedTableRepository(Builder<T> builder) {
        this.database = builder.database;
        this.partitionRetentionPeriod = builder.partitionRetentionPeriod;
        this.autoManagePartitions = builder.autoManagePartitions;
        this.partitionAdjustmentTime = builder.partitionAdjustmentTime;
        this.initializePartitionsOnStart = builder.initializePartitionsOnStart;
        this.entityClass = builder.entityClass;
        this.charset = builder.charset;
        this.collation = builder.collation;
        
        // Initialize entity metadata (performs reflection once)
        this.metadata = new EntityMetadata<>(entityClass);
        
        // Use provided table name or derive from entity
        this.tableName = builder.tableName != null ? builder.tableName : metadata.getTableName();
        
        // Initialize logger
        this.logger = builder.logger != null ? builder.logger : 
            new ConsoleLogger("PartitionedRepo." + tableName);
        
        // Create ConnectionProvider
        this.connectionProvider = new ConnectionProvider.Builder()
            .host(builder.host)
            .port(builder.port)
            .database(builder.database)
            .username(builder.username)
            .password(builder.password)
            .build();
        
        // Initialize monitoring if enabled
        if (builder.monitoringConfig != null && builder.monitoringConfig.isEnabled()) {
            RepositoryMetrics metrics = new RepositoryMetrics("Partitioned", tableName, 
                    builder.monitoringConfig.getInstanceId());
            MetricsCollector metricsCollector = new MetricsCollector(connectionProvider, database);
            this.monitoringService = new DefaultMonitoringService(builder.monitoringConfig, metrics, metricsCollector);
            this.monitoringService.start();
        } else {
            this.monitoringService = null;
        }
        
        // Initialize table and partitions if needed
        // Initialize table and partitions for retention period on startup
        if (initializePartitionsOnStart) {
            try {
                logger.info("Initializing partitioned table and partitions for retention period...");
                initializeTable();
                // Partitions are now created in initializeTable(), no need for separate initialization
            } catch (SQLException e) {
                logger.error("Failed to initialize partitioned table: " + e.getMessage(), e);
                // Try to create table without partitions as fallback
                try {
                    createSimpleTable();
                    logger.warn("Created non-partitioned table as fallback. Performance may be impacted.");
                } catch (SQLException fallbackError) {
                    throw new RuntimeException("Failed to initialize table even without partitions", fallbackError);
                }
            }
        }
        
        // Start scheduler if auto-management is enabled
        if (autoManagePartitions) {
            startScheduler();
        }
    }
    
    /**
     * Insert entity into partitioned table (MySQL handles routing)
     * Note: Target partition must exist (created during startup), otherwise SQLException will be thrown
     */
    @Override
    public void insert(T entity) throws SQLException {
        String fullTableName = database + "." + tableName;
        String sql = String.format(metadata.getInsertSQL(), fullTableName);
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            
            metadata.setInsertParameters(stmt, entity);
            stmt.executeUpdate();
            
            // Set generated ID if applicable
            if (metadata.getIdField() != null && metadata.getIdField().isAutoGenerated()) {
                try (ResultSet keys = stmt.getGeneratedKeys()) {
                    // Auto-generated IDs not supported - all IDs must be externally generated strings
                }
            }
        }
    }
    
    /**
     * Insert multiple entities
     * Note: Target partitions must exist (created during startup), otherwise SQLException will be thrown
     */
    @Override
    public void insertMultiple(List<T> entities) throws SQLException {
        if (entities == null || entities.isEmpty()) {
            return;
        }
        
        String fullTableName = database + "." + tableName;
        String sql = String.format(metadata.getInsertSQL(), fullTableName);
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            
            for (T entity : entities) {
                metadata.setInsertParameters(stmt, entity);
                stmt.addBatch();
            }
            
            stmt.executeBatch();
            
            // Set generated IDs if applicable
            if (metadata.getIdField() != null && metadata.getIdField().isAutoGenerated()) {
                try (ResultSet keys = stmt.getGeneratedKeys()) {
                    for (T entity : entities) {
                        // Auto-generated IDs not supported - all IDs must be externally generated strings
                    }
                }
            }
        }
    }
    
    /**
     * Find all entities by date range (with partition pruning)
     */
    @Override
    public List<T> findAllByDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        String fullTableName = database + "." + tableName;
        
        String sql = String.format("SELECT * FROM %s WHERE %s BETWEEN ? AND ?", 
            fullTableName, shardingColumn);
        
        List<T> results = new ArrayList<>();
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setTimestamp(1, Timestamp.valueOf(startDate));
            stmt.setTimestamp(2, Timestamp.valueOf(endDate));
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(metadata.mapResultSet(rs));
                }
            }
        }
        
        return results;
    }
    
    /**
     * Find entity by ID (MySQL scans all partitions)
     */
    @Override
    public T findById(String id) throws SQLException {
        String fullTableName = database + "." + tableName;
        String sql = String.format(metadata.getSelectByIdSQL(), fullTableName);
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            setIdParameter(stmt, 1, id);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return metadata.mapResultSet(rs);
                }
            }
        }
        
        return null;
    }
    
    /**
     * Find entity by ID within a date range
     */
    @Override
    public T findByIdAndDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        // Returns the first entity found in the date range
        List<T> entities = findAllByDateRange(startDate, endDate);
        return entities.isEmpty() ? null : entities.get(0);
    }
    
    /**
     * Find all entities by IDs within a date range
     */
    @Override
    public List<T> findAllByIdsAndDateRange(List<String> ids, LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        
        String idColumn = metadata.getIdField().getColumnName();
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        String fullTableName = database + "." + tableName;
        
        // Create IN clause with placeholders
        String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
        String sql = String.format("SELECT * FROM %s WHERE %s IN (%s) AND %s >= ? AND %s <= ?", 
                                 fullTableName, idColumn, placeholders, shardingColumn, shardingColumn);
        
        List<T> results = new ArrayList<>();
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            // Set ID parameters
            int paramIndex = 1;
            for (String id : ids) {
                setIdParameter(stmt, paramIndex++, id);
            }
            
            // Set date range parameters
            stmt.setTimestamp(paramIndex++, Timestamp.valueOf(startDate));
            stmt.setTimestamp(paramIndex, Timestamp.valueOf(endDate));
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(metadata.mapResultSet(rs));
                }
            }
        }
        
        return results;
    }
    
    /**
     * Find all entities before a specific date
     */
    @Override
    public List<T> findAllBeforeDate(LocalDateTime beforeDate) throws SQLException {
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        String fullTableName = database + "." + tableName;
        
        String sql = String.format("SELECT * FROM %s WHERE %s < ?", fullTableName, shardingColumn);
        
        List<T> results = new ArrayList<>();
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setTimestamp(1, Timestamp.valueOf(beforeDate));
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(metadata.mapResultSet(rs));
                }
            }
        }
        
        return results;
    }
    
    /**
     * Find all entities after a specific date
     */
    @Override
    public List<T> findAllAfterDate(LocalDateTime afterDate) throws SQLException {
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        String fullTableName = database + "." + tableName;
        
        String sql = String.format("SELECT * FROM %s WHERE %s > ?", fullTableName, shardingColumn);
        
        List<T> results = new ArrayList<>();
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setTimestamp(1, Timestamp.valueOf(afterDate));
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(metadata.mapResultSet(rs));
                }
            }
        }
        
        return results;
    }
    
    /**
     * Update entity by primary key in partitioned table
     */
    @Override
    public void updateById(String id, T entity) throws SQLException {
        String fullTableName = database + "." + tableName;
        String sql = String.format(metadata.getUpdateByIdSQL(), fullTableName);
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            metadata.setUpdateParameters(stmt, entity, id);
            
            int rowsUpdated = stmt.executeUpdate();
            if (rowsUpdated == 0) {
                throw new SQLException("Entity with ID " + id + " not found");
            }
        }
    }
    
    /**
     * Update entity by primary key within a specific date range
     */
    @Override
    public void updateByIdAndDateRange(String id, T entity, LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        String fullTableName = database + "." + tableName;
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        
        // Add date range to the WHERE clause for partition pruning
        String sql = String.format(metadata.getUpdateByIdSQL() + " AND %s >= ? AND %s <= ?",
                                 fullTableName, shardingColumn, shardingColumn);
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            metadata.setUpdateParameters(stmt, entity, id);
            
            // Add date range parameters
            int paramCount = stmt.getParameterMetaData().getParameterCount();
            stmt.setTimestamp(paramCount - 1, Timestamp.valueOf(startDate));
            stmt.setTimestamp(paramCount, Timestamp.valueOf(endDate));
            
            int rowsUpdated = stmt.executeUpdate();
            if (rowsUpdated == 0) {
                throw new SQLException("Entity with ID " + id + " not found in date range " + 
                                     startDate + " to " + endDate);
            }
        }
    }
    
    /**
     * Find one entity with ID greater than the specified ID
     * Scans the full partitioned table across all partitions
     */
    @Override
    public T findOneByIdGreaterThan(String id) throws SQLException {
        String fullTableName = database + "." + tableName;
        String idColumn = metadata.getIdField().getColumnName();
        
        // Build query to find one entity with ID > specified ID
        // ORDER BY id ASC to get the smallest ID that is greater than the specified ID
        String sql = String.format(
            "SELECT * FROM %s WHERE %s > ? ORDER BY %s ASC LIMIT 1",
            fullTableName, idColumn, idColumn
        );
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            setIdParameter(stmt, 1, id);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return metadata.mapResultSet(rs);
                }
            }
        }
        
        return null; // No entity found with ID greater than specified ID
    }
    
    /**
     * Find batch of entities with ID greater than the specified ID
     * Executes single query with LIMIT across all partitions
     */
    @Override
    public List<T> findBatchByIdGreaterThan(String id, int batchSize) throws SQLException {
        String fullTableName = database + "." + tableName;
        String idColumn = metadata.getIdField().getColumnName();
        List<T> results = new ArrayList<>();
        
        // Build query to find batch of entities with ID > specified ID
        // ORDER BY id ASC to get results in increasing ID order
        String sql = String.format(
            "SELECT * FROM %s WHERE %s > ? ORDER BY %s ASC LIMIT ?",
            fullTableName, idColumn, idColumn
        );
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            setIdParameter(stmt, 1, id);
            stmt.setInt(2, batchSize);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(metadata.mapResultSet(rs));
                }
            }
        }
        
        return results;
    }
    
    private void initializeTable() throws SQLException {
        String fullTableName = database + "." + tableName;
        String createSQL = String.format(metadata.getCreateTableSQL(), fullTableName);

        // For partitioned tables, MySQL requires the partitioning column to be part of PRIMARY KEY
        // Modify PRIMARY KEY to include sharding column
        String idColumn = metadata.getIdField().getColumnName();
        String shardingColumn = metadata.getShardingKeyField().getColumnName();

        // Handle both VARCHAR and BIGINT primary keys
        createSQL = createSQL.replace(idColumn + " VARCHAR(255) PRIMARY KEY",
                                     idColumn + " VARCHAR(255)");
        createSQL = createSQL.replace(idColumn + " BIGINT PRIMARY KEY AUTO_INCREMENT",
                                     idColumn + " BIGINT AUTO_INCREMENT");

        // Add composite primary key with both ID and sharding column
        createSQL = createSQL.replace(", KEY idx_" + shardingColumn,
                                     ", PRIMARY KEY (" + idColumn + ", " + shardingColumn + "), KEY idx_" + shardingColumn);

        // Replace charset/collation with configured values
        createSQL = createSQL.replace(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
                ") ENGINE=InnoDB DEFAULT CHARSET=" + charset + " COLLATE=" + collation);

        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {

            // Check if table exists
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getTables(database, null, tableName, null)) {
                if (!rs.next()) {
                    // Table doesn't exist, create it with ALL partitions for the retention period
                    LocalDateTime now = LocalDateTime.now();
                    LocalDateTime startDate = now.minusDays(partitionRetentionPeriod);
                    LocalDateTime endDate = now.plusDays(partitionRetentionPeriod);

                    // Build partition clause with single initial partition
                    // Use TO_DAYS function for simpler partition definition
                    StringBuilder partitionClause = new StringBuilder();
                    partitionClause.append("\nPARTITION BY RANGE (TO_DAYS(").append(shardingColumn).append("))\n(");

                    // Create just one partition for today
                    // Additional partitions will be created on demand
                    LocalDateTime tomorrow = LocalDateTime.now().plusDays(1);
                    String partitionName = "p" + LocalDateTime.now().format(DATE_FORMAT);

                    partitionClause.append("\n  PARTITION ").append(partitionName)
                                  .append(" VALUES LESS THAN (TO_DAYS('")
                                  .append(tomorrow.toLocalDate().toString())
                                  .append("'))");

                    partitionClause.append("\n)");
                    createSQL += partitionClause.toString();

                    logger.info("Creating partitioned table with " + (partitionRetentionPeriod * 2 + 1) + " partitions");
                    logger.info("Executing CREATE TABLE SQL: " + createSQL);
                    stmt.execute(createSQL);
                    logger.info("Created partitioned table: " + fullTableName + " with partitions from " +
                               startDate.toLocalDate() + " to " + endDate.toLocalDate());
                }
            }
        }
    }

    /**
     * Create a simple non-partitioned table as fallback
     */
    private void createSimpleTable() throws SQLException {
        String fullTableName = database + "." + tableName;
        String createSQL = String.format(metadata.getCreateTableSQL(), fullTableName);

        // For non-partitioned tables, keep the simple PRIMARY KEY
        String idColumn = metadata.getIdField().getColumnName();
        String shardingColumn = metadata.getShardingKeyField().getColumnName();

        // Keep original PRIMARY KEY for non-partitioned table
        // Just add index on sharding column for query performance
        if (!createSQL.contains("idx_" + shardingColumn)) {
            createSQL = createSQL.replace(") ENGINE=InnoDB",
                ", INDEX idx_" + shardingColumn + " (" + shardingColumn + ")) ENGINE=InnoDB");
        }

        // Replace charset/collation with configured values
        createSQL = createSQL.replace("DEFAULT CHARSET=utf8mb4",
                "DEFAULT CHARSET=" + charset + " COLLATE=" + collation);

        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {

            // Check if table exists
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getTables(database, null, tableName, null)) {
                if (!rs.next()) {
                    logger.info("Creating non-partitioned table: " + fullTableName);
                    stmt.execute(createSQL);
                    logger.info("Created non-partitioned table: " + fullTableName);
                }
            }
        }
    }


    private boolean partitionExists(String partitionName) throws SQLException {
        String sql = "SELECT partition_name FROM information_schema.partitions " +
                    "WHERE table_schema = ? AND table_name = ? AND partition_name = ?";
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, database);
            stmt.setString(2, tableName);
            stmt.setString(3, partitionName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }
    
    private void createPartition(String partitionName, LocalDateTime date) throws SQLException {
        // Create single partition - kept for backward compatibility
        Map<String, LocalDateTime> singlePartition = new LinkedHashMap<>();
        singlePartition.put(partitionName, date);
        createMultiplePartitions(singlePartition);
    }

    private void createMultiplePartitions(Map<String, LocalDateTime> partitions) throws SQLException {
        if (partitions.isEmpty()) {
            return;
        }

        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(database).append(".").append(tableName)
           .append(" ADD PARTITION (");

        boolean first = true;
        for (Map.Entry<String, LocalDateTime> entry : partitions.entrySet()) {
            String partitionName = entry.getKey();
            LocalDateTime date = entry.getValue();
            LocalDateTime nextDay = date.plusDays(1);

            if (!first) {
                sql.append(",");
            }
            sql.append("\n    PARTITION ").append(partitionName)
               .append(" VALUES LESS THAN (TO_DAYS('")
               .append(nextDay.toLocalDate().toString()).append("'))");
            first = false;
        }
        sql.append("\n)");

        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            logger.info("Creating " + partitions.size() + " partitions in single ALTER TABLE command");
            stmt.execute(sql.toString());
            logger.info("Successfully created " + partitions.size() + " partitions: " + partitions.keySet());
        } catch (SQLException e) {
            logger.error("Failed to create partitions. SQL: " + sql.toString(), e);
            throw e;
        }
    }

    private void dropPartition(String partitionName) throws SQLException {
        // Drop single partition - kept for backward compatibility
        dropMultiplePartitions(Arrays.asList(partitionName));
    }

    private void dropMultiplePartitions(List<String> partitionNames) throws SQLException {
        if (partitionNames.isEmpty()) {
            return;
        }

        String partitionList = String.join(", ", partitionNames);
        String sql = String.format("ALTER TABLE %s.%s DROP PARTITION %s",
            database, tableName, partitionList);

        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            logger.info("Dropping " + partitionNames.size() + " partitions in single ALTER TABLE command");
            stmt.execute(sql);
            logger.info("Successfully dropped partitions: " + partitionNames);
        } catch (SQLException e) {
            logger.error("Failed to drop partitions. SQL: " + sql, e);
            throw e;
        }
    }
    
    private List<String> getPartitions() throws SQLException {
        String sql = "SELECT partition_name FROM information_schema.partitions " +
                    "WHERE table_schema = ? AND table_name = ? AND partition_name IS NOT NULL " +
                    "ORDER BY partition_ordinal_position";
        
        List<String> partitions = new ArrayList<>();
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
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
    
    private void setIdParameter(PreparedStatement stmt, int index, String id) throws SQLException {
        stmt.setString(index, id);
    }
    
    private <R> List<R> executeQuery(String sql, Map<String, Object> parameters, 
                                    ResultSetMapper<R> mapper) throws SQLException {
        List<R> results = new ArrayList<>();
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            if (parameters != null) {
                int index = 1;
                for (Object value : parameters.values()) {
                    stmt.setObject(index++, value);
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
    
    private void performAutomaticMaintenance(LocalDateTime referenceDate) throws SQLException {
        // Use MaintenanceConnection to get exclusive access during maintenance
        try (MaintenanceConnection maintenanceConn = connectionProvider.getMaintenanceConnection(
                "Automatic partition maintenance for " + tableName)) {
            
            logger.info("Starting automatic partition maintenance for " + tableName);
            
            LocalDateTime startDate = referenceDate.minusDays(partitionRetentionPeriod);
            LocalDateTime endDate = referenceDate.plusDays(partitionRetentionPeriod);
            
            createPartitionsForDateRange(startDate, endDate);
            
            LocalDateTime cutoffDate = referenceDate.minusDays(partitionRetentionPeriod);
            dropOldPartitions(cutoffDate);
            
            logger.info("Partition maintenance completed for " + tableName);
        }
    }
    
    public void createPartitionsForDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        // Collect all partitions that need to be created
        Map<String, LocalDateTime> partitionsToCreate = new LinkedHashMap<>();
        LocalDateTime current = startDate;

        while (!current.isAfter(endDate)) {
            String partitionName = "p" + current.format(DATE_FORMAT);
            if (!partitionExists(partitionName)) {
                partitionsToCreate.put(partitionName, current);
            }
            current = current.plusDays(1);
        }

        // Create all partitions in a single ALTER TABLE command
        if (!partitionsToCreate.isEmpty()) {
            createMultiplePartitions(partitionsToCreate);
        }
    }
    
    public void dropOldPartitions(LocalDateTime cutoffDate) throws SQLException {
        if (!autoManagePartitions) {
            return;
        }

        String cutoffDateStr = cutoffDate.format(DATE_FORMAT);
        List<String> partitions = getPartitions();
        List<String> partitionsToDrop = new ArrayList<>();

        for (String partitionName : partitions) {
            if (partitionName.startsWith("p") && partitionName.length() > 1) {
                String dateStr = partitionName.substring(1);
                if (dateStr.compareTo(cutoffDateStr) < 0) {
                    partitionsToDrop.add(partitionName);
                }
            }
        }

        // Drop all old partitions in a single ALTER TABLE command
        if (!partitionsToDrop.isEmpty()) {
            dropMultiplePartitions(partitionsToDrop);
        }
    }
    
    /**
     * Initialize all partitions needed for the retention period at startup.
     * NOTE: This is now done in initializeTable() to avoid partition ordering issues.
     * Kept for backward compatibility but does nothing.
     */
    private void initializePartitionsForRetentionPeriod() throws SQLException {
        // Partitions are now created during table creation in initializeTable()
        // This method is kept for backward compatibility but does nothing
        logger.info("Partition initialization is now handled during table creation");
    }
    
    private void startScheduler() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "PartitionedTableRepository-Scheduler-" + tableName);
            thread.setDaemon(true);
            return thread;
        });
        
        scheduleNextRun();
    }
    
    private void scheduleNextRun() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nextRun = now.toLocalDate().plusDays(1).atTime(partitionAdjustmentTime);
        
        if (nextRun.isBefore(now)) {
            nextRun = nextRun.plusDays(1);
        }
        
        long delay = java.time.Duration.between(now, nextRun).toMillis();
        
        scheduler.schedule(() -> {
            try {
                performScheduledMaintenance();
                scheduleNextRun();
            } catch (Exception e) {
                logger.error("Failed to perform scheduled maintenance: " + e.getMessage());
                scheduleNextRun();
            }
        }, delay, TimeUnit.MILLISECONDS);
    }
    
    private void performScheduledMaintenance() throws SQLException {
        LocalDateTime now = LocalDateTime.now();
        performAutomaticMaintenance(now);
    }
    
    @Override
    public void shutdown() {
        // Shutdown scheduler first
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        // Shutdown ConnectionProvider
        if (connectionProvider != null) {
            connectionProvider.shutdown();
            logger.info("ConnectionProvider shutdown");
        }
    }
    
    
    /**
     * Functional interface for mapping ResultSet to entity
     */
    @FunctionalInterface
    public interface ResultSetMapper<R> {
        R map(ResultSet rs) throws SQLException;
    }
    
    /**
     * Builder for GenericPartitionedTableRepository
     */
    static class Builder<T extends ShardingEntity> {
        private final Class<T> entityClass;
        private String host = "localhost";
        private int port = 3306;
        private String database;
        private String username;
        private String password;
        private String tableName;
        private int partitionRetentionPeriod = 7;
        private boolean autoManagePartitions = true;
        private LocalTime partitionAdjustmentTime = LocalTime.of(4, 0);
        private boolean initializePartitionsOnStart = true;
        private MonitoringConfig monitoringConfig;
        private Logger logger;
        private PartitionType partitionType = PartitionType.DATE_BASED;
        private String partitionKeyColumn = "created_at";
        private String charset = "utf8mb4";
        private String collation = "utf8mb4_bin";
        
        
        Builder(Class<T> entityClass) {
            this.entityClass = entityClass;
        }
        
        public Builder<T> host(String host) {
            this.host = host;
            return this;
        }
        
        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }
        
        public Builder<T> database(String database) {
            this.database = database;
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
        
        public Builder<T> tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }
        
        public Builder<T> partitionRetentionPeriod(int days) {
            this.partitionRetentionPeriod = days;
            return this;
        }
        
        public Builder<T> autoManagePartitions(boolean enable) {
            this.autoManagePartitions = enable;
            return this;
        }
        
        public Builder<T> partitionAdjustmentTime(int hour, int minute) {
            this.partitionAdjustmentTime = LocalTime.of(hour, minute);
            return this;
        }
        
        public Builder<T> partitionAdjustmentTime(LocalTime time) {
            this.partitionAdjustmentTime = time;
            return this;
        }
        
        public Builder<T> initializePartitionsOnStart(boolean initialize) {
            this.initializePartitionsOnStart = initialize;
            return this;
        }
        
        public Builder<T> withPartitionType(PartitionType partitionType) {
            if (partitionType != null) {
                partitionType.validateSupported();
                this.partitionType = partitionType;
            }
            return this;
        }
        
        public Builder<T> withPartitionKeyColumn(String partitionKeyColumn) {
            if (partitionKeyColumn != null && !partitionKeyColumn.trim().isEmpty()) {
                this.partitionKeyColumn = partitionKeyColumn;
            }
            return this;
        }

        public Builder<T> charset(String charset) {
            if (charset != null && !charset.trim().isEmpty()) {
                this.charset = charset;
            }
            return this;
        }

        public Builder<T> collation(String collation) {
            if (collation != null && !collation.trim().isEmpty()) {
                this.collation = collation;
            }
            return this;
        }
        
        public Builder<T> monitoring(MonitoringConfig monitoringConfig) {
            this.monitoringConfig = monitoringConfig;
            return this;
        }
        
        public Builder<T> logger(Logger logger) {
            this.logger = logger;
            return this;
        }
        
        
        public GenericPartitionedTableRepository<T> build() {
            if (database == null || username == null || password == null) {
                throw new IllegalStateException("Database, username, and password are required");
            }
            return new GenericPartitionedTableRepository<>(this);
        }
    }
    
    /**
     * Create a new builder
     */
    // Package-private factory method - only SplitVerseRepository can use this
    static <T extends ShardingEntity> Builder<T> builder(Class<T> entityClass) {
        return new Builder<>(entityClass);
    }
}
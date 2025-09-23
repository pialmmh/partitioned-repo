package com.telcobright.core.repository;

import com.telcobright.api.ShardingRepository;
import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.connection.ConnectionProvider;
import com.telcobright.core.metadata.EntityMetadata;
import com.telcobright.core.metadata.FieldMetadata;
import com.telcobright.core.enums.PartitionRange;
import com.telcobright.core.logging.Logger;
import com.telcobright.core.logging.ConsoleLogger;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simple Sequential Repository with automatic ID management.
 *
 * Features:
 * - Integer/Long based sequential IDs only (no String IDs)
 * - Built-in ID generation with getNextId() and getNextN()
 * - Automatic state management and persistence
 * - Wraparound support when max value exceeded
 * - Date-based partitioning for log rotation
 * - Thread-safe ID generation
 *
 * Use Case: High-performance sequential log storage with guaranteed ordering
 *
 * @param <T> Entity type that implements ShardingEntity<LocalDateTime>
 */
public class SimpleSequentialRepository<T extends ShardingEntity<LocalDateTime>> {

    private final Logger logger;
    private final ShardingRepository<T, LocalDateTime> delegateRepo;
    private final ConnectionProvider connectionProvider;
    private final String database;
    private final String tableName;
    private final String stateTableName;
    private final long maxId;
    private final boolean wrapAround;
    private final boolean allowClientIds;

    // State management
    private final AtomicLong currentId = new AtomicLong(0);
    private final ReentrantLock idLock = new ReentrantLock();
    private volatile long lastPersistedId = 0;
    private static final long PERSIST_THRESHOLD = 1000; // Persist state every 1000 IDs

    private SimpleSequentialRepository(Builder<T> builder) {
        this.database = builder.database;
        this.tableName = builder.tableName;
        this.stateTableName = tableName + "_seq_state";
        this.maxId = builder.maxId;
        this.wrapAround = builder.wrapAround;
        this.allowClientIds = builder.allowClientIds;

        String loggerName = String.format("SimpleSeqRepo.%s", tableName);
        this.logger = new ConsoleLogger(loggerName);

        // Initialize connection provider
        this.connectionProvider = new ConnectionProvider(
            builder.host, builder.port, database,
            builder.username, builder.password
        );

        // Create delegate repository using GenericPartitionedTableRepository
        this.delegateRepo = GenericPartitionedTableRepository.<T, LocalDateTime>builder(builder.entityClass)
            .host(builder.host)
            .port(builder.port)
            .database(database)
            .username(builder.username)
            .password(builder.password)
            .tableName(tableName)
            .partitionRetentionPeriod(builder.retentionDays)
            .autoManagePartitions(builder.autoMaintenance)
            .partitionAdjustmentTime(builder.maintenanceTime)
            .initializePartitionsOnStart(true)
            .charset("utf8mb4")
            .collation("utf8mb4_bin")
            .build();

        logger.info(String.format(
            "Initializing SimpleSequentialRepository: table=%s, maxId=%d, wrapAround=%s",
            tableName, maxId, wrapAround
        ));

        // Initialize state management
        initializeStateManagement();
    }

    private void initializeStateManagement() {
        try (Connection conn = connectionProvider.getConnection()) {
            // Create state table if not exists
            createStateTable(conn);

            // Load last ID from state table
            loadLastId(conn);

            logger.info("State management initialized, starting from ID: " + currentId.get());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize state management: " + e.getMessage(), e);
        }
    }

    private void createStateTable(Connection conn) throws SQLException {
        String sql = String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s (" +
            "  table_name VARCHAR(255) PRIMARY KEY," +
            "  last_id BIGINT NOT NULL DEFAULT 0," +
            "  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            database, stateTableName
        );

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }

        // Insert initial record if not exists
        String insertSql = String.format(
            "INSERT IGNORE INTO %s.%s (table_name, last_id) VALUES (?, 0)",
            database, stateTableName
        );

        try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
            stmt.setString(1, tableName);
            stmt.executeUpdate();
        }
    }

    private void loadLastId(Connection conn) throws SQLException {
        String sql = String.format(
            "SELECT last_id FROM %s.%s WHERE table_name = ?",
            database, stateTableName
        );

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    long lastId = rs.getLong("last_id");
                    currentId.set(lastId);
                    lastPersistedId = lastId;
                }
            }
        }
    }

    private void persistCurrentId() {
        try (Connection conn = connectionProvider.getConnection()) {
            String sql = String.format(
                "UPDATE %s.%s SET last_id = ?, updated_at = CURRENT_TIMESTAMP WHERE table_name = ?",
                database, stateTableName
            );

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                long idToPersist = currentId.get();
                stmt.setLong(1, idToPersist);
                stmt.setString(2, tableName);
                stmt.executeUpdate();
                lastPersistedId = idToPersist;

                logger.debug("Persisted current ID: " + idToPersist);
            }
        } catch (SQLException e) {
            logger.error("Failed to persist current ID: " + e.getMessage(), e);
        }
    }

    /**
     * Get the next sequential ID.
     * Thread-safe and automatically handles wraparound if configured.
     *
     * @return The next sequential ID
     * @throws IllegalStateException if max ID reached and wraparound disabled
     */
    public long getNextId() {
        idLock.lock();
        try {
            long nextId = currentId.incrementAndGet();

            // Check for max ID
            if (nextId > maxId) {
                if (wrapAround) {
                    currentId.set(1);
                    nextId = 1;
                    logger.info("ID wrapped around to 1 (max " + maxId + " reached)");
                } else {
                    currentId.decrementAndGet(); // Revert increment
                    throw new IllegalStateException("Maximum ID " + maxId + " reached");
                }
            }

            // Persist state periodically
            if (nextId - lastPersistedId >= PERSIST_THRESHOLD) {
                persistCurrentId();
            }

            return nextId;
        } finally {
            idLock.unlock();
        }
    }

    /**
     * Get the next N sequential IDs.
     * Thread-safe and automatically handles wraparound if configured.
     *
     * @param count Number of IDs to generate
     * @return List of sequential IDs
     * @throws IllegalArgumentException if count <= 0 or count > maxId
     * @throws IllegalStateException if not enough IDs available and wraparound disabled
     */
    public List<Long> getNextN(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("Count must be positive");
        }
        if (count > maxId) {
            throw new IllegalArgumentException("Count exceeds maximum ID value");
        }

        idLock.lock();
        try {
            List<Long> ids = new ArrayList<>(count);
            long startId = currentId.get();

            for (int i = 0; i < count; i++) {
                long nextId = currentId.incrementAndGet();

                // Check for max ID
                if (nextId > maxId) {
                    if (wrapAround) {
                        currentId.set(1);
                        nextId = 1;
                        logger.info("ID wrapped around to 1 (max " + maxId + " reached)");
                    } else {
                        // Revert all increments
                        currentId.set(startId);
                        throw new IllegalStateException(
                            "Not enough IDs available (need " + count + ", have " + (maxId - startId) + ")"
                        );
                    }
                }

                ids.add(nextId);
            }

            // Persist state after bulk generation
            if (currentId.get() - lastPersistedId >= PERSIST_THRESHOLD) {
                persistCurrentId();
            }

            return ids;
        } finally {
            idLock.unlock();
        }
    }

    /**
     * Get current ID without incrementing.
     *
     * @return Current ID value
     */
    public long getCurrentId() {
        return currentId.get();
    }

    /**
     * Reset ID counter to a specific value.
     * Use with caution - may cause duplicate IDs if not careful.
     *
     * @param newId The new starting ID
     * @throws IllegalArgumentException if newId < 0 or newId > maxId
     */
    public void resetId(long newId) {
        if (newId < 0 || newId > maxId) {
            throw new IllegalArgumentException("Invalid ID: " + newId);
        }

        idLock.lock();
        try {
            currentId.set(newId);
            persistCurrentId();
            logger.info("ID counter reset to: " + newId);
        } finally {
            idLock.unlock();
        }
    }

    /**
     * Insert entity with auto-generated sequential ID.
     * The entity's ID will be set automatically.
     *
     * @param entity Entity to insert (ID will be overwritten)
     * @return The assigned ID
     */
    public long insertWithAutoId(T entity) throws SQLException {
        long id = getNextId();
        entity.setId(String.valueOf(id));
        delegateRepo.insert(entity);
        return id;
    }

    /**
     * Insert entity with client-provided ID.
     * Only works if allowClientIds is enabled in builder.
     * Updates internal counter if provided ID is higher than current.
     *
     * @param entity Entity to insert
     * @param clientId Client-provided ID
     * @throws IllegalStateException if allowClientIds is false
     * @throws IllegalArgumentException if ID is invalid (negative or > maxId)
     */
    public void insertWithClientId(T entity, long clientId) throws SQLException {
        if (!allowClientIds) {
            throw new IllegalStateException("Client IDs not allowed. Enable with allowClientIds(true) in builder");
        }

        if (clientId <= 0 || clientId > maxId) {
            throw new IllegalArgumentException("Invalid client ID: " + clientId + " (must be 1-" + maxId + ")");
        }

        // Update internal counter if client ID is higher
        idLock.lock();
        try {
            if (clientId > currentId.get()) {
                currentId.set(clientId);
                // Persist if threshold reached
                if (clientId - lastPersistedId >= PERSIST_THRESHOLD) {
                    persistCurrentId();
                }
            }
        } finally {
            idLock.unlock();
        }

        entity.setId(String.valueOf(clientId));
        delegateRepo.insert(entity);
    }

    /**
     * Insert multiple entities with client-provided IDs.
     * Only works if allowClientIds is enabled.
     * Updates internal counter to highest provided ID if greater than current.
     *
     * @param entities Entities to insert
     * @param clientIds Client-provided IDs (must match entities size)
     * @throws IllegalStateException if allowClientIds is false
     * @throws IllegalArgumentException if IDs are invalid or sizes don't match
     */
    public void insertMultipleWithClientIds(List<T> entities, List<Long> clientIds) throws SQLException {
        if (!allowClientIds) {
            throw new IllegalStateException("Client IDs not allowed. Enable with allowClientIds(true) in builder");
        }

        if (entities.size() != clientIds.size()) {
            throw new IllegalArgumentException("Entity count must match ID count");
        }

        // Validate all IDs and find max
        long maxClientId = 0;
        for (Long id : clientIds) {
            if (id == null || id <= 0 || id > maxId) {
                throw new IllegalArgumentException("Invalid client ID: " + id);
            }
            maxClientId = Math.max(maxClientId, id);
        }

        // Update internal counter if needed
        idLock.lock();
        try {
            if (maxClientId > currentId.get()) {
                currentId.set(maxClientId);
                if (maxClientId - lastPersistedId >= PERSIST_THRESHOLD) {
                    persistCurrentId();
                }
            }
        } finally {
            idLock.unlock();
        }

        // Set IDs and insert
        for (int i = 0; i < entities.size(); i++) {
            entities.get(i).setId(String.valueOf(clientIds.get(i)));
        }

        delegateRepo.insertMultiple(entities);
    }

    /**
     * Insert multiple entities with auto-generated sequential IDs.
     *
     * @param entities Entities to insert (IDs will be overwritten)
     * @return List of assigned IDs
     */
    public List<Long> insertMultipleWithAutoId(List<T> entities) throws SQLException {
        List<Long> ids = getNextN(entities.size());

        for (int i = 0; i < entities.size(); i++) {
            entities.get(i).setId(String.valueOf(ids.get(i)));
        }

        delegateRepo.insertMultiple(entities);
        return ids;
    }

    /**
     * Reserve a range of IDs for future use.
     * Advances the internal counter to the end of the range.
     * Useful for pre-allocating ID blocks for distributed systems.
     *
     * @param count Number of IDs to reserve
     * @return Range object containing start and end IDs (inclusive)
     * @throws IllegalArgumentException if count <= 0 or exceeds available IDs
     */
    public IdRange reserveIdRange(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("Count must be positive");
        }

        idLock.lock();
        try {
            long startId = currentId.get() + 1;
            long endId = startId + count - 1;

            // Check for max ID
            if (endId > maxId) {
                if (wrapAround) {
                    // Handle wraparound for range
                    currentId.set(count);
                    startId = 1;
                    endId = count;
                    logger.info("ID range wrapped around (max " + maxId + " reached)");
                } else {
                    throw new IllegalStateException(
                        "Not enough IDs available for range (need " + count + ", available " + (maxId - currentId.get()) + ")"
                    );
                }
            } else {
                currentId.set(endId);
            }

            // Persist state after range reservation
            if (endId - lastPersistedId >= PERSIST_THRESHOLD) {
                persistCurrentId();
            }

            return new IdRange(startId, endId, count);
        } finally {
            idLock.unlock();
        }
    }

    /**
     * Insert entities with a specific ID range.
     * The range must have been previously reserved or you must use allowClientIds.
     * Entities count must match the range size.
     *
     * @param entities Entities to insert
     * @param range ID range to use
     * @throws IllegalArgumentException if entity count doesn't match range size
     * @throws SQLException on database error
     */
    public void insertWithIdRange(List<T> entities, IdRange range) throws SQLException {
        if (entities.size() != range.getCount()) {
            throw new IllegalArgumentException(
                "Entity count (" + entities.size() + ") must match range size (" + range.getCount() + ")"
            );
        }

        // Assign IDs from range
        long currentId = range.getStart();
        for (T entity : entities) {
            entity.setId(String.valueOf(currentId++));
        }

        delegateRepo.insertMultiple(entities);
    }

    /**
     * Insert entities using IDs from a specific range.
     * Validates that all IDs fall within the specified range.
     * Only works if allowClientIds is enabled.
     *
     * @param entities Entities to insert
     * @param startId Start of ID range (inclusive)
     * @param endId End of ID range (inclusive)
     * @throws IllegalStateException if allowClientIds is false
     * @throws IllegalArgumentException if IDs would exceed range
     */
    public void insertWithinIdRange(List<T> entities, long startId, long endId) throws SQLException {
        if (!allowClientIds) {
            throw new IllegalStateException("Client IDs not allowed. Enable with allowClientIds(true) in builder");
        }

        if (startId <= 0 || endId > maxId || startId > endId) {
            throw new IllegalArgumentException("Invalid ID range: " + startId + " to " + endId);
        }

        int rangeSize = (int)(endId - startId + 1);
        if (entities.size() > rangeSize) {
            throw new IllegalArgumentException(
                "Too many entities (" + entities.size() + ") for range size (" + rangeSize + ")"
            );
        }

        // Update internal counter if needed
        idLock.lock();
        try {
            if (endId > currentId.get()) {
                currentId.set(endId);
                if (endId - lastPersistedId >= PERSIST_THRESHOLD) {
                    persistCurrentId();
                }
            }
        } finally {
            idLock.unlock();
        }

        // Assign IDs sequentially within range
        long id = startId;
        for (T entity : entities) {
            entity.setId(String.valueOf(id++));
        }

        delegateRepo.insertMultiple(entities);
    }

    /**
     * Find an entity by ID.
     */
    public T findById(long id) throws SQLException {
        return delegateRepo.findById(String.valueOf(id));
    }

    /**
     * Retrieve a batch of records starting from a specific ID.
     * Simple sequential retrieval: startId=1000, batchSize=100 returns IDs 1000-1099
     *
     * @param startId The starting ID (inclusive)
     * @param batchSize Number of records to retrieve
     * @return List of entities with IDs in range [startId, startId+batchSize-1]
     */
    public List<T> findByIdRange(long startId, int batchSize) throws SQLException {
        List<T> results = new ArrayList<>();

        for (long id = startId; id < startId + batchSize; id++) {
            T entity = findById(id);
            if (entity != null) {
                results.add(entity);
            }
        }

        return results;
    }

    /**
     * Represents a reserved ID range.
     */
    public static class IdRange {
        private final long start;
        private final long end;
        private final int count;

        public IdRange(long start, long end, int count) {
            this.start = start;
            this.end = end;
            this.count = count;
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }

        public int getCount() {
            return count;
        }

        public boolean contains(long id) {
            return id >= start && id <= end;
        }

        @Override
        public String toString() {
            return String.format("IdRange[%d-%d, count=%d]", start, end, count);
        }
    }

    /**
     * Find entity by numeric ID.
     *
     * @param id Numeric ID
     * @return Entity or null if not found
     */
    public T findById(long id) throws SQLException {
        return delegateRepo.findById(String.valueOf(id));
    }

    /**
     * Find batch of entities starting from ID.
     * IDs are numeric but stored as strings, so ordering is numeric.
     *
     * @param startId Starting ID (exclusive)
     * @param batchSize Number of entities to retrieve
     * @return List of entities
     */
    public List<T> findBatchFromId(long startId, int batchSize) throws SQLException {
        // Since IDs are numeric strings, we need to handle ordering properly
        // The delegate repository's findBatchByIdGreaterThan should work if IDs are zero-padded
        return delegateRepo.findBatchByIdGreaterThan(String.valueOf(startId), batchSize);
    }

    /**
     * Get delegate repository for advanced operations.
     *
     * @return The underlying ShardingRepository
     */
    public ShardingRepository<T, LocalDateTime> getDelegate() {
        return delegateRepo;
    }

    /**
     * Shutdown the repository and persist final state.
     */
    public void shutdown() {
        idLock.lock();
        try {
            // Persist final state
            if (currentId.get() != lastPersistedId) {
                persistCurrentId();
            }

            // Shutdown delegate
            if (delegateRepo != null) {
                delegateRepo.shutdown();
            }

            // Shutdown connection provider
            if (connectionProvider != null) {
                connectionProvider.shutdown();
            }

            logger.info("SimpleSequentialRepository shutdown completed");
        } finally {
            idLock.unlock();
        }
    }

    /**
     * Builder for SimpleSequentialRepository
     */
    public static class Builder<T extends ShardingEntity<LocalDateTime>> {
        private final Class<T> entityClass;

        // Connection settings
        private String host = "127.0.0.1";
        private int port = 3306;
        private String database;
        private String username;
        private String password;

        // Table settings
        private String tableName;

        // ID settings
        private long maxId = Long.MAX_VALUE;
        private boolean wrapAround = true;
        private boolean allowClientIds = false;

        // Partition settings
        private int retentionDays = 30;
        private boolean autoMaintenance = true;
        private LocalTime maintenanceTime = LocalTime.of(2, 0);

        public Builder(Class<T> entityClass) {
            this.entityClass = entityClass;
        }

        public Builder<T> connection(String host, int port, String database, String username, String password) {
            this.host = host;
            this.port = port;
            this.database = database;
            this.username = username;
            this.password = password;
            return this;
        }

        public Builder<T> tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder<T> maxId(long maxId) {
            if (maxId <= 0) {
                throw new IllegalArgumentException("Max ID must be positive");
            }
            this.maxId = maxId;
            return this;
        }

        public Builder<T> wrapAround(boolean wrapAround) {
            this.wrapAround = wrapAround;
            return this;
        }

        public Builder<T> allowClientIds(boolean allow) {
            this.allowClientIds = allow;
            return this;
        }

        public Builder<T> retentionDays(int days) {
            this.retentionDays = days;
            return this;
        }

        public Builder<T> autoMaintenance(boolean enabled) {
            this.autoMaintenance = enabled;
            return this;
        }

        public Builder<T> maintenanceTime(LocalTime time) {
            this.maintenanceTime = time;
            return this;
        }

        public SimpleSequentialRepository<T> build() {
            // Validate required fields
            if (database == null || database.isEmpty()) {
                throw new IllegalStateException("Database name is required");
            }
            if (username == null || password == null) {
                throw new IllegalStateException("Database credentials are required");
            }
            if (tableName == null || tableName.isEmpty()) {
                throw new IllegalStateException("Table name is required");
            }

            return new SimpleSequentialRepository<>(this);
        }
    }

    /**
     * Static factory method for easy instantiation
     */
    public static <T extends ShardingEntity<LocalDateTime>> Builder<T> builder(Class<T> entityClass) {
        return new Builder<>(entityClass);
    }
}
package com.telcobright.core.repository;

import com.telcobright.core.annotation.Column;
import com.telcobright.core.annotation.Id;
import com.telcobright.core.annotation.ShardingKey;
import com.telcobright.core.annotation.Table;
import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.partitioning.PartitionRange;
import com.telcobright.core.partitioning.RepositoryMode;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Multi-entity repository that acts as a router/coordinator for multiple entity types.
 * This repository maintains a HashMap of entity-specific single-entity repositories
 * and routes API calls based on entity type with zero reflection overhead after initialization.
 *
 * Architecture:
 * - Uses existing single-entity repositories (GenericPartitionedTableRepository, GenericMultiTableRepository, etc.)
 * - Performs reflection only during builder/registration phase
 * - Runtime operations use direct HashMap lookups - reflection-free
 * - Each entity can have its own repository type and configuration
 */
public class MultiEntityRepository {
    private static final Logger logger = Logger.getLogger(MultiEntityRepository.class.getName());

    // Core routing map: entity class -> its configured repository instance
    private final Map<Class<?>, Object> repositoryMap = new ConcurrentHashMap<>();

    // Metadata cached during registration (reflection done once)
    private final Map<Class<?>, EntityInfo> entityInfoMap = new ConcurrentHashMap<>();

    // Registered entity classes for validation
    private final Set<Class<?>> registeredEntities = Collections.synchronizedSet(new HashSet<>());

    // Global configuration (can be overridden per entity)
    private final String defaultHost;
    private final int defaultPort;
    private final String defaultDatabase;
    private final String defaultUsername;
    private final String defaultPassword;

    private MultiEntityRepository(Builder builder) {
        this.defaultHost = builder.defaultHost;
        this.defaultPort = builder.defaultPort;
        this.defaultDatabase = builder.defaultDatabase;
        this.defaultUsername = builder.defaultUsername;
        this.defaultPassword = builder.defaultPassword;

        // Initialize all repositories during construction
        initializeRepositories(builder);
    }

    /**
     * Initialize repositories for all registered entities.
     * Reflection is performed here once per entity and cached.
     */
    private void initializeRepositories(Builder builder) {
        for (Map.Entry<Class<?>, EntityRegistration> entry : builder.registrations.entrySet()) {
            Class<?> entityClass = entry.getKey();
            EntityRegistration registration = entry.getValue();

            // Validate entity and extract metadata (reflection done once here)
            EntityInfo entityInfo = validateAndExtractInfo(entityClass);
            entityInfoMap.put(entityClass, entityInfo);

            // Create the repository instance
            Object repository = createRepository(entityClass, entityInfo, registration);
            repositoryMap.put(entityClass, repository);
            registeredEntities.add(entityClass);

            logger.info(String.format("Registered entity %s with table %s using %s repository",
                entityClass.getSimpleName(), entityInfo.tableName, registration.repositoryMode));
        }
    }

    /**
     * Validate entity and extract metadata using reflection (done once during registration).
     */
    private EntityInfo validateAndExtractInfo(Class<?> entityClass) {
        EntityInfo info = new EntityInfo();
        info.entityClass = entityClass;

        // Check if implements ShardingEntity
        if (!ShardingEntity.class.isAssignableFrom(entityClass)) {
            throw new IllegalArgumentException("Entity " + entityClass.getName() +
                " must implement ShardingEntity interface");
        }

        // Extract table name from @Table annotation
        Table tableAnnotation = entityClass.getAnnotation(Table.class);
        if (tableAnnotation == null) {
            throw new IllegalArgumentException("Entity " + entityClass.getName() +
                " must have @Table annotation");
        }
        info.tableName = tableAnnotation.name();

        // Validate fields
        int idCount = 0;
        int shardingKeyCount = 0;
        Field idField = null;
        Field shardingKeyField = null;

        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(Id.class)) {
                idCount++;
                idField = field;
            }
            if (field.isAnnotationPresent(ShardingKey.class)) {
                shardingKeyCount++;
                shardingKeyField = field;
                info.shardingKeyType = field.getType();
            }
        }

        // Validate exactly one @Id field
        if (idCount != 1) {
            throw new IllegalArgumentException("Entity " + entityClass.getName() +
                " must have exactly ONE field annotated with @Id (found: " + idCount + ")");
        }

        // Validate exactly one @ShardingKey field
        if (shardingKeyCount != 1) {
            throw new IllegalArgumentException("Entity " + entityClass.getName() +
                " must have exactly ONE field annotated with @ShardingKey (found: " + shardingKeyCount + ")");
        }

        // Validate ID field type must be String
        if (!String.class.equals(idField.getType())) {
            throw new IllegalArgumentException("Entity " + entityClass.getName() +
                " @Id field must be of type String (found: " + idField.getType() + ")");
        }

        // Validate ShardingKey field type must be LocalDateTime
        if (!LocalDateTime.class.equals(shardingKeyField.getType())) {
            throw new IllegalArgumentException("Entity " + entityClass.getName() +
                " @ShardingKey field must be of type LocalDateTime (found: " + shardingKeyField.getType() + ")");
        }

        // Validate @Id(autoGenerated = false)
        Id idAnnotation = idField.getAnnotation(Id.class);
        if (idAnnotation.autoGenerated()) {
            throw new IllegalArgumentException("Entity " + entityClass.getName() +
                " @Id field must have autoGenerated=false");
        }

        info.idFieldName = idField.getName();
        info.shardingKeyFieldName = shardingKeyField.getName();

        return info;
    }

    /**
     * Create repository instance for an entity based on its registration.
     */
    @SuppressWarnings("unchecked")
    private Object createRepository(Class<?> entityClass, EntityInfo entityInfo, EntityRegistration registration) {
        // Use registration's connection details or fall back to defaults
        String host = registration.host != null ? registration.host : defaultHost;
        int port = registration.port > 0 ? registration.port : defaultPort;
        String database = registration.database != null ? registration.database : defaultDatabase;
        String username = registration.username != null ? registration.username : defaultUsername;
        String password = registration.password != null ? registration.password : defaultPassword;

        switch (registration.repositoryMode) {
            case PARTITIONED:
                return GenericPartitionedTableRepository.builder(entityClass)
                    .host(host)
                    .port(port)
                    .database(database)
                    .username(username)
                    .password(password)
                    .tableName(entityInfo.tableName)
                    .partitionRange(registration.partitionRange)
                    .retentionDays(registration.retentionDays)
                    .autoMaintenance(registration.autoMaintenance)
                    .maintenanceTime(registration.maintenanceTime)
                    .maxPoolSize(registration.maxPoolSize)
                    .build();

            case MULTI_TABLE:
                GenericMultiTableRepository.Builder multiTableBuilder =
                    GenericMultiTableRepository.builder(entityClass)
                        .host(host)
                        .port(port)
                        .database(database)
                        .username(username)
                        .password(password)
                        .tablePrefix(entityInfo.tableName)
                        .retentionDays(registration.retentionDays)
                        .autoMaintenance(registration.autoMaintenance)
                        .maintenanceTime(registration.maintenanceTime)
                        .maxPoolSize(registration.maxPoolSize);

                // Configure nested partitions based on sharding key type
                if (LocalDateTime.class.equals(entityInfo.shardingKeyType)) {
                    // For DateTime: 24 hourly partitions per table
                    multiTableBuilder.nestedPartitionsEnabled(true)
                                   .nestedPartitionCount(24);
                } else if (registration.partitionsPerTable > 0) {
                    // For other types: use configured partition count
                    multiTableBuilder.nestedPartitionsEnabled(true)
                                   .nestedPartitionCount(registration.partitionsPerTable);
                }

                return multiTableBuilder.build();

            default:
                throw new UnsupportedOperationException("Repository mode " + registration.repositoryMode + " not supported");
        }
    }

    /**
     * Get repository for entity class - REFLECTION-FREE lookup.
     * This is O(1) HashMap lookup with no reflection.
     */
    @SuppressWarnings("unchecked")
    private <T extends ShardingEntity<?>> Object getRepository(Class<T> entityClass) {
        Object repository = repositoryMap.get(entityClass);
        if (repository == null) {
            throw new IllegalArgumentException("Entity " + entityClass.getName() +
                " is not registered. Registered entities: " + registeredEntities);
        }
        return repository;
    }

    // ================== PUBLIC API METHODS ==================
    // All methods are REFLECTION-FREE after initial registration

    /**
     * Find entity by ID.
     * REFLECTION-FREE - Direct HashMap lookup.
     */
    @SuppressWarnings("unchecked")
    public <T extends ShardingEntity<?>> T findById(Class<T> entityClass, String id) throws SQLException {
        Object repo = getRepository(entityClass);

        if (repo instanceof GenericPartitionedTableRepository) {
            return ((GenericPartitionedTableRepository<T>) repo).findById(id);
        } else if (repo instanceof GenericMultiTableRepository) {
            return ((GenericMultiTableRepository<T>) repo).findById(id);
        } else if (repo instanceof SimpleSequentialRepository) {
            return ((SimpleSequentialRepository<T>) repo).findById(Long.parseLong(id));
        } else {
            throw new UnsupportedOperationException("Unknown repository type for " + entityClass.getName());
        }
    }

    /**
     * Find entity by ID and partition column value for optimized lookup.
     * Uses partition pruning for efficient queries.
     * REFLECTION-FREE operation.
     */
    @SuppressWarnings("unchecked")
    public <T extends ShardingEntity<LocalDateTime>> T findByIdAndPartitionColValue(
            Class<T> entityClass, String id, LocalDateTime partitionValue) throws SQLException {

        Object repo = getRepository(entityClass);

        if (repo instanceof GenericPartitionedTableRepository) {
            // Native partition mode - partition pruning via WHERE clause
            return ((GenericPartitionedTableRepository<T>) repo).findByIdAndPartitionColValue(id, partitionValue);
        } else if (repo instanceof GenericMultiTableRepository) {
            // Multi-table mode - targets specific table + WHERE clause for nested partition pruning
            return ((GenericMultiTableRepository<T>) repo).findByIdAndPartitionColValue(id, partitionValue);
        } else {
            // Fallback to regular findById for repositories without this optimization
            return findById(entityClass, id);
        }
    }

    /**
     * Insert entity.
     * REFLECTION-FREE operation.
     */
    @SuppressWarnings("unchecked")
    public <T extends ShardingEntity<?>> void insert(Class<T> entityClass, T entity) throws SQLException {
        Object repo = getRepository(entityClass);

        if (repo instanceof GenericPartitionedTableRepository) {
            ((GenericPartitionedTableRepository<T>) repo).insert(entity);
        } else if (repo instanceof GenericMultiTableRepository) {
            ((GenericMultiTableRepository<T>) repo).insert(entity);
        } else if (repo instanceof SimpleSequentialRepository) {
            ((SimpleSequentialRepository<T>) repo).insert(entity);
        } else {
            throw new UnsupportedOperationException("Unknown repository type for " + entityClass.getName());
        }
    }

    /**
     * Insert multiple entities.
     * REFLECTION-FREE operation.
     */
    @SuppressWarnings("unchecked")
    public <T extends ShardingEntity<?>> void insertMultiple(Class<T> entityClass, List<T> entities) throws SQLException {
        Object repo = getRepository(entityClass);

        if (repo instanceof GenericPartitionedTableRepository) {
            ((GenericPartitionedTableRepository<T>) repo).insertMultiple(entities);
        } else if (repo instanceof GenericMultiTableRepository) {
            ((GenericMultiTableRepository<T>) repo).insertMultiple(entities);
        } else if (repo instanceof SimpleSequentialRepository) {
            ((SimpleSequentialRepository<T>) repo).insertMultiple(entities);
        } else {
            throw new UnsupportedOperationException("Unknown repository type for " + entityClass.getName());
        }
    }

    /**
     * Update entity by ID.
     * REFLECTION-FREE operation.
     */
    @SuppressWarnings("unchecked")
    public <T extends ShardingEntity<?>> void updateById(Class<T> entityClass, String id, T entity) throws SQLException {
        Object repo = getRepository(entityClass);

        if (repo instanceof GenericPartitionedTableRepository) {
            ((GenericPartitionedTableRepository<T>) repo).updateById(id, entity);
        } else if (repo instanceof GenericMultiTableRepository) {
            ((GenericMultiTableRepository<T>) repo).updateById(id, entity);
        } else if (repo instanceof SimpleSequentialRepository) {
            ((SimpleSequentialRepository<T>) repo).updateById(id, entity);
        } else {
            throw new UnsupportedOperationException("Unknown repository type for " + entityClass.getName());
        }
    }

    /**
     * Delete entity by ID.
     * REFLECTION-FREE operation.
     */
    @SuppressWarnings("unchecked")
    public <T extends ShardingEntity<?>> void deleteById(Class<T> entityClass, String id) throws SQLException {
        Object repo = getRepository(entityClass);

        if (repo instanceof GenericPartitionedTableRepository) {
            ((GenericPartitionedTableRepository<T>) repo).deleteById(id);
        } else if (repo instanceof GenericMultiTableRepository) {
            ((GenericMultiTableRepository<T>) repo).deleteById(id);
        } else if (repo instanceof SimpleSequentialRepository) {
            ((SimpleSequentialRepository<T>) repo).deleteById(id);
        } else {
            throw new UnsupportedOperationException("Unknown repository type for " + entityClass.getName());
        }
    }

    /**
     * Find all entities within date range.
     * REFLECTION-FREE operation with partition pruning.
     */
    @SuppressWarnings("unchecked")
    public <T extends ShardingEntity<LocalDateTime>> List<T> findByPartitionColBetween(
            Class<T> entityClass, LocalDateTime start, LocalDateTime end) throws SQLException {

        Object repo = getRepository(entityClass);

        if (repo instanceof GenericPartitionedTableRepository) {
            return ((GenericPartitionedTableRepository<T>) repo).findByPartitionColBetween(start, end);
        } else if (repo instanceof GenericMultiTableRepository) {
            return ((GenericMultiTableRepository<T>) repo).findByPartitionColBetween(start, end);
        } else if (repo instanceof SimpleSequentialRepository) {
            return ((SimpleSequentialRepository<T>) repo).findAllByPartitionRange(start, end);
        } else {
            throw new UnsupportedOperationException("Unknown repository type for " + entityClass.getName());
        }
    }

    /**
     * Find batch using cursor-based pagination.
     * REFLECTION-FREE operation.
     */
    @SuppressWarnings("unchecked")
    public <T extends ShardingEntity<?>> List<T> findBatchByIdGreaterThan(
            Class<T> entityClass, String lastId, int batchSize) throws SQLException {

        Object repo = getRepository(entityClass);

        if (repo instanceof GenericPartitionedTableRepository) {
            return ((GenericPartitionedTableRepository<T>) repo).findBatchByIdGreaterThan(lastId, batchSize);
        } else if (repo instanceof GenericMultiTableRepository) {
            return ((GenericMultiTableRepository<T>) repo).findBatchByIdGreaterThan(lastId, batchSize);
        } else if (repo instanceof SimpleSequentialRepository) {
            long startId = lastId == null ? 0 : Long.parseLong(lastId) + 1;
            return ((SimpleSequentialRepository<T>) repo).findByIdRange(startId, batchSize);
        } else {
            throw new UnsupportedOperationException("Unknown repository type for " + entityClass.getName());
        }
    }

    /**
     * Count entities within date range.
     * REFLECTION-FREE operation.
     */
    @SuppressWarnings("unchecked")
    public <T extends ShardingEntity<LocalDateTime>> long countByPartitionColBetween(
            Class<T> entityClass, LocalDateTime start, LocalDateTime end) throws SQLException {

        Object repo = getRepository(entityClass);

        if (repo instanceof GenericPartitionedTableRepository) {
            return ((GenericPartitionedTableRepository<T>) repo).countByPartitionColBetween(start, end);
        } else if (repo instanceof GenericMultiTableRepository) {
            return ((GenericMultiTableRepository<T>) repo).countByPartitionColBetween(start, end);
        } else {
            // SimpleSequentialRepository doesn't have count method, use findAll and count
            List<T> results = findByPartitionColBetween(entityClass, start, end);
            return results.size();
        }
    }

    /**
     * Get all registered entity classes.
     */
    public Set<Class<?>> getRegisteredEntities() {
        return new HashSet<>(registeredEntities);
    }

    /**
     * Check if entity is registered.
     * REFLECTION-FREE - Direct set lookup.
     */
    public boolean isEntityRegistered(Class<?> entityClass) {
        return registeredEntities.contains(entityClass);
    }

    /**
     * Get entity info (metadata) for a registered entity.
     * REFLECTION-FREE - Returns pre-cached info.
     */
    public EntityInfo getEntityInfo(Class<?> entityClass) {
        return entityInfoMap.get(entityClass);
    }

    /**
     * Shutdown all repositories.
     */
    public void shutdown() {
        for (Map.Entry<Class<?>, Object> entry : repositoryMap.entrySet()) {
            Object repo = entry.getValue();
            String entityName = entry.getKey().getSimpleName();

            try {
                if (repo instanceof GenericPartitionedTableRepository) {
                    ((GenericPartitionedTableRepository<?>) repo).shutdown();
                } else if (repo instanceof GenericMultiTableRepository) {
                    ((GenericMultiTableRepository<?>) repo).shutdown();
                } else if (repo instanceof SimpleSequentialRepository) {
                    ((SimpleSequentialRepository<?>) repo).shutdown();
                }
                logger.info("Shutdown repository for entity: " + entityName);
            } catch (Exception e) {
                logger.warning("Error shutting down repository for " + entityName + ": " + e.getMessage());
            }
        }
        logger.info("MultiEntityRepository shutdown complete");
    }

    // ================== INNER CLASSES ==================

    /**
     * Cached entity metadata - extracted once during registration.
     */
    public static class EntityInfo {
        public Class<?> entityClass;
        public String tableName;
        public String idFieldName;
        public String shardingKeyFieldName;
        public Class<?> shardingKeyType;

        @Override
        public String toString() {
            return String.format("EntityInfo[class=%s, table=%s, idField=%s, shardingKeyField=%s]",
                entityClass.getSimpleName(), tableName, idFieldName, shardingKeyFieldName);
        }
    }

    /**
     * Entity registration configuration.
     */
    private static class EntityRegistration {
        RepositoryMode repositoryMode = RepositoryMode.PARTITIONED;
        PartitionRange partitionRange = PartitionRange.DAILY;
        int retentionDays = 30;
        int partitionsPerTable = 20;  // For multi-table mode with non-DateTime sharding key
        boolean autoMaintenance = true;
        LocalTime maintenanceTime = LocalTime.of(4, 0);
        int maxPoolSize = 10;

        // Optional per-entity connection override
        String host;
        int port;
        String database;
        String username;
        String password;
    }

    // ================== BUILDER ==================

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final Map<Class<?>, EntityRegistration> registrations = new LinkedHashMap<>();

        // Default connection details (can be overridden per entity)
        private String defaultHost = "localhost";
        private int defaultPort = 3306;
        private String defaultDatabase;
        private String defaultUsername;
        private String defaultPassword;

        /**
         * Set default database connection for all entities.
         */
        public Builder defaultConnection(String host, int port, String database, String username, String password) {
            this.defaultHost = host;
            this.defaultPort = port;
            this.defaultDatabase = database;
            this.defaultUsername = username;
            this.defaultPassword = password;
            return this;
        }

        /**
         * Register an entity with default configuration.
         * Uses PARTITIONED mode with DAILY partitions.
         */
        public <T extends ShardingEntity<?>> Builder register(Class<T> entityClass) {
            registrations.put(entityClass, new EntityRegistration());
            return this;
        }

        /**
         * Register multiple entities with default configuration.
         */
        @SafeVarargs
        public final Builder register(Class<? extends ShardingEntity<?>>... entities) {
            for (Class<? extends ShardingEntity<?>> entity : entities) {
                register(entity);
            }
            return this;
        }

        /**
         * Register an entity with custom configuration.
         */
        public <T extends ShardingEntity<?>> EntityConfigBuilder<T> registerEntity(Class<T> entityClass) {
            return new EntityConfigBuilder<>(this, entityClass);
        }

        /**
         * Build the MultiEntityRepository.
         * Performs all reflection and validation during build.
         */
        public MultiEntityRepository build() {
            if (registrations.isEmpty()) {
                throw new IllegalStateException("At least one entity must be registered");
            }

            if (defaultDatabase == null || defaultUsername == null || defaultPassword == null) {
                throw new IllegalStateException("Default database connection details are required");
            }

            return new MultiEntityRepository(this);
        }

        /**
         * Fluent builder for entity-specific configuration.
         */
        public class EntityConfigBuilder<T extends ShardingEntity<?>> {
            private final Builder parent;
            private final Class<T> entityClass;
            private final EntityRegistration registration = new EntityRegistration();

            EntityConfigBuilder(Builder parent, Class<T> entityClass) {
                this.parent = parent;
                this.entityClass = entityClass;
            }

            public EntityConfigBuilder<T> repositoryMode(RepositoryMode mode) {
                registration.repositoryMode = mode;
                return this;
            }

            public EntityConfigBuilder<T> partitionRange(PartitionRange range) {
                registration.partitionRange = range;
                return this;
            }

            public EntityConfigBuilder<T> retentionDays(int days) {
                registration.retentionDays = days;
                return this;
            }

            /**
             * Set partitions per table for MULTI_TABLE mode with non-DateTime sharding key.
             * Maximum value is 100.
             */
            public EntityConfigBuilder<T> partitionsPerTable(int partitions) {
                if (partitions > 100) {
                    throw new IllegalArgumentException("partitionsPerTable cannot exceed 100 (provided: " + partitions + ")");
                }
                registration.partitionsPerTable = partitions;
                return this;
            }

            public EntityConfigBuilder<T> autoMaintenance(boolean enabled) {
                registration.autoMaintenance = enabled;
                return this;
            }

            public EntityConfigBuilder<T> maintenanceTime(LocalTime time) {
                registration.maintenanceTime = time;
                return this;
            }

            public EntityConfigBuilder<T> maxPoolSize(int size) {
                registration.maxPoolSize = size;
                return this;
            }

            /**
             * Override connection for this specific entity.
             */
            public EntityConfigBuilder<T> connection(String host, int port, String database,
                                                    String username, String password) {
                registration.host = host;
                registration.port = port;
                registration.database = database;
                registration.username = username;
                registration.password = password;
                return this;
            }

            /**
             * Complete entity registration and return to main builder.
             */
            public Builder and() {
                parent.registrations.put(entityClass, registration);
                return parent;
            }
        }
    }
}
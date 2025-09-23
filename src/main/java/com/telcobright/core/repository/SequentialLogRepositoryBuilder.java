package com.telcobright.core.repository;

import com.telcobright.api.ShardingRepository;
import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.partition.PartitionType;
import com.telcobright.core.enums.PartitionRange;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Simplified builder for creating a log-optimized repository using native partitioning.
 *
 * This builder creates a GenericPartitionedTableRepository configured specifically for:
 * - Sequential log storage with date-based partitioning
 * - Automatic partition rotation (old partitions dropped, new ones created)
 * - Optimized for sequential ID-based iteration
 * - Minimal configuration requirements
 *
 * Use Case: Application logs, audit trails, event streams
 *
 * Example Usage:
 * ```java
 * ShardingRepository<LogEntry, LocalDateTime> repo =
 *     SequentialLogRepositoryBuilder.create(LogEntry.class)
 *         .connection("localhost", 3306, "logs_db", "user", "pass")
 *         .tableName("app_logs")
 *         .retentionDays(30)
 *         .build();
 * ```
 */
public class SequentialLogRepositoryBuilder {

    /**
     * Create a log-optimized repository with minimal configuration.
     *
     * Features:
     * - Date-based partitioning (DAILY by default)
     * - Automatic maintenance at 2 AM
     * - 30-day retention by default
     * - Sequential ID support with cursor-based pagination
     *
     * @param entityClass Entity class implementing ShardingEntity<LocalDateTime>
     * @return Configured repository ready for log storage
     */
    public static <T extends ShardingEntity<LocalDateTime>> Builder<T> create(Class<T> entityClass) {
        return new Builder<>(entityClass);
    }

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

        // Partition settings
        private int retentionDays = 30;
        private PartitionRange partitionRange = PartitionRange.DAILY;
        private boolean autoMaintenance = true;
        private LocalTime maintenanceTime = LocalTime.of(2, 0);

        private Builder(Class<T> entityClass) {
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

        public Builder<T> retentionDays(int days) {
            if (days < 1) {
                throw new IllegalArgumentException("Retention days must be at least 1");
            }
            this.retentionDays = days;
            return this;
        }

        public Builder<T> partitionRange(PartitionRange range) {
            this.partitionRange = range;
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

        public ShardingRepository<T, LocalDateTime> build() {
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

            // Create and return configured GenericPartitionedTableRepository
            // Note: GenericPartitionedTableRepository supports DAILY partitions by default
            // For MONTHLY or other ranges, consider using GenericMultiTableRepository
            return GenericPartitionedTableRepository.<T, LocalDateTime>builder(entityClass)
                .host(host)
                .port(port)
                .database(database)
                .username(username)
                .password(password)
                .tableName(tableName)
                .partitionRetentionPeriod(retentionDays)
                .withPartitionType(PartitionType.DATE_BASED)
                .autoManagePartitions(autoMaintenance)
                .partitionAdjustmentTime(maintenanceTime)
                .initializePartitionsOnStart(true)
                .charset("utf8mb4")
                .collation("utf8mb4_bin") // Binary collation for sequential IDs
                .build();
        }
    }

    /**
     * Utility class for generating time-based sequential IDs.
     * These IDs are naturally sortable and work well with string comparison.
     */
    public static class SequentialIdGenerator {
        private static long sequence = 0;
        private static long lastTimestamp = -1;

        /**
         * Generate a time-based sequential ID.
         * Format: timestamp-sequence (e.g., "1737562800000-000001")
         *
         * This format ensures:
         * - Natural chronological ordering
         * - Uniqueness even with high concurrency
         * - Efficient string comparison in MySQL
         */
        public static synchronized String generate() {
            long timestamp = System.currentTimeMillis();

            if (timestamp == lastTimestamp) {
                sequence++;
            } else {
                sequence = 0;
                lastTimestamp = timestamp;
            }

            return String.format("%013d-%06d", timestamp, sequence);
        }

        /**
         * Generate an ID with a custom prefix.
         * Useful for identifying log sources or types.
         */
        public static synchronized String generate(String prefix) {
            return prefix + "-" + generate();
        }
    }
}
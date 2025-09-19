package com.telcobright.splitverse.tests;

import com.telcobright.api.ShardingRepository;
import com.telcobright.core.config.DataSourceConfig;
import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.enums.PartitionColumnType;
import com.telcobright.core.enums.PartitionRange;
import com.telcobright.core.enums.ShardingStrategy;
import com.telcobright.core.repository.SplitVerseRepository;
import com.telcobright.splitverse.config.RepositoryMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;

import java.time.LocalDateTime;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for different partition range scenarios including value-based partitioning.
 * NOTE: Tests temporarily disabled due to Maven classpath issues
 * Tests work when run directly with Java but fail under Maven
 */
@Disabled("Temporarily disabled due to Maven classpath issues with MySQL driver")
public class PartitionRangeTest {

    public static class Event implements ShardingEntity {
        private String id;
        private LocalDateTime createdAt;
        private Long sequenceNumber;
        private String eventType;
        private String data;

        @Override
        public String getId() { return id; }
        @Override
        public void setId(String id) { this.id = id; }
        @Override
        public LocalDateTime getCreatedAt() { return createdAt; }
        @Override
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

        public Long getSequenceNumber() { return sequenceNumber; }
        public void setSequenceNumber(Long sequenceNumber) { this.sequenceNumber = sequenceNumber; }
        public String getEventType() { return eventType; }
        public void setEventType(String eventType) { this.eventType = eventType; }
        public String getData() { return data; }
        public void setData(String data) { this.data = data; }
    }

    @Test
    public void testValueBasedPartitioning1Million() {
        // Test creating new table every 1 million records
        ShardingRepository<Event> repository = SplitVerseRepository.<Event>builder()
            .withEntityClass(Event.class)
            .withTableName("events")
            .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
            .withPartitionColumn("sequence_number", PartitionColumnType.LONG)
            .withPartitionRange(PartitionRange.VALUE_RANGE_1M)
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withDataSources(Arrays.asList(
                DataSourceConfig.create("127.0.0.1", 3306, "test_db", "root", "123456")
            ))
            .build();

        assertNotNull(repository);
        // This would create tables like:
        // events_range_0     (0 - 999,999)
        // events_range_1     (1,000,000 - 1,999,999)
        // events_range_2     (2,000,000 - 2,999,999)
    }

    @Test
    public void testValueBasedPartitioning100K() {
        // Test creating new table every 100K records
        ShardingRepository<Event> repository = SplitVerseRepository.<Event>builder()
            .withEntityClass(Event.class)
            .withTableName("events")
            .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
            .withPartitionColumn("sequence_number", PartitionColumnType.LONG)
            .withPartitionRange(PartitionRange.VALUE_RANGE_100K)
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withDataSources(Arrays.asList(
                DataSourceConfig.create("127.0.0.1", 3306, "test_db", "root", "123456")
            ))
            .build();

        assertNotNull(repository);
        // This would create tables like:
        // events_range_0     (0 - 99,999)
        // events_range_1     (100,000 - 199,999)
        // events_range_2     (200,000 - 299,999)
    }

    @Test
    public void testHashBasedPartitioning16Buckets() {
        // Test hash-based partitioning with 16 buckets
        ShardingRepository<Event> repository = SplitVerseRepository.<Event>builder()
            .withEntityClass(Event.class)
            .withTableName("events")
            .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_HASH)
            .withPartitionColumn("event_type", PartitionColumnType.STRING)
            .withPartitionRange(PartitionRange.HASH_16)
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withDataSources(Arrays.asList(
                DataSourceConfig.create("127.0.0.1", 3306, "test_db", "root", "123456")
            ))
            .build();

        assertNotNull(repository);
        // This would create tables like:
        // events_hash_00
        // events_hash_01
        // ...
        // events_hash_15
    }

    @Test
    public void testTimeBasedPartitioningHourly() {
        // Test hourly partitioning for high-volume data
        ShardingRepository<Event> repository = SplitVerseRepository.<Event>builder()
            .withEntityClass(Event.class)
            .withTableName("events")
            .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
            .withPartitionColumn("created_at", PartitionColumnType.LOCAL_DATE_TIME)
            .withPartitionRange(PartitionRange.HOURLY)
            .withRetentionDays(1) // Keep only 24 hours
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withDataSources(Arrays.asList(
                DataSourceConfig.create("127.0.0.1", 3306, "test_db", "root", "123456")
            ))
            .build();

        assertNotNull(repository);
        // This would create tables like:
        // events_2024_01_01_00
        // events_2024_01_01_01
        // ...
        // events_2024_01_01_23
    }

    @Test
    public void testTimeBasedPartitioningMonthly() {
        // Test monthly partitioning for long-term storage
        ShardingRepository<Event> repository = SplitVerseRepository.<Event>builder()
            .withEntityClass(Event.class)
            .withTableName("events")
            .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
            .withPartitionColumn("created_at", PartitionColumnType.LOCAL_DATE_TIME)
            .withPartitionRange(PartitionRange.MONTHLY)
            .withRetentionDays(365) // Keep one year
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withDataSources(Arrays.asList(
                DataSourceConfig.create("127.0.0.1", 3306, "test_db", "root", "123456")
            ))
            .build();

        assertNotNull(repository);
        // This would create tables like:
        // events_2024_01
        // events_2024_02
        // ...
        // events_2024_12
    }

    @Test
    public void testIntegerBasedPartitioning() {
        // Test partitioning with integer column
        ShardingRepository<Event> repository = SplitVerseRepository.<Event>builder()
            .withEntityClass(Event.class)
            .withTableName("events")
            .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
            .withPartitionColumn("status_code", PartitionColumnType.INTEGER)
            .withPartitionRange(PartitionRange.VALUE_RANGE_10K)
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withDataSources(Arrays.asList(
                DataSourceConfig.create("127.0.0.1", 3306, "test_db", "root", "123456")
            ))
            .build();

        assertNotNull(repository);
    }

    @Test
    public void testDoubleBasedPartitioning() {
        // Test partitioning with double column (e.g., for measurement data)
        ShardingRepository<Event> repository = SplitVerseRepository.<Event>builder()
            .withEntityClass(Event.class)
            .withTableName("measurements")
            .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
            .withPartitionColumn("temperature", PartitionColumnType.DOUBLE)
            .withPartitionRange(PartitionRange.VALUE_RANGE_1K)
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withDataSources(Arrays.asList(
                DataSourceConfig.create("127.0.0.1", 3306, "test_db", "root", "123456")
            ))
            .build();

        assertNotNull(repository);
    }
}
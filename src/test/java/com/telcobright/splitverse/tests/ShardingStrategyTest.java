package com.telcobright.splitverse.tests;

import com.telcobright.api.ShardingRepository;
import com.telcobright.core.config.DataSourceConfig;
import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.enums.PartitionColumnType;
import com.telcobright.core.enums.PartitionRange;
import com.telcobright.core.enums.ShardingStrategy;
import com.telcobright.core.repository.SplitVerseRepository;
import com.telcobright.splitverse.config.RepositoryMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for new ShardingStrategy design.
 */
public class ShardingStrategyTest {

    // Test entity implementing ShardingEntity
    public static class TestOrder implements ShardingEntity {
        private String id;
        private LocalDateTime createdAt;
        private String customerId;
        private Long sequenceNumber;
        private Double amount;

        @Override
        public String getId() { return id; }
        @Override
        public void setId(String id) { this.id = id; }
        @Override
        public LocalDateTime getCreatedAt() { return createdAt; }
        @Override
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public Long getSequenceNumber() { return sequenceNumber; }
        public void setSequenceNumber(Long sequenceNumber) { this.sequenceNumber = sequenceNumber; }
        public Double getAmount() { return amount; }
        public void setAmount(Double amount) { this.amount = amount; }
    }

    private List<DataSourceConfig> testDataSources;

    @BeforeEach
    public void setUp() {
        // Configure test data sources
        testDataSources = Arrays.asList(
            DataSourceConfig.create("127.0.0.1", 3306, "test_shard_1", "root", "123456")
        );
    }

    @Test
    public void testSingleKeyHashStrategy() {
        // Test SINGLE_KEY_HASH strategy - simplest case
        ShardingRepository<TestOrder> repository = SplitVerseRepository.<TestOrder>builder()
            .withEntityClass(TestOrder.class)
            .withTableName("orders")
            .withShardingStrategy(ShardingStrategy.SINGLE_KEY_HASH)
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withDataSources(testDataSources)
            .build();

        assertNotNull(repository);
    }

    @Test
    public void testDualKeyHashRangeStrategy_TimeBasedPartitioning() {
        // Test DUAL_KEY_HASH_RANGE with time-based partitioning
        ShardingRepository<TestOrder> repository = SplitVerseRepository.<TestOrder>builder()
            .withEntityClass(TestOrder.class)
            .withTableName("orders")
            .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
            .withPartitionColumn("created_at", PartitionColumnType.LOCAL_DATE_TIME)
            .withPartitionRange(PartitionRange.DAILY)
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withDataSources(testDataSources)
            .build();

        assertNotNull(repository);
    }

    @Test
    public void testDualKeyHashRangeStrategy_ValueBasedPartitioning() {
        // Test DUAL_KEY_HASH_RANGE with value-based partitioning
        ShardingRepository<TestOrder> repository = SplitVerseRepository.<TestOrder>builder()
            .withEntityClass(TestOrder.class)
            .withTableName("orders")
            .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
            .withPartitionColumn("sequence_number", PartitionColumnType.LONG)
            .withPartitionRange(PartitionRange.VALUE_RANGE_1M)
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withDataSources(testDataSources)
            .build();

        assertNotNull(repository);
    }

    @Test
    public void testDualKeyHashHashStrategy() {
        // Test DUAL_KEY_HASH_HASH with hash-based partitioning
        ShardingRepository<TestOrder> repository = SplitVerseRepository.<TestOrder>builder()
            .withEntityClass(TestOrder.class)
            .withTableName("orders")
            .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_HASH)
            .withPartitionColumn("customer_id", PartitionColumnType.STRING)
            .withPartitionRange(PartitionRange.HASH_16)
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withDataSources(testDataSources)
            .build();

        assertNotNull(repository);
    }

    @Test
    public void testMultipleDataSources() {
        // Test with multiple shards
        List<DataSourceConfig> multipleShards = Arrays.asList(
            DataSourceConfig.create("127.0.0.1", 3306, "shard1", "root", "123456"),
            DataSourceConfig.create("127.0.0.1", 3306, "shard2", "root", "123456"),
            DataSourceConfig.create("127.0.0.1", 3306, "shard3", "root", "123456")
        );

        ShardingRepository<TestOrder> repository = SplitVerseRepository.<TestOrder>builder()
            .withEntityClass(TestOrder.class)
            .withTableName("orders")
            .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
            .withPartitionColumn("created_at", PartitionColumnType.LOCAL_DATE_TIME)
            .withPartitionRange(PartitionRange.MONTHLY)
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withDataSources(multipleShards)
            .build();

        assertNotNull(repository);
    }

    @Test
    public void testIdSizeConfiguration() {
        // Test ID size configuration
        ShardingRepository<TestOrder> repository = SplitVerseRepository.<TestOrder>builder()
            .withEntityClass(TestOrder.class)
            .withTableName("orders")
            .withShardingStrategy(ShardingStrategy.SINGLE_KEY_HASH)
            .withIdSize(16) // 16-byte IDs
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withDataSources(testDataSources)
            .build();

        assertNotNull(repository);
    }

    @Test
    public void testRetentionConfiguration() {
        // Test retention configuration
        ShardingRepository<TestOrder> repository = SplitVerseRepository.<TestOrder>builder()
            .withEntityClass(TestOrder.class)
            .withTableName("orders")
            .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
            .withPartitionColumn("created_at", PartitionColumnType.LOCAL_DATE_TIME)
            .withPartitionRange(PartitionRange.HOURLY)
            .withRetentionDays(7) // Keep data for 7 days
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withDataSources(testDataSources)
            .build();

        assertNotNull(repository);
    }

    @Test
    public void testValidationErrors() {
        // Test missing partition column for DUAL_KEY strategy
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestOrder>builder()
                .withEntityClass(TestOrder.class)
                .withTableName("orders")
                .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
                // Missing: withPartitionColumn
                .withPartitionRange(PartitionRange.DAILY)
                .withRepositoryMode(RepositoryMode.MULTI_TABLE)
                .withDataSources(testDataSources)
                .build();
        });

        // Test missing partition range for DUAL_KEY strategy
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestOrder>builder()
                .withEntityClass(TestOrder.class)
                .withTableName("orders")
                .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
                .withPartitionColumn("created_at", PartitionColumnType.LOCAL_DATE_TIME)
                // Missing: withPartitionRange
                .withRepositoryMode(RepositoryMode.MULTI_TABLE)
                .withDataSources(testDataSources)
                .build();
        });

        // Test invalid ID size
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestOrder>builder()
                .withEntityClass(TestOrder.class)
                .withTableName("orders")
                .withShardingStrategy(ShardingStrategy.SINGLE_KEY_HASH)
                .withIdSize(30) // Invalid: must be 8-22
                .withRepositoryMode(RepositoryMode.MULTI_TABLE)
                .withDataSources(testDataSources)
                .build();
        });

        // Test missing data sources
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestOrder>builder()
                .withEntityClass(TestOrder.class)
                .withTableName("orders")
                .withShardingStrategy(ShardingStrategy.SINGLE_KEY_HASH)
                .withRepositoryMode(RepositoryMode.MULTI_TABLE)
                // Missing: withDataSources
                .build();
        });

        // Test empty data sources
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestOrder>builder()
                .withEntityClass(TestOrder.class)
                .withTableName("orders")
                .withShardingStrategy(ShardingStrategy.SINGLE_KEY_HASH)
                .withRepositoryMode(RepositoryMode.MULTI_TABLE)
                .withDataSources(Arrays.asList()) // Empty list
                .build();
        });
    }

    @Test
    public void testAllPartitionRanges() {
        // Test various partition ranges for time-based
        for (PartitionRange range : Arrays.asList(
            PartitionRange.HOURLY, PartitionRange.DAILY,
            PartitionRange.MONTHLY, PartitionRange.YEARLY)) {

            ShardingRepository<TestOrder> repository = SplitVerseRepository.<TestOrder>builder()
                .withEntityClass(TestOrder.class)
                .withTableName("orders_" + range)
                .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
                .withPartitionColumn("created_at", PartitionColumnType.LOCAL_DATE_TIME)
                .withPartitionRange(range)
                .withRepositoryMode(RepositoryMode.MULTI_TABLE)
                .withDataSources(testDataSources)
                .build();

            assertNotNull(repository, "Failed for range: " + range);
        }

        // Test hash-based ranges
        for (PartitionRange range : Arrays.asList(
            PartitionRange.HASH_4, PartitionRange.HASH_8,
            PartitionRange.HASH_16, PartitionRange.HASH_32)) {

            ShardingRepository<TestOrder> repository = SplitVerseRepository.<TestOrder>builder()
                .withEntityClass(TestOrder.class)
                .withTableName("orders_" + range)
                .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_HASH)
                .withPartitionColumn("customer_id", PartitionColumnType.STRING)
                .withPartitionRange(range)
                .withRepositoryMode(RepositoryMode.MULTI_TABLE)
                .withDataSources(testDataSources)
                .build();

            assertNotNull(repository, "Failed for range: " + range);
        }

        // Test value-based ranges
        for (PartitionRange range : Arrays.asList(
            PartitionRange.VALUE_RANGE_1K, PartitionRange.VALUE_RANGE_10K,
            PartitionRange.VALUE_RANGE_100K, PartitionRange.VALUE_RANGE_1M)) {

            ShardingRepository<TestOrder> repository = SplitVerseRepository.<TestOrder>builder()
                .withEntityClass(TestOrder.class)
                .withTableName("orders_" + range)
                .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
                .withPartitionColumn("sequence_number", PartitionColumnType.LONG)
                .withPartitionRange(range)
                .withRepositoryMode(RepositoryMode.MULTI_TABLE)
                .withDataSources(testDataSources)
                .build();

            assertNotNull(repository, "Failed for range: " + range);
        }
    }

    @Test
    public void testAllPartitionColumnTypes() {
        // Test different partition column types
        testPartitionColumnType(PartitionColumnType.LOCAL_DATE_TIME, PartitionRange.DAILY);
        testPartitionColumnType(PartitionColumnType.LONG, PartitionRange.VALUE_RANGE_1M);
        testPartitionColumnType(PartitionColumnType.INTEGER, PartitionRange.VALUE_RANGE_10K);
        testPartitionColumnType(PartitionColumnType.DOUBLE, PartitionRange.VALUE_RANGE_100K);
        testPartitionColumnType(PartitionColumnType.STRING, PartitionRange.HASH_16);
    }

    private void testPartitionColumnType(PartitionColumnType columnType, PartitionRange range) {
        String columnName = switch (columnType) {
            case LOCAL_DATE_TIME -> "created_at";
            case LONG -> "sequence_number";
            case INTEGER -> "status_code";
            case DOUBLE -> "amount";
            case STRING -> "customer_id";
        };

        ShardingStrategy strategy = (columnType == PartitionColumnType.STRING)
            ? ShardingStrategy.DUAL_KEY_HASH_HASH
            : ShardingStrategy.DUAL_KEY_HASH_RANGE;

        ShardingRepository<TestOrder> repository = SplitVerseRepository.<TestOrder>builder()
            .withEntityClass(TestOrder.class)
            .withTableName("orders_" + columnType)
            .withShardingStrategy(strategy)
            .withPartitionColumn(columnName, columnType)
            .withPartitionRange(range)
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withDataSources(testDataSources)
            .build();

        assertNotNull(repository, "Failed for column type: " + columnType);
    }
}
package com.telcobright.db.metadata;

import com.telcobright.db.test.entity.TestEntity;
import com.telcobright.db.test.entity.SimpleTestEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Comprehensive unit tests for EntityMetadata class
 */
@DisplayName("EntityMetadata Tests")
class EntityMetadataTest {
    
    @Test
    @DisplayName("Should parse entity metadata correctly for complex entity")
    void testParseComplexEntityMetadata() {
        // Given
        Class<TestEntity> entityClass = TestEntity.class;
        Class<Long> keyClass = Long.class;
        
        // When
        EntityMetadata<TestEntity, Long> metadata = new EntityMetadata<>(entityClass, keyClass);
        
        // Then
        assertThat(metadata.getTableName()).isEqualTo("test_data");
        
        // Check ID field
        assertThat(metadata.getIdField().getFieldName()).isEqualTo("id");
        assertThat(metadata.getIdField().getColumnName()).isEqualTo("id");
        
        // Check sharding key field
        assertThat(metadata.getShardingKeyField().getFieldName()).isEqualTo("createdAt");
        assertThat(metadata.getShardingKeyField().getColumnName()).isEqualTo("created_at");
        
        // Check all fields are present
        assertThat(metadata.getFields()).hasSize(10); // All fields including id and createdAt
        
        // Check specific field column mappings
        var nameField = metadata.getFields().stream()
            .filter(f -> f.getFieldName().equals("name"))
            .findFirst().orElse(null);
        assertThat(nameField).isNotNull();
        assertThat(nameField.getColumnName()).isEqualTo("name");
        
        var updatedAtField = metadata.getFields().stream()
            .filter(f -> f.getFieldName().equals("updatedAt"))
            .findFirst().orElse(null);
        assertThat(updatedAtField).isNotNull();
        assertThat(updatedAtField.getColumnName()).isEqualTo("updated_at");
    }
    
    @Test
    @DisplayName("Should parse entity metadata correctly for simple entity")
    void testParseSimpleEntityMetadata() {
        // Given
        Class<SimpleTestEntity> entityClass = SimpleTestEntity.class;
        Class<String> keyClass = String.class;
        
        // When
        EntityMetadata<SimpleTestEntity, String> metadata = new EntityMetadata<>(entityClass, keyClass);
        
        // Then
        assertThat(metadata.getTableName()).isEqualTo("simple_test");
        
        // Check ID field
        assertThat(metadata.getIdField().getFieldName()).isEqualTo("testId");
        assertThat(metadata.getIdField().getColumnName()).isEqualTo("test_id");
        
        // Check sharding key field
        assertThat(metadata.getShardingKeyField().getFieldName()).isEqualTo("timestamp");
        assertThat(metadata.getShardingKeyField().getColumnName()).isEqualTo("timestamp");
        
        // Check all fields are present
        assertThat(metadata.getFields()).hasSize(3);
    }
    
    @Test
    @DisplayName("Should generate correct INSERT SQL for complex entity")
    void testGenerateInsertSqlForComplexEntity() {
        // Given
        EntityMetadata<TestEntity, Long> metadata = new EntityMetadata<>(TestEntity.class, Long.class);
        
        // When
        String sql = String.format(metadata.getInsertSQL(), "test_data_20250808");
        
        // Then
        assertThat(sql).startsWith("INSERT INTO test_data_20250808");
        assertThat(sql).contains("name");
        assertThat(sql).contains("email");
        assertThat(sql).contains("status");
        assertThat(sql).contains("age");
        assertThat(sql).contains("balance");
        assertThat(sql).contains("active");
        assertThat(sql).contains("description");
        assertThat(sql).contains("created_at");
        assertThat(sql).contains("updated_at");
        assertThat(sql).doesNotContain("id"); // ID is not insertable
        
        // Count placeholders
        long placeholderCount = sql.chars().filter(ch -> ch == '?').count();
        assertThat(placeholderCount).isEqualTo(9); // All fields except ID
    }
    
    @Test
    @DisplayName("Should generate correct SELECT SQL")
    void testGenerateSelectSql() {
        // Given
        EntityMetadata<TestEntity, Long> metadata = new EntityMetadata<>(TestEntity.class, Long.class);
        
        // When
        String sql = String.format(metadata.getSelectByIdSQL(), "test_data_20250808");
        
        // Then
        assertThat(sql).startsWith("SELECT");
        assertThat(sql).contains("FROM test_data_20250808");
        assertThat(sql).contains("id");
        assertThat(sql).contains("name");
        assertThat(sql).contains("email");
        assertThat(sql).contains("created_at");
        assertThat(sql).contains("updated_at");
    }
    
    @Test
    @DisplayName("Should generate correct UPDATE SQL")
    void testGenerateUpdateSql() {
        // Given
        EntityMetadata<TestEntity, Long> metadata = new EntityMetadata<>(TestEntity.class, Long.class);
        
        // When
        String sql = String.format(metadata.getUpdateByIdSQL(), "test_data_20250808");
        
        // Then
        assertThat(sql).startsWith("UPDATE test_data_20250808 SET");
        assertThat(sql).contains("WHERE id = ?");
        assertThat(sql).contains("name = ?");
        assertThat(sql).contains("email = ?");
        assertThat(sql).contains("WHERE id = ?"); // ID is in WHERE clause
        assertThat(sql).doesNotContain("SET id = ?"); // ID is not in SET clause
        assertThat(sql).doesNotContain("created_at = ?"); // created_at is not updatable
    }
    
    @Test
    @DisplayName("Should generate correct CREATE TABLE SQL with indexes")
    void testGenerateCreateTableSql() {
        // Given
        EntityMetadata<TestEntity, Long> metadata = new EntityMetadata<>(TestEntity.class, Long.class);
        
        // When
        String sql = String.format(metadata.getCreateTableSQL(), "test_data_20250808");
        
        // Then
        assertThat(sql).startsWith("CREATE TABLE IF NOT EXISTS test_data_20250808");
        assertThat(sql).contains("id BIGINT PRIMARY KEY AUTO_INCREMENT");
        assertThat(sql).contains("name VARCHAR(255) NOT NULL");
        assertThat(sql).contains("email VARCHAR(255)");
        assertThat(sql).contains("created_at DATETIME NOT NULL");
        assertThat(sql).contains("balance DECIMAL(10,2)");
        assertThat(sql).contains("active BOOLEAN");
        // Primary key is already defined in column definition
        assertThat(sql).contains("PRIMARY KEY AUTO_INCREMENT");
        
        // Check indexes
        assertThat(sql).contains("KEY idx_name (name)");
        assertThat(sql).contains("UNIQUE KEY idx_email_unique (email)");
        assertThat(sql).contains("KEY idx_status_search (status) COMMENT 'Fast status filtering'");
    }
    
    @Test
    @DisplayName("Should correctly set parameters for entity")
    void testSetParameters() throws Exception {
        // Given
        EntityMetadata<TestEntity, Long> metadata = new EntityMetadata<>(TestEntity.class, Long.class);
        TestEntity entity = new TestEntity(
            "John Doe",
            "john@example.com", 
            "ACTIVE",
            25,
            new BigDecimal("1000.00"),
            true,
            "Test description",
            LocalDateTime.now()
        );
        
        // Mock PreparedStatement would be needed for full test
        // This tests the parameter setting logic preparation
        // When/Then
        assertThat(metadata.getInsertSQL()).isNotNull();
        // Note: Full parameter setting test would require mock PreparedStatement
    }
    
    @Test
    @DisplayName("Should map result set to entity")
    void testMapEntity() throws Exception {
        // Given
        EntityMetadata<TestEntity, Long> metadata = new EntityMetadata<>(TestEntity.class, Long.class);
        
        // Mock ResultSet would be needed for full test
        // This tests the entity mapping logic preparation
        // When/Then
        assertThat(metadata.getSelectByIdSQL()).isNotNull();
        // Note: Full result set mapping test would require mock ResultSet
    }
    
    @ParameterizedTest
    @ValueSource(strings = {"test_table", "users_20250808", "events_20250809"})
    @DisplayName("Should generate SQL for various table names")
    void testSqlGenerationWithVariousTableNames(String tableName) {
        // Given
        EntityMetadata<TestEntity, Long> metadata = new EntityMetadata<>(TestEntity.class, Long.class);
        
        // When
        String insertSql = String.format(metadata.getInsertSQL(), tableName);
        String selectSql = String.format(metadata.getSelectByIdSQL(), tableName);
        String updateSql = String.format(metadata.getUpdateByIdSQL(), tableName);
        String createSql = String.format(metadata.getCreateTableSQL(), tableName);
        
        // Then
        assertThat(insertSql).contains(tableName);
        assertThat(selectSql).contains(tableName);
        assertThat(updateSql).contains(tableName);
        assertThat(createSql).startsWith("CREATE TABLE IF NOT EXISTS " + tableName);
    }
    
    @Test
    @DisplayName("Should handle nullable and non-nullable fields correctly")
    void testNullableFieldHandling() {
        // Given
        EntityMetadata<TestEntity, Long> metadata = new EntityMetadata<>(TestEntity.class, Long.class);
        
        // When
        String createSql = String.format(metadata.getCreateTableSQL(), "test_table");
        
        // Then
        // name is marked as nullable = false
        assertThat(createSql).contains("name VARCHAR(255) NOT NULL");
        // created_at is marked as nullable = false
        assertThat(createSql).contains("created_at DATETIME NOT NULL");
        // email is nullable (default)
        assertThat(createSql).contains("email VARCHAR(255)");
        assertThat(createSql).doesNotContain("email VARCHAR(255) NOT NULL");
    }
    
    @Test
    @DisplayName("Should handle insertable and updatable constraints correctly")
    void testInsertableUpdatableConstraints() {
        // Given
        EntityMetadata<TestEntity, Long> metadata = new EntityMetadata<>(TestEntity.class, Long.class);
        
        // When
        String insertSql = String.format(metadata.getInsertSQL(), "test_table");
        String updateSql = String.format(metadata.getUpdateByIdSQL(), "test_table");
        
        // Then
        // id is insertable = false, updatable = false
        assertThat(insertSql).doesNotContain("id");
        assertThat(updateSql).doesNotContain("SET id = ?"); // ID not in SET clause
        assertThat(updateSql).contains("WHERE id = ?"); // but ID is in WHERE clause
        
        // created_at is updatable = false
        assertThat(insertSql).contains("created_at");
        assertThat(updateSql).doesNotContain("created_at = ?");
        
        // Other fields should be in both
        assertThat(insertSql).contains("name");
        assertThat(updateSql).contains("name = ?");
    }
}
package com.telcobright.db.application;

import com.telcobright.db.connection.ConnectionProvider;
import com.telcobright.db.metadata.EntityMetadata;
import com.telcobright.db.test.entity.TestEntity;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Essential Features Test - Direct database operations testing core functionality
 * Tests the most basic essential features without complex repository initialization
 */
@Testcontainers
@DisplayName("Essential Features Test - Core Database Operations")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class EssentialFeaturesTest {
    
    private static final Logger logger = LoggerFactory.getLogger(EssentialFeaturesTest.class);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    @Container
    static final MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0.33")
            .withDatabaseName("essential_test_db")
            .withUsername("test_user")
            .withPassword("test_pass");
    
    private static ConnectionProvider connectionProvider;
    private static EntityMetadata<TestEntity, Long> entityMetadata;
    
    // Test data
    private static final List<TestEntity> TEST_ENTITIES = new ArrayList<>();
    private static final LocalDateTime BASE_TIME = LocalDateTime.now().minusDays(1);
    
    @BeforeAll
    static void setupEssentialTest() {
        logger.info("=== ESSENTIAL FEATURES TEST SETUP ===");
        
        String host = mysql.getHost();
        int port = mysql.getFirstMappedPort();
        String database = mysql.getDatabaseName();
        String username = mysql.getUsername();
        String password = mysql.getPassword();
        
        logger.info("MySQL Container: {}:{}/{}", host, port, database);
        
        // Initialize connection provider
        connectionProvider = new ConnectionProvider(host, port, database, username, password);
        
        // Initialize entity metadata for direct SQL operations
        entityMetadata = new EntityMetadata<>(TestEntity.class, Long.class);
        
        // Create test data
        createTestData();
        
        logger.info("Essential features test setup completed");
    }
    
    private static void createTestData() {
        logger.info("Creating test data...");
        
        // Create entities for testing
        for (int i = 0; i < 4; i++) {
            LocalDateTime timestamp = BASE_TIME.plusHours(i * 6);
            
            TestEntity entity = new TestEntity(
                "EssentialUser" + i,
                "essential" + i + "@test.com",
                "ACTIVE",
                25 + i,
                new BigDecimal("1000.00").add(new BigDecimal(i * 250)),
                true,
                "Essential test entity " + i,
                timestamp
            );
            TEST_ENTITIES.add(entity);
        }
        
        logger.info("Created {} test entities", TEST_ENTITIES.size());
    }
    
    @Test
    @Order(1)
    @DisplayName("1. Database Connection Test")
    void testDatabaseConnection() throws Exception {
        logger.info("=== TESTING DATABASE CONNECTION ===");
        
        try (Connection conn = connectionProvider.getConnection()) {
            assertThat(conn).isNotNull();
            assertThat(conn.isClosed()).isFalse();
            
            // Test basic query
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT DATABASE(), USER(), VERSION()")) {
                
                assertThat(rs.next()).isTrue();
                String currentDb = rs.getString(1);
                String currentUser = rs.getString(2);
                String version = rs.getString(3);
                
                logger.info("Connected to database: {} as user: {} (MySQL {})", 
                    currentDb, currentUser, version);
                
                assertThat(currentDb).isEqualTo(mysql.getDatabaseName());
            }
        }
        
        logger.info("✅ Database connection test successful");
    }
    
    @Test
    @Order(2)
    @DisplayName("2. Create Table Test")
    void testTableCreation() throws Exception {
        logger.info("=== TESTING TABLE CREATION ===");
        
        String tableName = "essential_test_" + BASE_TIME.toLocalDate().format(DATE_FORMAT);
        
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Generate CREATE TABLE SQL
            String createTableSQL = String.format(entityMetadata.getCreateTableSQL(), tableName);
            logger.info("Creating table with SQL: {}", createTableSQL);
            
            // Execute CREATE TABLE
            stmt.executeUpdate(createTableSQL);
            
            // Verify table was created
            try (ResultSet rs = conn.getMetaData().getTables(null, mysql.getDatabaseName(), tableName, null)) {
                assertThat(rs.next()).isTrue();
                String createdTable = rs.getString("TABLE_NAME");
                assertThat(createdTable).isEqualTo(tableName);
                
                logger.info("✓ Successfully created table: {}", createdTable);
            }
            
            // Verify table structure
            verifyTableStructure(conn, tableName);
        }
        
        logger.info("✅ Table creation test successful");
    }
    
    private void verifyTableStructure(Connection conn, String tableName) throws SQLException {
        logger.info("Verifying structure of table: {}", tableName);
        
        Set<String> columns = new HashSet<>();
        try (ResultSet rs = conn.getMetaData().getColumns(null, mysql.getDatabaseName(), tableName, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String columnType = rs.getString("TYPE_NAME");
                columns.add(columnName);
                logger.debug("Column: {} {}", columnName, columnType);
            }
        }
        
        // Verify essential columns
        assertThat(columns).contains("id", "name", "email", "status", "age", 
            "balance", "active", "description", "created_at", "updated_at");
            
        logger.info("✓ Table structure verification completed");
    }
    
    @Test
    @Order(3)
    @DisplayName("3. Insert Operations Test")
    void testInsertOperations() throws Exception {
        logger.info("=== TESTING INSERT OPERATIONS ===");
        
        String tableName = "essential_test_" + BASE_TIME.toLocalDate().format(DATE_FORMAT);
        
        try (Connection conn = connectionProvider.getConnection()) {
            String insertSQL = String.format(entityMetadata.getInsertSQL(), tableName);
            logger.info("Insert SQL: {}", insertSQL);
            
            int successfulInserts = 0;
            
            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS)) {
                
                for (TestEntity entity : TEST_ENTITIES) {
                    // Set parameters using entity metadata
                    entityMetadata.setInsertParameters(pstmt, entity);
                    
                    int affectedRows = pstmt.executeUpdate();
                    assertThat(affectedRows).isEqualTo(1);
                    
                    // Get generated ID
                    try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                        assertThat(generatedKeys.next()).isTrue();
                        long generatedId = generatedKeys.getLong(1);
                        entity.setId(generatedId);
                        
                        logger.debug("✓ Inserted entity: ID={}, Name={}", generatedId, entity.getName());
                        successfulInserts++;
                    }
                }
            }
            
            assertThat(successfulInserts).isEqualTo(TEST_ENTITIES.size());
            logger.info("✅ Successfully inserted {} entities", successfulInserts);
        }
    }
    
    @Test
    @Order(4)
    @DisplayName("4. Read Operations Test") 
    void testReadOperations() throws Exception {
        logger.info("=== TESTING READ OPERATIONS ===");
        
        String tableName = "essential_test_" + BASE_TIME.toLocalDate().format(DATE_FORMAT);
        
        try (Connection conn = connectionProvider.getConnection()) {
            String selectSQL = String.format(entityMetadata.getSelectByIdSQL(), tableName);
            logger.info("Select SQL: {}", selectSQL);
            
            int successfulReads = 0;
            
            try (PreparedStatement pstmt = conn.prepareStatement(selectSQL)) {
                
                for (TestEntity originalEntity : TEST_ENTITIES) {
                    Long id = originalEntity.getId();
                    assertThat(id).isNotNull();
                    
                    pstmt.setLong(1, id);
                    
                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            TestEntity retrieved = entityMetadata.mapResultSet(rs);
                            
                            // Verify data integrity
                            assertThat(retrieved.getId()).isEqualTo(originalEntity.getId());
                            assertThat(retrieved.getName()).isEqualTo(originalEntity.getName());
                            assertThat(retrieved.getEmail()).isEqualTo(originalEntity.getEmail());
                            assertThat(retrieved.getStatus()).isEqualTo(originalEntity.getStatus());
                            assertThat(retrieved.getBalance()).isEqualByComparingTo(originalEntity.getBalance());
                            
                            logger.debug("✓ Retrieved entity: ID={}, Name={}", id, retrieved.getName());
                            successfulReads++;
                        }
                    }
                }
            }
            
            assertThat(successfulReads).isEqualTo(TEST_ENTITIES.size());
            logger.info("✅ Successfully read {} entities", successfulReads);
        }
    }
    
    @Test
    @Order(5)
    @DisplayName("5. Update Operations Test")
    void testUpdateOperations() throws Exception {
        logger.info("=== TESTING UPDATE OPERATIONS ===");
        
        String tableName = "essential_test_" + BASE_TIME.toLocalDate().format(DATE_FORMAT);
        
        try (Connection conn = connectionProvider.getConnection()) {
            String updateSQL = String.format(entityMetadata.getUpdateByIdSQL(), tableName);
            logger.info("Update SQL: {}", updateSQL);
            
            int successfulUpdates = 0;
            
            try (PreparedStatement pstmt = conn.prepareStatement(updateSQL)) {
                
                for (TestEntity entity : TEST_ENTITIES) {
                    // Modify entity data
                    String updatedName = entity.getName() + "_UPDATED";
                    entity.setName(updatedName);
                    entity.setStatus("UPDATED");
                    
                    // Set parameters using entity metadata
                    entityMetadata.setUpdateParameters(pstmt, entity, entity.getId());
                    
                    int affectedRows = pstmt.executeUpdate();
                    assertThat(affectedRows).isEqualTo(1);
                    
                    logger.debug("✓ Updated entity: ID={}, NewName={}", entity.getId(), updatedName);
                    successfulUpdates++;
                }
            }
            
            assertThat(successfulUpdates).isEqualTo(TEST_ENTITIES.size());
            logger.info("✅ Successfully updated {} entities", successfulUpdates);
        }
    }
    
    @Test
    @Order(6)
    @DisplayName("6. Data Verification After Updates")
    void testDataVerificationAfterUpdates() throws Exception {
        logger.info("=== VERIFYING DATA AFTER UPDATES ===");
        
        String tableName = "essential_test_" + BASE_TIME.toLocalDate().format(DATE_FORMAT);
        
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY id")) {
            
            int verifiedRecords = 0;
            
            while (rs.next()) {
                TestEntity entity = entityMetadata.mapResultSet(rs);
                
                // Verify updates were applied
                assertThat(entity.getName()).endsWith("_UPDATED");
                assertThat(entity.getStatus()).isEqualTo("UPDATED");
                assertThat(entity.getId()).isNotNull();
                
                logger.debug("✓ Verified updated entity: ID={}, Name={}", 
                    entity.getId(), entity.getName());
                verifiedRecords++;
            }
            
            assertThat(verifiedRecords).isEqualTo(TEST_ENTITIES.size());
            logger.info("✅ Verified {} updated records", verifiedRecords);
        }
    }
    
    @Test
    @Order(7)
    @DisplayName("7. Table Management - Multiple Tables")
    void testMultipleTableManagement() throws Exception {
        logger.info("=== TESTING MULTIPLE TABLE MANAGEMENT ===");
        
        // Create tables for different dates
        List<String> tableNames = new ArrayList<>();
        LocalDateTime testDate = BASE_TIME;
        
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Create tables for 3 different days
            for (int i = 0; i < 3; i++) {
                String dateStr = testDate.plusDays(i).toLocalDate().format(DATE_FORMAT);
                String tableName = "multi_test_" + dateStr;
                
                String createTableSQL = String.format(entityMetadata.getCreateTableSQL(), tableName);
                stmt.executeUpdate(createTableSQL);
                
                tableNames.add(tableName);
                logger.info("✓ Created table: {}", tableName);
            }
            
            // Verify all tables exist
            try (ResultSet rs = conn.getMetaData().getTables(null, mysql.getDatabaseName(), 
                "multi_test_%", new String[]{"TABLE"})) {
                
                Set<String> foundTables = new HashSet<>();
                while (rs.next()) {
                    foundTables.add(rs.getString("TABLE_NAME"));
                }
                
                for (String expectedTable : tableNames) {
                    assertThat(foundTables).contains(expectedTable);
                }
                
                logger.info("✓ Verified {} tables exist", foundTables.size());
            }
        }
        
        logger.info("✅ Multiple table management test successful");
    }
    
    @Test
    @Order(8)
    @DisplayName("8. Connection Provider Features")
    void testConnectionProviderFeatures() throws Exception {
        logger.info("=== TESTING CONNECTION PROVIDER FEATURES ===");
        
        // Test maintenance mode status
        assertThat(connectionProvider.isMaintenanceInProgress()).isFalse();
        assertThat(connectionProvider.getMaintenanceReason()).isEmpty();
        logger.info("✓ Maintenance status check passed");
        
        // Test multiple connections
        List<Connection> connections = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                Connection conn = connectionProvider.getConnection();
                assertThat(conn).isNotNull();
                assertThat(conn.isClosed()).isFalse();
                connections.add(conn);
            }
            
            logger.info("✓ Successfully obtained {} connections", connections.size());
            
        } finally {
            // Clean up connections
            for (Connection conn : connections) {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            }
            logger.info("✓ Closed all test connections");
        }
        
        logger.info("✅ Connection provider features test successful");
    }
    
    @Test
    @Order(9)
    @DisplayName("9. Final Data Integrity Check")
    void testFinalDataIntegrity() throws Exception {
        logger.info("=== FINAL DATA INTEGRITY CHECK ===");
        
        // Count all data across all created tables
        int totalRecords = 0;
        List<String> allTables = new ArrayList<>();
        
        try (Connection conn = connectionProvider.getConnection();
             ResultSet rs = conn.getMetaData().getTables(null, mysql.getDatabaseName(), 
                "%test_%", new String[]{"TABLE"})) {
            
            while (rs.next()) {
                allTables.add(rs.getString("TABLE_NAME"));
            }
        }
        
        logger.info("Found {} test tables", allTables.size());
        
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            
            for (String table : allTables) {
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                    if (rs.next()) {
                        int count = rs.getInt(1);
                        totalRecords += count;
                        logger.info("Table {} contains {} records", table, count);
                    }
                }
            }
        }
        
        logger.info("Total records across all tables: {}", totalRecords);
        assertThat(totalRecords).isGreaterThan(0);
        
        // Verify data quality on main test table
        String mainTable = "essential_test_" + BASE_TIME.toLocalDate().format(DATE_FORMAT);
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total, " +
                "COUNT(DISTINCT id) as unique_ids, " +
                "COUNT(CASE WHEN name IS NOT NULL THEN 1 END) as non_null_names " +
                "FROM " + mainTable)) {
            
            if (rs.next()) {
                int total = rs.getInt("total");
                int uniqueIds = rs.getInt("unique_ids");
                int nonNullNames = rs.getInt("non_null_names");
                
                assertThat(total).isEqualTo(uniqueIds); // All IDs should be unique
                assertThat(nonNullNames).isEqualTo(total); // All names should be non-null
                
                logger.info("Data quality check passed: {} total, {} unique IDs, {} non-null names", 
                    total, uniqueIds, nonNullNames);
            }
        }
        
        logger.info("✅ Final data integrity check completed successfully");
    }
    
    @AfterAll
    static void cleanupEssentialTest() {
        logger.info("=== ESSENTIAL FEATURES TEST CLEANUP ===");
        
        try {
            if (connectionProvider != null) {
                connectionProvider.shutdown();
            }
            logger.info("Essential features test cleanup completed");
        } catch (Exception e) {
            logger.error("Error during cleanup", e);
        }
    }
}
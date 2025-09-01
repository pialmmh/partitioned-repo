package com.telcobright.db.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Base class for integration tests using TestContainers MySQL
 */
@Testcontainers
public abstract class BaseIntegrationTest {
    
    private static final Logger logger = LoggerFactory.getLogger(BaseIntegrationTest.class);
    
    @Container
    public static final MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0.33")
            .withDatabaseName("test_db")
            .withUsername("test_user")
            .withPassword("test_password")
            .withReuse(true);
    
    protected static String host;
    protected static int port;
    protected static String database;
    protected static String username;
    protected static String password;
    protected static String jdbcUrl;
    
    @BeforeAll
    static void setUpDatabase() {
        mysql.start();
        
        host = mysql.getHost();
        port = mysql.getFirstMappedPort();
        database = mysql.getDatabaseName();
        username = mysql.getUsername();
        password = mysql.getPassword();
        jdbcUrl = mysql.getJdbcUrl();
        
        logger.info("MySQL Container started:");
        logger.info("  Host: {}", host);
        logger.info("  Port: {}", port);
        logger.info("  Database: {}", database);
        logger.info("  JDBC URL: {}", jdbcUrl);
        
        // Create additional test databases using the main connection
        String connectionUrl = jdbcUrl;
        if (!connectionUrl.contains("allowPublicKeyRetrieval")) {
            connectionUrl += connectionUrl.contains("?") ? "&" : "?";
            connectionUrl += "allowPublicKeyRetrieval=true";
        }
        
        try (Connection conn = DriverManager.getConnection(connectionUrl, username, password);
             Statement stmt = conn.createStatement()) {
            
            // Create databases
            stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS test_multi");
            stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS test_partitioned");
            stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS test_monitoring");
            
            // The test user already has permissions within the container
            logger.info("Test databases created successfully (using container user)");
            
            logger.info("Additional test databases created and permissions granted successfully");
            
        } catch (SQLException e) {
            logger.error("Failed to create additional test databases", e);
            throw new RuntimeException("Database setup failed", e);
        }
    }
    
    @AfterAll
    static void tearDownDatabase() {
        if (mysql.isRunning()) {
            mysql.stop();
            logger.info("MySQL Container stopped");
        }
    }
    
    /**
     * Get connection to main test database
     */
    protected static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }
    
    /**
     * Get connection to specific database
     */
    protected static Connection getConnection(String databaseName) throws SQLException {
        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true", 
                                  host, port, databaseName);
        return DriverManager.getConnection(url, username, password);
    }
    
    /**
     * Execute SQL statement on main test database
     */
    protected static void executeSql(String sql) throws SQLException {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }
    
    /**
     * Execute SQL statement on specific database
     */
    protected static void executeSql(String databaseName, String sql) throws SQLException {
        try (Connection conn = getConnection(databaseName);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }
    
    /**
     * Clean up all tables in test database
     */
    protected static void cleanupDatabase() throws SQLException {
        cleanupDatabase("test_db");
    }
    
    /**
     * Clean up all tables in specified database
     */
    protected static void cleanupDatabase(String databaseName) throws SQLException {
        try (Connection conn = getConnection(databaseName);
             Statement stmt = conn.createStatement()) {
            
            // Disable foreign key checks
            stmt.executeUpdate("SET FOREIGN_KEY_CHECKS = 0");
            
            // Get all table names
            var rs = stmt.executeQuery("SHOW TABLES");
            while (rs.next()) {
                String tableName = rs.getString(1);
                stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
                logger.debug("Dropped table: {}", tableName);
            }
            
            // Re-enable foreign key checks
            stmt.executeUpdate("SET FOREIGN_KEY_CHECKS = 1");
            
            logger.info("Cleaned up all tables in database: {}", databaseName);
        }
    }
    
    /**
     * Get table count in database
     */
    protected static int getTableCount(String databaseName) throws SQLException {
        try (Connection conn = getConnection(databaseName);
             Statement stmt = conn.createStatement()) {
            
            var rs = stmt.executeQuery("SELECT COUNT(*) FROM information_schema.tables " +
                                     "WHERE table_schema = '" + databaseName + "'");
            return rs.next() ? rs.getInt(1) : 0;
        }
    }
    
    /**
     * Check if table exists in database
     */
    protected static boolean tableExists(String databaseName, String tableName) throws SQLException {
        try (Connection conn = getConnection(databaseName);
             Statement stmt = conn.createStatement()) {
            
            var rs = stmt.executeQuery("SELECT COUNT(*) FROM information_schema.tables " +
                                     "WHERE table_schema = '" + databaseName + "' " +
                                     "AND table_name = '" + tableName + "'");
            return rs.next() && rs.getInt(1) > 0;
        }
    }
}
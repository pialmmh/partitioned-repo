package com.telcobright.db.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * Connection provider using plain JDBC with maintenance locking mechanism.
 * During maintenance operations, all CRUD operations are blocked.
 */
public class ConnectionProvider {
    private static final Logger LOGGER = Logger.getLogger(ConnectionProvider.class.getName());
    
    private final String jdbcUrl;
    private final String username;
    private final String password;
    
    // Read-write lock for maintenance operations
    // Read lock: Normal CRUD operations can proceed concurrently
    // Write lock: Maintenance operations have exclusive access
    private final ReadWriteLock maintenanceLock = new ReentrantReadWriteLock(true);
    private volatile boolean maintenanceInProgress = false;
    private volatile String maintenanceReason = "";
    
    /**
     * Create a new ConnectionProvider
     */
    public ConnectionProvider(String host, int port, String database, String username, String password) {
        this.username = username;
        this.password = password;
        
        // Build JDBC URL with timezone settings to prevent LocalDateTime conversion issues
        this.jdbcUrl = String.format(
            "jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC&useLegacyDatetimeCode=false&preserveInstants=true",
            host, port, database
        );
        
        // Load MySQL driver
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("MySQL JDBC driver not found", e);
        }
        
        LOGGER.info("ConnectionProvider initialized for: " + jdbcUrl);
    }
    
    /**
     * Get a connection for normal CRUD operations.
     * This will block if maintenance is in progress.
     * 
     * @return A new database connection
     * @throws SQLException if connection cannot be established
     * @throws MaintenanceInProgressException if maintenance is actively running
     */
    public Connection getConnection() throws SQLException {
        // Try to acquire read lock (will block if maintenance has write lock)
        maintenanceLock.readLock().lock();
        try {
            if (maintenanceInProgress) {
                throw new MaintenanceInProgressException(
                    "Database connections are blocked for maintenance: " + maintenanceReason
                );
            }
            
            return createConnection();
        } finally {
            maintenanceLock.readLock().unlock();
        }
    }
    
    /**
     * Get a connection for maintenance operations.
     * This will acquire exclusive access and block all other operations.
     * 
     * @param reason Description of the maintenance operation
     * @return A maintenance connection holder that must be closed to release the lock
     */
    public MaintenanceConnection getMaintenanceConnection(String reason) throws SQLException {
        LOGGER.info("Acquiring maintenance lock for: " + reason);
        
        // Acquire write lock (will wait for all read locks to be released)
        maintenanceLock.writeLock().lock();
        maintenanceInProgress = true;
        maintenanceReason = reason;
        
        try {
            Connection conn = createConnection();
            return new MaintenanceConnection(conn, this);
        } catch (SQLException e) {
            // Release lock if connection fails
            releaseMaintenanceLock();
            throw e;
        }
    }
    
    /**
     * Create a new JDBC connection
     */
    private Connection createConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }
    
    /**
     * Release the maintenance lock (called by MaintenanceConnection.close())
     */
    void releaseMaintenanceLock() {
        maintenanceInProgress = false;
        maintenanceReason = "";
        maintenanceLock.writeLock().unlock();
        LOGGER.info("Maintenance lock released");
    }
    
    /**
     * Check if maintenance is currently in progress
     */
    public boolean isMaintenanceInProgress() {
        return maintenanceInProgress;
    }
    
    /**
     * Get the reason for current maintenance (if any)
     */
    public String getMaintenanceReason() {
        return maintenanceReason;
    }
    
    /**
     * Shutdown the connection provider
     */
    public void shutdown() {
        LOGGER.info("ConnectionProvider shutdown");
    }
    
    /**
     * Builder for ConnectionProvider
     */
    public static class Builder {
        private String host = "localhost";
        private int port = 3306;
        private String database;
        private String username;
        private String password;
        
        public Builder host(String host) {
            this.host = host;
            return this;
        }
        
        public Builder port(int port) {
            this.port = port;
            return this;
        }
        
        public Builder database(String database) {
            this.database = database;
            return this;
        }
        
        public Builder username(String username) {
            this.username = username;
            return this;
        }
        
        public Builder password(String password) {
            this.password = password;
            return this;
        }
        
        public ConnectionProvider build() {
            if (database == null || username == null || password == null) {
                throw new IllegalArgumentException("Database, username, and password are required");
            }
            return new ConnectionProvider(host, port, database, username, password);
        }
    }
    
    /**
     * Wrapper for maintenance connections that ensures lock is released
     */
    public static class MaintenanceConnection implements AutoCloseable {
        private final Connection connection;
        private final ConnectionProvider provider;
        private boolean closed = false;
        
        MaintenanceConnection(Connection connection, ConnectionProvider provider) {
            this.connection = connection;
            this.provider = provider;
        }
        
        public Connection getConnection() {
            if (closed) {
                throw new IllegalStateException("Maintenance connection is closed");
            }
            return connection;
        }
        
        @Override
        public void close() {
            if (!closed) {
                closed = true;
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOGGER.warning("Error closing maintenance connection: " + e.getMessage());
                } finally {
                    provider.releaseMaintenanceLock();
                }
            }
        }
    }
    
    /**
     * Exception thrown when trying to get a connection during maintenance
     */
    public static class MaintenanceInProgressException extends SQLException {
        public MaintenanceInProgressException(String message) {
            super(message);
        }
    }
}
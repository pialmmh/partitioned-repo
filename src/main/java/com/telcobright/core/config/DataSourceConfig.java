package com.telcobright.core.config;

/**
 * Configuration for a data source connection.
 * Immutable configuration object for database connections.
 */
public class DataSourceConfig {
    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private final String password;

    private DataSourceConfig(String host, int port, String database, String username, String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    /**
     * Create a data source configuration with default credentials.
     */
    public static DataSourceConfig create(String host, int port, String database) {
        return create(host, port, database, "root", "");
    }

    /**
     * Create a data source configuration with custom credentials.
     */
    public static DataSourceConfig create(String host, int port, String database, String username, String password) {
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("Host cannot be null or empty");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        if (database == null || database.trim().isEmpty()) {
            throw new IllegalArgumentException("Database cannot be null or empty");
        }
        if (username == null) {
            username = "root";
        }
        if (password == null) {
            password = "";
        }
        return new DataSourceConfig(host, port, database, username, password);
    }

    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getDatabase() { return database; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }

    @Override
    public String toString() {
        return String.format("DataSource[%s:%d/%s]", host, port, database);
    }
}
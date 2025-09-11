package com.telcobright.splitverse.config;

/**
 * Configuration for a single shard in the Split-Verse system.
 * Each shard represents a MySQL instance with its own partitioned-repo.
 */
public class ShardConfig {
    private final String shardId;
    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private final String password;
    private final int connectionPoolSize;
    private final boolean enabled;
    
    private ShardConfig(Builder builder) {
        this.shardId = builder.shardId;
        this.host = builder.host;
        this.port = builder.port;
        this.database = builder.database;
        this.username = builder.username;
        this.password = builder.password;
        this.connectionPoolSize = builder.connectionPoolSize;
        this.enabled = builder.enabled;
    }
    
    // Getters
    public String getShardId() { return shardId; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getDatabase() { return database; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public int getConnectionPoolSize() { return connectionPoolSize; }
    public boolean isEnabled() { return enabled; }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String shardId;
        private String host = "127.0.0.1";
        private int port = 3306;
        private String database;
        private String username = "root";
        private String password;
        private int connectionPoolSize = 10;
        private boolean enabled = true;
        
        public Builder shardId(String shardId) {
            this.shardId = shardId;
            return this;
        }
        
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
        
        public Builder connectionPoolSize(int connectionPoolSize) {
            this.connectionPoolSize = connectionPoolSize;
            return this;
        }
        
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }
        
        public ShardConfig build() {
            if (shardId == null || shardId.isEmpty()) {
                throw new IllegalArgumentException("Shard ID is required");
            }
            if (database == null || database.isEmpty()) {
                throw new IllegalArgumentException("Database name is required");
            }
            if (password == null) {
                throw new IllegalArgumentException("Password is required");
            }
            return new ShardConfig(this);
        }
    }
    
    @Override
    public String toString() {
        return String.format("ShardConfig{id='%s', host='%s:%d', database='%s', enabled=%s}",
            shardId, host, port, database, enabled);
    }
}
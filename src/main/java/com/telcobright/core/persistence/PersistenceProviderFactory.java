package com.telcobright.core.persistence;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Factory for creating and managing PersistenceProvider instances.
 * Uses singleton pattern to ensure single instance per database type.
 *
 * @author Split-Verse Framework
 */
public class PersistenceProviderFactory {

    private static final Map<PersistenceProvider.DatabaseType, PersistenceProvider> providers
        = new ConcurrentHashMap<>();

    /**
     * Private constructor to prevent instantiation
     */
    private PersistenceProviderFactory() {
    }

    /**
     * Get persistence provider for the specified database type.
     * Uses singleton pattern - only one instance per database type.
     *
     * @param databaseType Type of database
     * @return PersistenceProvider instance
     * @throws UnsupportedOperationException if database type is not supported
     */
    public static PersistenceProvider getProvider(PersistenceProvider.DatabaseType databaseType) {
        return providers.computeIfAbsent(databaseType, type -> {
            switch (type) {
                case MYSQL:
                case MARIADB:  // MariaDB is MySQL-compatible
                case TIDB:     // TiDB is MySQL-compatible
                    return new MySQLPersistenceProvider();

                case POSTGRESQL:
                case COCKROACHDB:  // CockroachDB is PostgreSQL-compatible
                    return new PostgreSQLPersistenceProvider();

                case ORACLE:
                    return new OraclePersistenceProvider();

                case SQLSERVER:
                    return new SQLServerPersistenceProvider();

                default:
                    throw new UnsupportedOperationException(
                        "Database type " + type + " is not supported yet"
                    );
            }
        });
    }

    /**
     * Get persistence provider based on JDBC URL.
     * Detects database type from the connection URL.
     *
     * @param jdbcUrl JDBC connection URL
     * @return PersistenceProvider instance
     * @throws IllegalArgumentException if URL format is invalid
     * @throws UnsupportedOperationException if database type is not supported
     */
    public static PersistenceProvider getProviderFromUrl(String jdbcUrl) {
        if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:")) {
            throw new IllegalArgumentException("Invalid JDBC URL: " + jdbcUrl);
        }

        String lowerUrl = jdbcUrl.toLowerCase();

        if (lowerUrl.startsWith("jdbc:mysql:")) {
            return getProvider(PersistenceProvider.DatabaseType.MYSQL);
        } else if (lowerUrl.startsWith("jdbc:mariadb:")) {
            return getProvider(PersistenceProvider.DatabaseType.MARIADB);
        } else if (lowerUrl.startsWith("jdbc:postgresql:")) {
            return getProvider(PersistenceProvider.DatabaseType.POSTGRESQL);
        } else if (lowerUrl.startsWith("jdbc:cockroachdb:")) {
            return getProvider(PersistenceProvider.DatabaseType.COCKROACHDB);
        } else if (lowerUrl.startsWith("jdbc:oracle:")) {
            return getProvider(PersistenceProvider.DatabaseType.ORACLE);
        } else if (lowerUrl.startsWith("jdbc:sqlserver:") || lowerUrl.startsWith("jdbc:microsoft:sqlserver:")) {
            return getProvider(PersistenceProvider.DatabaseType.SQLSERVER);
        } else if (lowerUrl.contains("tidb")) {
            return getProvider(PersistenceProvider.DatabaseType.TIDB);
        } else {
            throw new UnsupportedOperationException(
                "Cannot determine database type from URL: " + jdbcUrl
            );
        }
    }

    /**
     * Check if a database type is currently supported with full implementation.
     *
     * @param databaseType Type of database
     * @return true if fully supported, false if only stub implementation exists
     */
    public static boolean isFullySupported(PersistenceProvider.DatabaseType databaseType) {
        switch (databaseType) {
            case MYSQL:
            case MARIADB:
            case TIDB:
                return true;  // Full MySQL implementation available

            case POSTGRESQL:
            case COCKROACHDB:
            case ORACLE:
            case SQLSERVER:
                return false; // Only stub implementations

            default:
                return false;
        }
    }

    /**
     * Get default database type (MySQL)
     */
    public static PersistenceProvider.DatabaseType getDefaultDatabaseType() {
        return PersistenceProvider.DatabaseType.MYSQL;
    }

    /**
     * Clear cached providers (useful for testing)
     */
    public static void clearCache() {
        providers.clear();
    }
}
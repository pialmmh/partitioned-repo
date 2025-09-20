package com.telcobright.core.sql;

import com.telcobright.core.sql.mysql.MySQLSqlGenerator;
import com.telcobright.core.sql.oracle.OracleSqlGenerator;
import com.telcobright.core.sql.postgresql.PostgreSQLSqlGenerator;
import com.telcobright.core.sql.sqlserver.SqlServerSqlGenerator;
import com.telcobright.core.persistence.PersistenceProvider;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Factory for creating database-specific SQL generators
 * Uses singleton pattern to cache generator instances
 */
public class SqlGeneratorFactory {

    private static final Map<DatabaseType, SqlGenerator> generatorCache = new ConcurrentHashMap<>();

    public enum DatabaseType {
        MYSQL("MySQL"),
        POSTGRESQL("PostgreSQL"),
        ORACLE("Oracle"),
        SQLSERVER("Microsoft SQL Server"),
        GENERIC("Generic");

        private final String productName;

        DatabaseType(String productName) {
            this.productName = productName;
        }

        public String getProductName() {
            return productName;
        }
    }

    /**
     * Get SQL generator for specific database type
     */
    public static SqlGenerator getGenerator(DatabaseType type) {
        return generatorCache.computeIfAbsent(type, SqlGeneratorFactory::createGenerator);
    }

    /**
     * Get SQL generator based on PersistenceProvider database type
     */
    public static SqlGenerator getGenerator(PersistenceProvider.DatabaseType dbType) {
        DatabaseType type = mapPersistenceType(dbType);
        return getGenerator(type);
    }

    /**
     * Auto-detect database type from connection and get appropriate generator
     */
    public static SqlGenerator getGenerator(Connection connection) throws SQLException {
        DatabaseType type = detectDatabaseType(connection);
        return getGenerator(type);
    }

    /**
     * Create new generator instance for database type
     */
    private static SqlGenerator createGenerator(DatabaseType type) {
        switch (type) {
            case MYSQL:
                return new MySQLSqlGenerator();
            case POSTGRESQL:
                return new PostgreSQLSqlGenerator();
            case ORACLE:
                return new OracleSqlGenerator();
            case SQLSERVER:
                return new SqlServerSqlGenerator();
            case GENERIC:
            default:
                return new BaseSqlGenerator();
        }
    }

    /**
     * Map PersistenceProvider database type to our database type
     */
    private static DatabaseType mapPersistenceType(PersistenceProvider.DatabaseType dbType) {
        switch (dbType) {
            case MYSQL:
                return DatabaseType.MYSQL;
            case POSTGRESQL:
                return DatabaseType.POSTGRESQL;
            case ORACLE:
                return DatabaseType.ORACLE;
            case SQLSERVER:
                return DatabaseType.SQLSERVER;
            default:
                return DatabaseType.GENERIC;
        }
    }

    /**
     * Detect database type from connection metadata
     */
    private static DatabaseType detectDatabaseType(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        String productName = metaData.getDatabaseProductName().toLowerCase();

        if (productName.contains("mysql") || productName.contains("mariadb")) {
            return DatabaseType.MYSQL;
        } else if (productName.contains("postgresql") || productName.contains("postgres")) {
            return DatabaseType.POSTGRESQL;
        } else if (productName.contains("oracle")) {
            return DatabaseType.ORACLE;
        } else if (productName.contains("microsoft") || productName.contains("sql server")) {
            return DatabaseType.SQLSERVER;
        } else {
            return DatabaseType.GENERIC;
        }
    }

    /**
     * Clear cached generators (useful for testing)
     */
    public static void clearCache() {
        generatorCache.clear();
    }

    /**
     * Get database type name from connection
     */
    public static String getDatabaseTypeName(Connection connection) throws SQLException {
        DatabaseType type = detectDatabaseType(connection);
        return type.getProductName();
    }

    /**
     * Check if database supports native partitioning
     */
    public static boolean supportsPartitioning(DatabaseType type) {
        SqlGenerator generator = getGenerator(type);
        return generator.supportsNativePartitioning();
    }

    /**
     * Check if database supports IF NOT EXISTS clause
     */
    public static boolean supportsIfNotExists(DatabaseType type) {
        SqlGenerator generator = getGenerator(type);
        return generator.supportsIfNotExists();
    }
}
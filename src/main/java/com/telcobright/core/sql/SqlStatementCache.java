package com.telcobright.core.sql;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Cache for pre-generated SQL statements per entity and table.
 * Stores INSERT, UPDATE, DELETE, and batch INSERT statements.
 *
 * @author Split-Verse Framework
 */
public class SqlStatementCache {

    /**
     * SQL statement types that can be cached
     */
    public enum StatementType {
        INSERT_SINGLE,
        INSERT_BATCH_10,
        INSERT_BATCH_100,
        INSERT_BATCH_1000,
        INSERT_BATCH_5000,
        UPDATE_BY_ID,
        DELETE_BY_ID,
        SELECT_BY_ID,
        SELECT_BY_DATE_RANGE,
        SELECT_WITH_PAGINATION
    }

    // Cache structure: EntityClass -> TableName -> StatementType -> SQL
    private final Map<Class<?>, Map<String, Map<StatementType, String>>> cache = new ConcurrentHashMap<>();

    /**
     * Store a SQL statement in the cache
     */
    public void put(Class<?> entityClass, String tableName, StatementType type, String sql) {
        cache.computeIfAbsent(entityClass, k -> new ConcurrentHashMap<>())
             .computeIfAbsent(tableName, k -> new ConcurrentHashMap<>())
             .put(type, sql);
    }

    /**
     * Retrieve a SQL statement from the cache
     */
    public String get(Class<?> entityClass, String tableName, StatementType type) {
        Map<String, Map<StatementType, String>> entityCache = cache.get(entityClass);
        if (entityCache == null) return null;

        Map<StatementType, String> tableCache = entityCache.get(tableName);
        if (tableCache == null) return null;

        return tableCache.get(type);
    }

    /**
     * Get or compute a SQL statement
     */
    public String getOrCompute(Class<?> entityClass, String tableName, StatementType type,
                              SqlGenerator generator) {
        return cache.computeIfAbsent(entityClass, k -> new ConcurrentHashMap<>())
                   .computeIfAbsent(tableName, k -> new ConcurrentHashMap<>())
                   .computeIfAbsent(type, k -> generator.generate());
    }

    /**
     * Check if a statement is cached
     */
    public boolean contains(Class<?> entityClass, String tableName, StatementType type) {
        return get(entityClass, tableName, type) != null;
    }

    /**
     * Clear all cached statements for an entity
     */
    public void clearEntity(Class<?> entityClass) {
        cache.remove(entityClass);
    }

    /**
     * Clear all cached statements
     */
    public void clearAll() {
        cache.clear();
    }

    /**
     * Get cache statistics
     */
    public CacheStatistics getStatistics() {
        int totalEntities = cache.size();
        int totalStatements = cache.values().stream()
            .mapToInt(tables -> tables.values().stream()
                .mapToInt(Map::size)
                .sum())
            .sum();

        return new CacheStatistics(totalEntities, totalStatements);
    }

    /**
     * Functional interface for lazy SQL generation
     */
    @FunctionalInterface
    public interface SqlGenerator {
        String generate();
    }

    /**
     * Cache statistics
     */
    public static class CacheStatistics {
        public final int totalEntities;
        public final int totalStatements;

        public CacheStatistics(int totalEntities, int totalStatements) {
            this.totalEntities = totalEntities;
            this.totalStatements = totalStatements;
        }

        @Override
        public String toString() {
            return String.format("SqlCache[entities=%d, statements=%d]",
                               totalEntities, totalStatements);
        }
    }
}
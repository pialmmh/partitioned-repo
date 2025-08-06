package com.telcobright.db.entity;

import java.time.LocalDateTime;

/**
 * Base interface for all entities used with ShardingRepository.
 * 
 * This interface enforces that entities have the required structure for sharding,
 * but allows flexible field naming through annotations:
 * 
 * - Primary key field: Must be annotated with @Id (field name can be anything)
 * - Partitioning field: Must be annotated with @ShardingKey (field name can be anything)
 * 
 * Examples of valid entities:
 * - Field named 'id' with @Id annotation
 * - Field named 'userId' with @Id annotation  
 * - Field named 'uuid' with @Id annotation
 * - Field named 'createdAt' with @ShardingKey annotation
 * - Field named 'timestamp' with @ShardingKey annotation
 * - Field named 'eventTime' with @ShardingKey annotation
 * 
 * The actual column names are determined by @Column annotations.
 * 
 * @param <K> The primary key type (Long, String, UUID, etc.)
 */
public interface ShardingEntity<K> {
    // This interface serves as a marker interface for type safety.
    // The actual ID and sharding key fields are detected via reflection
    // using @Id and @ShardingKey annotations, allowing flexible naming.
}
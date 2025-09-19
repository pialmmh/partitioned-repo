package com.telcobright.splitverse.repository.jpa;

import com.telcobright.util.db.repository.MySqlOptimizedRepository;
import com.telcobright.splitverse.entity.jpa.TestUser;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Repository using db-util's MySqlOptimizedRepository for MySQL batch optimization.
 * Inherits all JPA functionality plus insertExtendedToMysql() method.
 */
@Repository
public interface TestUserRepository extends MySqlOptimizedRepository<TestUser, String> {

    // Standard Spring Data JPA derived query methods
    Optional<TestUser> findByUsername(String username);

    List<TestUser> findByEmail(String email);

    List<TestUser> findByActiveTrue();

    List<TestUser> findByCreatedAtBetween(LocalDateTime start, LocalDateTime end);

    // Custom JPQL queries
    @Query("SELECT u FROM TestUser u WHERE u.createdAt >= :date AND u.active = true")
    List<TestUser> findActiveUsersCreatedAfter(@Param("date") LocalDateTime date);

    @Query("SELECT u FROM TestUser u WHERE u.firstName LIKE %:keyword% OR u.lastName LIKE %:keyword%")
    List<TestUser> searchByName(@Param("keyword") String keyword);

    // Native SQL query for partition-specific operations
    @Query(value = "SELECT * FROM test_users PARTITION (p202401) WHERE user_id = ?1",
           nativeQuery = true)
    Optional<TestUser> findInJanuaryPartition(String userId);

    // Count queries
    long countByActiveTrue();

    @Query("SELECT COUNT(u) FROM TestUser u WHERE u.createdAt >= :date")
    long countUsersCreatedAfter(@Param("date") LocalDateTime date);
}
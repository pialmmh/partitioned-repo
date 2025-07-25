package com.telcobright.db.example;

import com.telcobright.db.repository.MultiTableRepository;
import com.telcobright.db.sharding.ShardingConfig;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

public class SmsMultiTableRepository extends MultiTableRepository<SmsEntity> {
    
    public SmsMultiTableRepository(ShardingConfig config) {
        super(config);
    }
    
    @Override
    protected String getInsertSql() {
        return "INSERT INTO " + config.getEntityName() + 
               " (phone_number, message, status, created_at, user_id) VALUES (?, ?, ?, ?, ?)";
    }
    
    @Override
    protected void setInsertParameters(PreparedStatement stmt, SmsEntity entity) throws SQLException {
        stmt.setString(1, entity.getPhoneNumber());
        stmt.setString(2, entity.getMessage());
        stmt.setString(3, entity.getStatus());
        stmt.setObject(4, entity.getCreatedAt());
        stmt.setString(5, entity.getUserId());
    }
    
    @Override
    protected SmsEntity mapResultSetToEntity(ResultSet rs) throws SQLException {
        SmsEntity entity = new SmsEntity();
        entity.setId(rs.getLong("id"));
        entity.setPhoneNumber(rs.getString("phone_number"));
        entity.setMessage(rs.getString("message"));
        entity.setStatus(rs.getString("status"));
        entity.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
        entity.setUserId(rs.getString("user_id"));
        return entity;
    }
    
    @Override
    protected String getTableSchema() {
        return "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
               "phone_number VARCHAR(20) NOT NULL, " +
               "message TEXT, " +
               "status VARCHAR(20) NOT NULL, " +
               "created_at DATETIME NOT NULL, " +
               "user_id VARCHAR(50), " +
               "INDEX idx_created_at (created_at), " +
               "INDEX idx_phone_number (phone_number), " +
               "INDEX idx_user_id (user_id)";
    }
    
    @Override
    protected String getTableNameForEntity(SmsEntity entity) {
        return getTableNameForDate(entity.getCreatedAt());
    }
    
    @Override
    protected LocalDateTime getDateFromEntity(SmsEntity entity) {
        return entity.getCreatedAt();
    }
}
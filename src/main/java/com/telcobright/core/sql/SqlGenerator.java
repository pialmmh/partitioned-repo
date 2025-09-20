package com.telcobright.core.sql;

import com.telcobright.core.metadata.EntityMetadata;
import com.telcobright.core.metadata.FieldMetadata;
import java.util.List;
import java.util.Map;

/**
 * SQL Generator Interface
 * Defines methods for generating SQL statements for different database operations
 */
public interface SqlGenerator {

    // DDL Operations
    String generateCreateTable(String tableName, EntityMetadata metadata);
    String generateCreateTableWithPartitions(String tableName, EntityMetadata metadata,
                                           String partitionColumn, String partitionType);
    String generateAlterTableAddPartition(String tableName, String partitionName,
                                        String partitionDefinition);
    String generateDropTable(String tableName);
    String generateCreateIndex(String tableName, String indexName, List<String> columns);
    String generateDropIndex(String tableName, String indexName);

    // DML Operations - Basic CRUD
    String generateInsert(String tableName, Map<String, Object> values);
    String generateBatchInsert(String tableName, List<Map<String, Object>> records);
    String generateSelectById(String tableName, String idColumn);
    String generateSelectAll(String tableName);
    String generateSelectWithWhere(String tableName, Map<String, Object> conditions);
    String generateSelectWithDateRange(String tableName, String dateColumn,
                                      String startDate, String endDate);
    String generateSelectByIdAndDateRange(String tableName, String idColumn,
                                        String dateColumn, String startDate, String endDate);
    String generateUpdate(String tableName, Map<String, Object> values,
                         Map<String, Object> conditions);
    String generateUpdateById(String tableName, String idColumn, Map<String, Object> values);
    String generateDelete(String tableName, Map<String, Object> conditions);
    String generateDeleteById(String tableName, String idColumn);
    String generateDeleteByDateRange(String tableName, String dateColumn,
                                   String startDate, String endDate);

    // Pagination and Sorting
    String generateSelectWithPagination(String baseQuery, int limit, int offset);
    String generateSelectWithSorting(String baseQuery, List<SortField> sortFields);
    String generateCountQuery(String tableName, Map<String, Object> conditions);

    // Advanced Queries
    String generateSelectBatchByIdGreaterThan(String tableName, String idColumn,
                                             String lastId, int batchSize);
    String generateSelectBeforeDate(String tableName, String dateColumn, String date);
    String generateSelectAfterDate(String tableName, String dateColumn, String date);
    String generateExistsQuery(String tableName, String idColumn);

    // Utility Methods
    String escapeIdentifier(String identifier);
    String escapeLiteral(Object value);
    String getDateFormat();
    String getCurrentTimestamp();
    boolean supportsIfNotExists();
    boolean supportsNativePartitioning();

    // Helper class for sorting
    class SortField {
        private final String column;
        private final SortDirection direction;

        public SortField(String column, SortDirection direction) {
            this.column = column;
            this.direction = direction;
        }

        public String getColumn() { return column; }
        public SortDirection getDirection() { return direction; }
    }

    enum SortDirection {
        ASC, DESC
    }
}
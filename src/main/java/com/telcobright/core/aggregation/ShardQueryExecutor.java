package com.telcobright.core.aggregation;

import com.telcobright.core.logging.Logger;
import com.telcobright.splitverse.config.RepositoryMode;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Executes aggregation queries on a single shard.
 * Handles both MULTI_TABLE and NATIVE_PARTITION modes.
 */
public class ShardQueryExecutor {
    private final Connection connection;
    private final RepositoryMode repositoryMode;
    private final String baseTableName;
    private final Logger logger;

    public ShardQueryExecutor(Connection connection, RepositoryMode repositoryMode, String baseTableName, Logger logger) {
        this.connection = connection;
        this.repositoryMode = repositoryMode;
        this.baseTableName = baseTableName;
        this.logger = logger;
    }

    /**
     * Execute aggregation query on this shard.
     */
    public List<PartialResult> execute(
        AggregationQuery query,
        List<String> tableNames,
        LocalDateTime startTime,
        LocalDateTime endTime,
        Object... params
    ) throws SQLException {

        String sql = buildAggregationSQL(query, tableNames, startTime, endTime);
        logger.debug("Executing aggregation on shard: " + sql);

        List<PartialResult> results = new ArrayList<>();

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int paramIndex = 1;

            // Set timestamp parameters (if applicable)
            if (repositoryMode == RepositoryMode.PARTITIONED) {
                ps.setTimestamp(paramIndex++, Timestamp.valueOf(startTime));
                ps.setTimestamp(paramIndex++, Timestamp.valueOf(endTime));
            }

            // Set WHERE clause parameters
            if (query.getWhereClause() != null && params != null) {
                for (Object param : params) {
                    ps.setObject(paramIndex++, param);
                }
            }

            try (ResultSet rs = ps.executeQuery()) {
                results = parseResultSet(rs, query);
            }
        }

        logger.debug("Shard returned " + results.size() + " partial results");
        return results;
    }

    private String buildAggregationSQL(
        AggregationQuery query,
        List<String> tableNames,
        LocalDateTime startTime,
        LocalDateTime endTime
    ) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");

        // Add GROUP BY columns
        for (int i = 0; i < query.getGroupByColumns().size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(query.getGroupByColumns().get(i));
        }

        // Add aggregate columns
        for (AggregateColumn aggCol : query.getAggregateColumns()) {
            if (!query.getGroupByColumns().isEmpty() || query.getAggregateColumns().indexOf(aggCol) > 0) {
                sql.append(", ");
            }

            switch (aggCol.getFunction()) {
                case SUM:
                    sql.append("SUM(").append(aggCol.getColumnExpression()).append(") AS ").append(aggCol.getAlias());
                    break;
                case COUNT:
                    sql.append("COUNT(").append(aggCol.getColumnExpression()).append(") AS ").append(aggCol.getAlias());
                    break;
                case MAX:
                    sql.append("MAX(").append(aggCol.getColumnExpression()).append(") AS ").append(aggCol.getAlias());
                    break;
                case MIN:
                    sql.append("MIN(").append(aggCol.getColumnExpression()).append(") AS ").append(aggCol.getAlias());
                    break;
                case AVG:
                    // Decompose AVG to SUM and COUNT for proper cross-shard merging
                    sql.append("SUM(").append(aggCol.getColumnExpression()).append(") AS __").append(aggCol.getAlias()).append("_sum");
                    sql.append(", COUNT(").append(aggCol.getColumnExpression()).append(") AS __").append(aggCol.getAlias()).append("_count");
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported aggregate function: " + aggCol.getFunction());
            }
        }

        // FROM clause
        if (repositoryMode == RepositoryMode.MULTI_TABLE) {
            // Build UNION ALL across multiple tables
            sql.append(" FROM (");
            for (int i = 0; i < tableNames.size(); i++) {
                if (i > 0) sql.append(" UNION ALL ");
                sql.append("SELECT * FROM ").append(tableNames.get(i));
            }
            sql.append(") AS t");
        } else {
            // Single partitioned table
            sql.append(" FROM ").append(baseTableName);
        }

        // WHERE clause
        List<String> whereClauses = new ArrayList<>();

        if (repositoryMode == RepositoryMode.PARTITIONED) {
            // Add timestamp range for partition pruning
            whereClauses.add("timestamp BETWEEN ? AND ?");
        }

        if (query.getWhereClause() != null && !query.getWhereClause().trim().isEmpty()) {
            whereClauses.add("(" + query.getWhereClause() + ")");
        }

        if (!whereClauses.isEmpty()) {
            sql.append(" WHERE ");
            sql.append(String.join(" AND ", whereClauses));
        }

        // GROUP BY clause
        if (!query.getGroupByColumns().isEmpty()) {
            sql.append(" GROUP BY ");
            sql.append(String.join(", ", query.getGroupByColumns()));
        }

        // NOTE: HAVING and ORDER BY are NOT applied at shard level
        // They will be applied after merging results from all shards

        return sql.toString();
    }

    private List<PartialResult> parseResultSet(ResultSet rs, AggregationQuery query) throws SQLException {
        List<PartialResult> results = new ArrayList<>();
        ResultSetMetaData metadata = rs.getMetaData();

        while (rs.next()) {
            PartialResult result = new PartialResult();

            // Extract GROUP BY values
            for (String groupCol : query.getGroupByColumns()) {
                Object value = rs.getObject(groupCol);
                result.addGroupByValue(groupCol, value);
            }

            // Extract aggregate values
            for (AggregateColumn aggCol : query.getAggregateColumns()) {
                if (aggCol.getFunction() == AggregateFunction.AVG) {
                    // For AVG, extract both sum and count
                    BigDecimal sum = rs.getBigDecimal("__" + aggCol.getAlias() + "_sum");
                    Long count = rs.getLong("__" + aggCol.getAlias() + "_count");
                    result.addAggregateValue("__" + aggCol.getAlias() + "_sum", sum);
                    result.addAggregateValue("__" + aggCol.getAlias() + "_count", count);
                } else {
                    Object value = rs.getObject(aggCol.getAlias());
                    result.addAggregateValue(aggCol.getAlias(), value);
                }
            }

            results.add(result);
        }

        return results;
    }
}

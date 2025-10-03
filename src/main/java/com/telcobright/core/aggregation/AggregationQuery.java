package com.telcobright.core.aggregation;

import com.telcobright.core.query.SortExpression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Defines an aggregation query that can be executed across multiple shards and tables.
 */
public class AggregationQuery {
    private final List<String> selectColumns;
    private final List<AggregateColumn> aggregateColumns;
    private final List<String> groupByColumns;
    private final String whereClause;
    private final String havingClause;
    private final SortExpression sortExpression;
    private final Integer limit;

    private AggregationQuery(Builder builder) {
        this.selectColumns = builder.selectColumns;
        this.aggregateColumns = builder.aggregateColumns;
        this.groupByColumns = builder.groupByColumns;
        this.whereClause = builder.whereClause;
        this.havingClause = builder.havingClause;
        this.sortExpression = builder.sortExpression;
        this.limit = builder.limit;
    }

    public List<String> getSelectColumns() {
        return selectColumns;
    }

    public List<AggregateColumn> getAggregateColumns() {
        return aggregateColumns;
    }

    public List<String> getGroupByColumns() {
        return groupByColumns;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public String getHavingClause() {
        return havingClause;
    }

    public SortExpression getSortExpression() {
        return sortExpression;
    }

    public Integer getLimit() {
        return limit;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<String> selectColumns = new ArrayList<>();
        private List<AggregateColumn> aggregateColumns = new ArrayList<>();
        private List<String> groupByColumns = new ArrayList<>();
        private String whereClause;
        private String havingClause;
        private SortExpression sortExpression;
        private Integer limit;

        public Builder select(String... columns) {
            this.selectColumns.addAll(Arrays.asList(columns));
            return this;
        }

        public Builder sum(String columnExpression, String alias) {
            this.aggregateColumns.add(new AggregateColumn(AggregateFunction.SUM, columnExpression, alias));
            return this;
        }

        public Builder avg(String columnExpression, String alias) {
            this.aggregateColumns.add(new AggregateColumn(AggregateFunction.AVG, columnExpression, alias));
            return this;
        }

        public Builder count(String columnExpression, String alias) {
            this.aggregateColumns.add(new AggregateColumn(AggregateFunction.COUNT, columnExpression, alias));
            return this;
        }

        public Builder max(String columnExpression, String alias) {
            this.aggregateColumns.add(new AggregateColumn(AggregateFunction.MAX, columnExpression, alias));
            return this;
        }

        public Builder min(String columnExpression, String alias) {
            this.aggregateColumns.add(new AggregateColumn(AggregateFunction.MIN, columnExpression, alias));
            return this;
        }

        public Builder groupBy(String... columns) {
            this.groupByColumns.addAll(Arrays.asList(columns));
            return this;
        }

        public Builder where(String clause) {
            this.whereClause = clause;
            return this;
        }

        public Builder having(String clause) {
            this.havingClause = clause;
            return this;
        }

        public Builder orderBy(SortExpression sortExpression) {
            this.sortExpression = sortExpression;
            return this;
        }

        public Builder limit(int limit) {
            this.limit = limit;
            return this;
        }

        public AggregationQuery build() {
            // Validation
            if (aggregateColumns.isEmpty()) {
                throw new IllegalArgumentException("At least one aggregate function is required");
            }

            if (!groupByColumns.isEmpty() && selectColumns.isEmpty()) {
                // Auto-add groupBy columns to select
                selectColumns.addAll(groupByColumns);
            }

            return new AggregationQuery(this);
        }
    }
}

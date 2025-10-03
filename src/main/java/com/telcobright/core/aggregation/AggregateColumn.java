package com.telcobright.core.aggregation;

/**
 * Represents an aggregate column specification.
 */
public class AggregateColumn {
    private final AggregateFunction function;
    private final String columnExpression;
    private final String alias;

    public AggregateColumn(AggregateFunction function, String columnExpression, String alias) {
        this.function = function;
        this.columnExpression = columnExpression;
        this.alias = alias;
    }

    public AggregateFunction getFunction() {
        return function;
    }

    public String getColumnExpression() {
        return columnExpression;
    }

    public String getAlias() {
        return alias;
    }

    @Override
    public String toString() {
        return function + "(" + columnExpression + ") AS " + alias;
    }
}

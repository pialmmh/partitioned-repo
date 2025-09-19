package com.telcobright.core.query;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents sort expressions for queries
 */
public class SortExpression {

    public enum SortOrder {
        ASC("ASC"),
        DESC("DESC");

        private final String sql;

        SortOrder(String sql) {
            this.sql = sql;
        }

        public String toSql() {
            return sql;
        }
    }

    public static class SortField {
        private final String fieldName;
        private final SortOrder order;

        public SortField(String fieldName, SortOrder order) {
            this.fieldName = fieldName;
            this.order = order;
        }

        public String getFieldName() {
            return fieldName;
        }

        public SortOrder getOrder() {
            return order;
        }

        public String toSql() {
            return fieldName + " " + order.toSql();
        }
    }

    private final List<SortField> sortFields;

    private SortExpression(Builder builder) {
        this.sortFields = new ArrayList<>(builder.sortFields);
    }

    public List<SortField> getSortFields() {
        return new ArrayList<>(sortFields);
    }

    public String toSql() {
        if (sortFields.isEmpty()) {
            return "";
        }

        StringBuilder sql = new StringBuilder(" ORDER BY ");
        boolean first = true;
        for (SortField field : sortFields) {
            if (!first) {
                sql.append(", ");
            }
            sql.append(field.toSql());
            first = false;
        }
        return sql.toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<SortField> sortFields = new ArrayList<>();

        public Builder addSort(String fieldName, SortOrder order) {
            sortFields.add(new SortField(fieldName, order));
            return this;
        }

        public Builder addAscending(String fieldName) {
            return addSort(fieldName, SortOrder.ASC);
        }

        public Builder addDescending(String fieldName) {
            return addSort(fieldName, SortOrder.DESC);
        }

        public SortExpression build() {
            return new SortExpression(this);
        }
    }
}
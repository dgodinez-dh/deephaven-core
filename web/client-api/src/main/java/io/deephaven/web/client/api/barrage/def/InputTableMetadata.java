//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.def;

import elemental2.core.JsArray;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.base.Any;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents input table metadata parsed from the schema.
 */
public class InputTableMetadata {
    private final Map<String, ColumnRestrictions> columnRestrictions;

    @JsIgnore
    public InputTableMetadata() {
        this.columnRestrictions = new HashMap<>();
    }

    @JsIgnore
    public void addColumnRestrictions(String columnName, ColumnRestrictions restrictions) {
        columnRestrictions.put(columnName, restrictions);
    }

    @JsIgnore
    @JsNullable
    public ColumnRestrictions getColumnRestrictions(String columnName) {
        return columnRestrictions.get(columnName);
    }

    /**
     * Represents restrictions on a column's values.
     */
    public static class ColumnRestrictions {
        private final JsArray<Any> restrictions;

        @JsIgnore
        public ColumnRestrictions() {
            this.restrictions = new JsArray<>();
        }

        @JsIgnore
        public void addRestriction(Any restriction) {
            restrictions.push(restriction);
        }

        @JsIgnore
        public JsArray<Any> getRestrictions() {
            return restrictions;
        }
    }
}


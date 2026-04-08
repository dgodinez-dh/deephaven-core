//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.def;

import elemental2.core.JsArray;
import io.deephaven.web.client.api.ColumnRestriction;
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
        private final JsArray<ColumnRestriction> restrictions;

        @JsIgnore
        public ColumnRestrictions() {
            this.restrictions = new JsArray<>();
        }

        @JsIgnore
        public void addRestriction(Any restrictionData) {
            // Convert the parsed restriction data into a ColumnRestriction object
            ColumnRestriction restriction = convertRestriction(restrictionData);
            if (restriction != null) {
                restrictions.push(restriction);
            }
        }

        @JsIgnore
        public JsArray<ColumnRestriction> getRestrictions() {
            return restrictions;
        }

        private static native ColumnRestriction convertRestriction(Any restrictionData) /*-{
            if (!restrictionData) return null;

            var type = restrictionData.type || "Unknown";
            console.log("convertRestriction: Converting restriction of type:", type);

            // Create the appropriate ColumnRestriction based on type
            if (type === 'IntegerRangeRestriction' || type === 'DoubleRangeRestriction') {
                var minValue = restrictionData.minInclusive !== undefined ? restrictionData.minInclusive : NaN;
                var maxValue = restrictionData.maxInclusive !== undefined ? restrictionData.maxInclusive : NaN;
                return @io.deephaven.web.client.api.ColumnRestriction::new(Ljava/lang/String;DD)(
                    type, minValue, maxValue
                );
            } else if (type === 'StringListRestriction') {
                var allowedValues = restrictionData.allowedValues || [];
                // Use the JS array directly - it will be cast to JsArray<Any> automatically
                return @io.deephaven.web.client.api.ColumnRestriction::new(Ljava/lang/String;Lelemental2/core/JsArray;)(
                    type, allowedValues
                );
            } else if (type === 'NotNullRestriction' || type === 'NonEmptyRestriction') {
                return @io.deephaven.web.client.api.ColumnRestriction::new(Ljava/lang/String;)(type);
            } else {
                console.warn("convertRestriction: Unknown restriction type:", type);
                return null;
            }
        }-*/;
    }
}


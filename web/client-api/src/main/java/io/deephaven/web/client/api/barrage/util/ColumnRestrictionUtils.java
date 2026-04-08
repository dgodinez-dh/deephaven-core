//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.util;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.web.client.api.ColumnRestriction;
import io.deephaven.web.client.fu.JsLog;
import jsinterop.base.Any;

/**
 * Utility class for parsing column restrictions from protobuf data.
 */
public class ColumnRestrictionUtils {

    private ColumnRestrictionUtils() {
        // Utility class - no instances
    }

    /**
     * Manually parse protobuf bytes to extract input table metadata and column restrictions.
     * This works around issues with google.protobuf.Any deserialization in JavaScript.
     *
     * @param bytes The protobuf bytes to parse
     * @return A map of column names to their metadata, or null if parsing fails
     */
    public static native Any parseProtoManually(Uint8Array bytes) /*-{
        // Manual protobuf parsing to work around missing google.protobuf.Any
        // Based on the pattern from AddToInputTable.java
        // Use the BinaryReader from dhinternal.jspb which is the JsInterop wrapper
        try {
            // Use the JsInterop BinaryReader class
            var BinaryReader = @io.deephaven.javascript.proto.dhinternal.jspb.BinaryReader::new(Lelemental2/core/Uint8Array;);
            var reader = new BinaryReader(bytes);
            var result = {};

            // Parse the DeephavenTableMetadata message
            // Field 1 is InputTableMetadata
            while (reader.nextField()) {
                if (reader.isEndGroup()) {
                    break;
                }
                var field = reader.getFieldNumber();

                if (field === 1) {
                    // This is the InputTableMetadata field
                    var inputTableMetadata = {};
                    reader.readMessage(inputTableMetadata, function(metadata, rdr) {
                        // Parse InputTableMetadata
                        // Field 1 is columnInfoMap (map<string, InputTableColumnInfo>)
                        while (rdr.nextField()) {
                            if (rdr.isEndGroup()) {
                                break;
                            }
                            var subfield = rdr.getFieldNumber();

                            if (subfield === 1) {
                                // This is the columnInfoMap
                                var mapEntry = {};
                                rdr.readMessage(mapEntry, function(entry, mapReader) {
                                    var key = null;
                                    var value = null;

                                    while (mapReader.nextField()) {
                                        if (mapReader.isEndGroup()) {
                                            break;
                                        }
                                        var mapField = mapReader.getFieldNumber();

                                        if (mapField === 1) {
                                            // Map key (column name)
                                            key = mapReader.readString();
                                        } else if (mapField === 2) {
                                            // Map value (InputTableColumnInfo)
                                            value = {};
                                            var restrictions = [];

                                            mapReader.readMessage(value, function(columnInfo, colReader) {
                                                while (colReader.nextField()) {
                                                    if (colReader.isEndGroup()) {
                                                        break;
                                                    }
                                                    var colField = colReader.getFieldNumber();

                                                    if (colField === 1) {
                                                        // kind field
                                                        columnInfo.kind = colReader.readEnum();
                                                    } else if (colField === 2) {
                                                        // restrictions field (repeated google.protobuf.Any)
                                                        // Parse the Any message to extract type_url and value
                                                        try {
                                                            var anyBytes = colReader.readBytes();

                                                            // Parse the google.protobuf.Any message
                                                            // Field 1 = type_url (string)
                                                            // Field 2 = value (bytes)
                                                            var anyReader = new BinaryReader(anyBytes);
                                                            var typeUrl = null;
                                                            var valueBytes = null;

                                                            while (anyReader.nextField()) {
                                                                if (anyReader.isEndGroup()) {
                                                                    break;
                                                                }
                                                                var anyField = anyReader.getFieldNumber();
                                                                if (anyField === 1) {
                                                                    typeUrl = anyReader.readString();
                                                                } else if (anyField === 2) {
                                                                    valueBytes = anyReader.readBytes();
                                                                }
                                                            }

                                                            // Now parse the actual restriction based on type_url
                                                            var restriction = @io.deephaven.web.client.api.barrage.util.ColumnRestrictionUtils::parseRestriction(*)(typeUrl, valueBytes);
                                                            if (restriction) {
                                                                restrictions.push(restriction);
                                                            }
                                                        } catch (e) {
                                                            @io.deephaven.web.client.fu.JsLog::warn(*)("Failed to parse restriction:", e);
                                                        }
                                                    }
                                                }
                                                columnInfo.restrictions = restrictions;
                                            });
                                        }
                                    }

                                    if (key !== null && value !== null) {
                                        if (!metadata.columnInfoMap) {
                                            metadata.columnInfoMap = {};
                                        }
                                        metadata.columnInfoMap[key] = value;
                                    }
                                });
                            }
                        }
                    });

                    result = inputTableMetadata.columnInfoMap || {};
                }
            }

            return result;

        } catch (e) {
            @io.deephaven.web.client.fu.JsLog::warn(*)("Failed to manually parse protobuf:", e);
            return null;
        }
    }-*/;

    /**
     * Parse a specific restriction type from protobuf bytes.
     *
     * @param typeUrl The type URL of the restriction
     * @param valueBytes The protobuf bytes containing the restriction data
     * @return The parsed restriction data, or null if parsing fails
     */
    private static native Any parseRestriction(String typeUrl, Uint8Array valueBytes) /*-{
        // Parse specific restriction types based on typeUrl
        // Types from inputtable.proto:
        // - IntegerRangeRestriction
        // - DoubleRangeRestriction
        // - NotNullRestriction
        // - NonEmptyRestriction
        // - StringListRestriction

        try {
            var BinaryReader = @io.deephaven.javascript.proto.dhinternal.jspb.BinaryReader::new(Lelemental2/core/Uint8Array;);
            var reader = new BinaryReader(valueBytes);

            // Extract the message type from the type URL
            // Format: "type.googleapis.com/io.deephaven.proto.backplane.grpc.IntegerRangeRestriction"
            // or "docs.deephaven.io/io.deephaven.proto.backplane.grpc.IntegerRangeRestriction"
            var typeName = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
            var shortName = typeName.substring(typeName.lastIndexOf('.') + 1);

            var restriction = {
                type: shortName,
                typeUrl: typeUrl
            };

            if (shortName === 'IntegerRangeRestriction') {
                // Field 1 = min_inclusive (int64)
                // Field 2 = max_inclusive (int64)
                while (reader.nextField()) {
                    if (reader.isEndGroup()) break;
                    var field = reader.getFieldNumber();
                    if (field === 1) {
                        restriction.minInclusive = reader.readInt64();
                    } else if (field === 2) {
                        restriction.maxInclusive = reader.readInt64();
                    }
                }
            } else if (shortName === 'DoubleRangeRestriction') {
                // Field 1 = min_inclusive (double)
                // Field 2 = max_inclusive (double)
                while (reader.nextField()) {
                    if (reader.isEndGroup()) break;
                    var field = reader.getFieldNumber();
                    if (field === 1) {
                        restriction.minInclusive = reader.readDouble();
                    } else if (field === 2) {
                        restriction.maxInclusive = reader.readDouble();
                    }
                }
            } else if (shortName === 'NotNullRestriction') {
                // No fields - just the type
                restriction.notNull = true;
            } else if (shortName === 'NonEmptyRestriction') {
                // No fields - just the type
                restriction.nonEmpty = true;
            } else if (shortName === 'StringListRestriction') {
                // Field 1 = allowed_values (repeated string)
                restriction.allowedValues = [];
                while (reader.nextField()) {
                    if (reader.isEndGroup()) break;
                    var field = reader.getFieldNumber();
                    if (field === 1) {
                        var value = reader.readString();
                        restriction.allowedValues.push(value);
                    }
                }
            } else {
                restriction.raw = valueBytes;
            }

            return restriction;

        } catch (e) {
            @io.deephaven.web.client.fu.JsLog::warn(*)("Failed to parse restriction:", e);
            return null;
        }
    }-*/;

    /**
     * Convert IntegerRangeRestriction data into a ColumnRestriction object.
     *
     * @param restrictionData The parsed restriction data from protobuf
     * @return A ColumnRestriction object, or null if conversion fails
     */
    public static native ColumnRestriction convertIntegerRangeRestriction(Any restrictionData) /*-{
        if (!restrictionData) return null;
        var minValue = restrictionData.minInclusive !== undefined ? restrictionData.minInclusive : NaN;
        var maxValue = restrictionData.maxInclusive !== undefined ? restrictionData.maxInclusive : NaN;
        return @io.deephaven.web.client.api.ColumnRestriction::new(Ljava/lang/String;DD)(
            "IntegerRangeRestriction", minValue, maxValue
        );
    }-*/;

    /**
     * Convert DoubleRangeRestriction data into a ColumnRestriction object.
     *
     * @param restrictionData The parsed restriction data from protobuf
     * @return A ColumnRestriction object, or null if conversion fails
     */
    public static native ColumnRestriction convertDoubleRangeRestriction(Any restrictionData) /*-{
        if (!restrictionData) return null;
        var minValue = restrictionData.minInclusive !== undefined ? restrictionData.minInclusive : NaN;
        var maxValue = restrictionData.maxInclusive !== undefined ? restrictionData.maxInclusive : NaN;
        return @io.deephaven.web.client.api.ColumnRestriction::new(Ljava/lang/String;DD)(
            "DoubleRangeRestriction", minValue, maxValue
        );
    }-*/;

    /**
     * Convert NotNullRestriction data into a ColumnRestriction object.
     *
     * @param restrictionData The parsed restriction data from protobuf
     * @return A ColumnRestriction object, or null if conversion fails
     */
    public static native ColumnRestriction convertNotNullRestriction(Any restrictionData) /*-{
        if (!restrictionData) return null;
        return @io.deephaven.web.client.api.ColumnRestriction::new(Ljava/lang/String;)("NotNullRestriction");
    }-*/;

    /**
     * Convert NonEmptyRestriction data into a ColumnRestriction object.
     *
     * @param restrictionData The parsed restriction data from protobuf
     * @return A ColumnRestriction object, or null if conversion fails
     */
    public static native ColumnRestriction convertNonEmptyRestriction(Any restrictionData) /*-{
        if (!restrictionData) return null;
        return @io.deephaven.web.client.api.ColumnRestriction::new(Ljava/lang/String;)("NonEmptyRestriction");
    }-*/;

    /**
     * Convert StringListRestriction data into a ColumnRestriction object.
     *
     * @param restrictionData The parsed restriction data from protobuf
     * @return A ColumnRestriction object, or null if conversion fails
     */
    public static native ColumnRestriction convertStringListRestriction(Any restrictionData) /*-{
        if (!restrictionData) return null;
        var allowedValues = restrictionData.allowedValues || [];
        // Use the JS array directly - it will be cast to JsArray<Any> automatically
        return @io.deephaven.web.client.api.ColumnRestriction::new(Ljava/lang/String;Lelemental2/core/JsArray;)(
            "StringListRestriction", allowedValues
        );
    }-*/;
}


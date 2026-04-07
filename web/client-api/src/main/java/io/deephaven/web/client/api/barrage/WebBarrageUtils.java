//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import com.google.flatbuffers.FlatBufferBuilder;
import elemental2.core.*;
import elemental2.dom.DomGlobal;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.web.client.api.barrage.def.ColumnDefinition;
import io.deephaven.web.client.api.barrage.def.InitialTableDefinition;
import io.deephaven.web.client.api.barrage.def.InputTableMetadata;
import io.deephaven.web.client.api.barrage.def.TableAttributesDefinition;
import io.deephaven.web.shared.data.*;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.Schema;
import org.gwtproject.nio.TypedArrayHelper;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;

/**
 * Utility to read barrage record batches.
 */
public class WebBarrageUtils {
    public static final int FLATBUFFER_MAGIC = 0x6E687064;

    public static Uint8Array wrapMessage(FlatBufferBuilder innerBuilder, byte messageType) {
        FlatBufferBuilder outerBuilder = new FlatBufferBuilder(1024);
        int messageOffset = BarrageMessageWrapper.createMsgPayloadVector(outerBuilder, innerBuilder.dataBuffer());
        int offset =
                BarrageMessageWrapper.createBarrageMessageWrapper(outerBuilder, FLATBUFFER_MAGIC, messageType,
                        messageOffset);
        outerBuilder.finish(offset);
        ByteBuffer byteBuffer = outerBuilder.dataBuffer();
        return bbToUint8ArrayView(byteBuffer);
    }

    public static Uint8Array bbToUint8ArrayView(ByteBuffer byteBuffer) {
        ArrayBufferView view = TypedArrayHelper.unwrap(byteBuffer);
        return new Uint8Array(view.buffer, byteBuffer.position() + view.byteOffset, byteBuffer.remaining());
    }

    public static Uint8Array emptyMessage() {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);
        int offset = BarrageMessageWrapper.createBarrageMessageWrapper(builder, FLATBUFFER_MAGIC,
                BarrageMessageType.None, 0);
        builder.finish(offset);
        return bbToUint8ArrayView(builder.dataBuffer());
    }

    public static InitialTableDefinition readTableDefinition(Uint8Array flightSchemaMessage) {
        return readTableDefinition(readSchemaMessage(flightSchemaMessage));
    }

    public static InitialTableDefinition readTableDefinition(Schema schema) {
        ColumnDefinition[] cols = readColumnDefinitions(schema);

        TableAttributesDefinition attributes = new TableAttributesDefinition(
                keyValuePairs("deephaven:attribute.", schema.customMetadataLength(), schema::customMetadata),
                keyValuePairs("deephaven:attribute_type.", schema.customMetadataLength(), schema::customMetadata),
                keyValuePairs("deephaven:unsent.attribute.", schema.customMetadataLength(), schema::customMetadata)
                        .keySet());

        // Parse input table metadata if present
        InputTableMetadata inputTableMetadata = parseInputTableMetadata(schema, cols);

        return new InitialTableDefinition()
                .setAttributes(attributes)
                .setColumns(cols)
                .setInputTableMetadata(inputTableMetadata);
    }

    private static InputTableMetadata parseInputTableMetadata(Schema schema, ColumnDefinition[] cols) {
        // Extract the tableMetadata from schema custom metadata
        Map<String, String> schemaMetadata =
                keyValuePairs("deephaven:", schema.customMetadataLength(), schema::customMetadata);

        consoleLog("parseInputTableMetadata: Checking for tableMetadata in schema");

        String tableMetadataBase64 = schemaMetadata.get("tableMetadata");
        if (tableMetadataBase64 == null || tableMetadataBase64.isEmpty()) {
            consoleLog("parseInputTableMetadata: No tableMetadata found in schema (null or empty)");
            return null;
        }

        consoleLog("parseInputTableMetadata: Found tableMetadata, length=" + tableMetadataBase64.length());
        InputTableMetadata metadata = new InputTableMetadata();

        try {
            // Decode base64 to Uint8Array (like Java's Base64.getDecoder().decode())
            consoleLog("parseInputTableMetadata: Decoding base64...");
            Uint8Array bytes = decodeBase64(tableMetadataBase64);
            consoleLog("parseInputTableMetadata: Decoded to " + bytes.length + " bytes");

            // The issue: JavaScript protobuf deserialization fails on google.protobuf.Any types
            // Solution: Parse the protobuf manually at the binary level to extract what we need
            consoleLog("parseInputTableMetadata: Attempting manual protobuf parsing...");
            jsinterop.base.Any result = parseProtoManually(bytes);

            if (result == null) {
                consoleLog("parseInputTableMetadata: Manual parsing returned null");
                return metadata;
            }

            consoleLog("parseInputTableMetadata: Successfully parsed, extracting column info");

            // Extract column restrictions from the manually parsed data
            for (ColumnDefinition col : cols) {
                String columnName = col.getName();
                jsinterop.base.Any columnInfo = getPropertyFromMap(result, columnName);

                if (columnInfo != null) {
                    consoleLog("parseInputTableMetadata: Found info for column '" + columnName + "'");

                    // Get restrictions array if available
                    jsinterop.base.Any restrictionsList = getProperty(columnInfo, "restrictions");
                    if (restrictionsList != null && isArray(restrictionsList)) {
                        elemental2.core.JsArray<jsinterop.base.Any> restrictions =
                            jsinterop.base.Js.uncheckedCast(restrictionsList);

                        if (restrictions.length > 0) {
                            consoleLog("parseInputTableMetadata: Column '" + columnName + "' has " +
                                       restrictions.length + " restrictions");

                            InputTableMetadata.ColumnRestrictions colRestrictions =
                                new InputTableMetadata.ColumnRestrictions();
                            for (int i = 0; i < restrictions.length; i++) {
                                colRestrictions.addRestriction(restrictions.getAt(i));
                            }
                            metadata.addColumnRestrictions(columnName, colRestrictions);
                        }
                    }
                }
            }

            consoleLog("parseInputTableMetadata: Successfully parsed metadata");
        } catch (Exception e) {
            consoleError("parseInputTableMetadata: Exception during parsing", e);
        }

        return metadata;
    }

    // Decode base64 string to Uint8Array using elemental2
    private static Uint8Array decodeBase64(String base64) {
        // Use DomGlobal.atob() to decode base64 to binary string
        String binaryString = DomGlobal.atob(base64);

        // Convert binary string to Uint8Array
        Uint8Array bytes = new Uint8Array(binaryString.length());
        for (int i = 0; i < binaryString.length(); i++) {
            bytes.setAt(i, (double) (binaryString.charAt(i) & 0xff));
        }
        return bytes;
    }

    private static native jsinterop.base.Any parseProtoManually(Uint8Array bytes) /*-{
        // Manual protobuf parsing to work around missing google.protobuf.Any
        // Based on the pattern from AddToInputTable.java
        // Use the BinaryReader from dhinternal.jspb which is the JsInterop wrapper
        try {
            console.log("parseProtoManually: Starting manual protobuf parsing");
            console.log("parseProtoManually: Bytes length =", bytes.length);

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
                console.log("parseProtoManually: Reading field", field);

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
                            console.log("parseProtoManually: InputTableMetadata field", subfield);

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
                                            console.log("parseProtoManually: Column name =", key);
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
                                                    console.log("parseProtoManually: InputTableColumnInfo field", colField);

                                                    if (colField === 1) {
                                                        // kind field
                                                        columnInfo.kind = colReader.readEnum();
                                                    } else if (colField === 2) {
                                                        // restrictions field (repeated google.protobuf.Any)
                                                        // Parse the Any message to extract type_url and value
                                                        try {
                                                            var anyBytes = colReader.readBytes();
                                                            console.log("parseProtoManually: Got Any bytes, length =", anyBytes.length);

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
                                                                    console.log("parseProtoManually: Restriction type_url =", typeUrl);
                                                                } else if (anyField === 2) {
                                                                    valueBytes = anyReader.readBytes();
                                                                    console.log("parseProtoManually: Restriction value bytes length =", valueBytes.length);
                                                                }
                                                            }

                                                            // Now parse the actual restriction based on type_url
                                                            var restriction = @io.deephaven.web.client.api.barrage.WebBarrageUtils::parseRestriction(*)(typeUrl, valueBytes);
                                                            if (restriction) {
                                                                restrictions.push(restriction);
                                                            }
                                                        } catch (e) {
                                                            console.warn("parseProtoManually: Failed to parse restriction:", e);
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
                                        console.log("parseProtoManually: Added column", key, "with", value.restrictions ? value.restrictions.length : 0, "restrictions");
                                    }
                                });
                            }
                        }
                    });

                    result = inputTableMetadata.columnInfoMap || {};
                    console.log("parseProtoManually: Parsed", Object.keys(result).length, "columns");
                }
            }

            return result;

        } catch (e) {
            console.error("parseProtoManually: Failed to manually parse protobuf:", e);
            console.error("Stack:", e.stack);
            return null;
        }
    }-*/;

    private static native jsinterop.base.Any parseRestriction(String typeUrl, Uint8Array valueBytes) /*-{
        // Parse specific restriction types based on typeUrl
        // Types from inputtable.proto:
        // - IntegerRangeRestriction
        // - DoubleRangeRestriction
        // - NotNullRestriction
        // - NonEmptyRestriction
        // - StringListRestriction

        try {
            console.log("parseRestriction: Parsing restriction type:", typeUrl);

            var BinaryReader = @io.deephaven.javascript.proto.dhinternal.jspb.BinaryReader::new(Lelemental2/core/Uint8Array;);
            var reader = new BinaryReader(valueBytes);

            // Extract the message type from the type URL
            // Format: "type.googleapis.com/io.deephaven.proto.backplane.grpc.IntegerRangeRestriction"
            // or "docs.deephaven.io/io.deephaven.proto.backplane.grpc.IntegerRangeRestriction"
            var typeName = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
            var shortName = typeName.substring(typeName.lastIndexOf('.') + 1);
            console.log("parseRestriction: Message type:", shortName);

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
                        console.log("parseRestriction: minInclusive =", restriction.minInclusive);
                    } else if (field === 2) {
                        restriction.maxInclusive = reader.readInt64();
                        console.log("parseRestriction: maxInclusive =", restriction.maxInclusive);
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
                        console.log("parseRestriction: minInclusive =", restriction.minInclusive);
                    } else if (field === 2) {
                        restriction.maxInclusive = reader.readDouble();
                        console.log("parseRestriction: maxInclusive =", restriction.maxInclusive);
                    }
                }
            } else if (shortName === 'NotNullRestriction') {
                // No fields - just the type
                restriction.notNull = true;
                console.log("parseRestriction: NotNull restriction");
            } else if (shortName === 'NonEmptyRestriction') {
                // No fields - just the type
                restriction.nonEmpty = true;
                console.log("parseRestriction: NonEmpty restriction");
            } else if (shortName === 'StringListRestriction') {
                // Field 1 = allowed_values (repeated string)
                restriction.allowedValues = [];
                while (reader.nextField()) {
                    if (reader.isEndGroup()) break;
                    var field = reader.getFieldNumber();
                    if (field === 1) {
                        var value = reader.readString();
                        restriction.allowedValues.push(value);
                        console.log("parseRestriction: allowed value =", value);
                    }
                }
            } else {
                console.warn("parseRestriction: Unknown restriction type:", shortName);
                restriction.raw = valueBytes;
            }

            return restriction;

        } catch (e) {
            console.error("parseRestriction: Failed to parse restriction:", e);
            return null;
        }
    }-*/;

    private static native jsinterop.base.Any getPropertyFromMap(jsinterop.base.Any map, String key) /*-{
        if (map == null) return null;
        return map[key] || null;
    }-*/;

    private static native jsinterop.base.Any getProperty(jsinterop.base.Any obj, String propertyName) /*-{
        if (obj == null) return null;
        var getter = 'get' + propertyName.charAt(0).toUpperCase() + propertyName.slice(1);
        if (typeof obj[getter] === 'function') {
            return obj[getter]();
        }
        return obj[propertyName];
    }-*/;

    private static native jsinterop.base.Any getMapEntry(jsinterop.base.Any map, String key) /*-{
        if (map == null) return null;
        if (typeof map.get === 'function') {
            return map.get(key);
        }
        if (typeof map.getMap === 'function') {
            var m = map.getMap();
            return m ? m[key] : null;
        }
        return map[key];
    }-*/;

    private static native boolean isArray(jsinterop.base.Any obj) /*-{
        return Array.isArray(obj);
    }-*/;

    private static native void consoleLog(String message) /*-{
        console.log(message);
    }-*/;

    private static native void consoleError(String message, Exception e) /*-{
        console.error(message, e);
    }-*/;

    private static native void logProtoObject(String label, jsinterop.base.Any obj) /*-{
        console.log(label + ":", obj);
        if (obj != null) {
            console.log(label + " keys:", Object.keys(obj));
            console.log(label + " methods:", Object.getOwnPropertyNames(Object.getPrototypeOf(obj)).filter(function(name) {
                return typeof obj[name] === 'function';
            }));
        }
    }-*/;

    private static ColumnDefinition[] readColumnDefinitions(Schema schema) {
        ColumnDefinition[] cols = new ColumnDefinition[(int) schema.fieldsLength()];
        for (int i = 0; i < schema.fieldsLength(); i++) {
            cols[i] = new ColumnDefinition(i, schema.fields(i));
        }
        return cols;
    }

    public static Schema readSchemaMessage(Uint8Array flightSchemaMessage) {
        // we conform to flight's schema representation of:
        // - IPC_CONTINUATION_TOKEN (4-byte int of -1)
        // - message size (4-byte int)
        // - a Message wrapping the schema
        ByteBuffer bb = TypedArrayHelper.wrap(flightSchemaMessage);
        bb.position(bb.position() + 8);
        Message headerMessage = Message.getRootAsMessage(bb);

        assert headerMessage.headerType() == MessageHeader.Schema;
        return (Schema) headerMessage.header(new Schema());
    }

    public static Map<String, String> keyValuePairs(String filterPrefix, double count,
            IntFunction<KeyValue> accessor) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < count; i++) {
            KeyValue pair = accessor.apply(i);
            String key = pair.key();
            if (key.startsWith(filterPrefix)) {
                key = key.substring(filterPrefix.length());
                String oldValue = map.put(key, pair.value());
                assert oldValue == null : key + " had " + oldValue + ", replaced with " + pair.value();
            }
        }
        return map;
    }

    public static ByteBuffer serializeRanges(Set<RangeSet> rangeSets) {
        final RangeSet s;
        if (rangeSets.isEmpty()) {
            return ByteBuffer.allocate(0);
        } else if (rangeSets.size() == 1) {
            s = rangeSets.iterator().next();
        } else {
            s = new RangeSet();
            for (RangeSet rangeSet : rangeSets) {
                s.addRangeSet(rangeSet);
            }
        }

        return CompressedRangeSetReader.writeRange(s);
    }
}

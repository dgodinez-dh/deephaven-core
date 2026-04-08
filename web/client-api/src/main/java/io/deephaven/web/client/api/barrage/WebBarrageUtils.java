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
import io.deephaven.web.client.api.barrage.util.ColumnRestrictionUtils;
import io.deephaven.web.client.fu.JsLog;
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

        String tableMetadataBase64 = schemaMetadata.get("tableMetadata");
        if (tableMetadataBase64 == null || tableMetadataBase64.isEmpty()) {
            return null;
        }

        InputTableMetadata metadata = new InputTableMetadata();

        try {
            // Decode base64 to Uint8Array (like Java's Base64.getDecoder().decode())
            Uint8Array bytes = decodeBase64(tableMetadataBase64);

            // The issue: JavaScript protobuf deserialization fails on google.protobuf.Any types
            // Solution: Parse the protobuf manually at the binary level to extract what we need
            jsinterop.base.Any result = ColumnRestrictionUtils.parseProtoManually(bytes);

            if (result == null) {
                return metadata;
            }

            // Extract column restrictions from the manually parsed data
            for (ColumnDefinition col : cols) {
                String columnName = col.getName();
                jsinterop.base.Any columnInfo = getPropertyFromMap(result, columnName);

                if (columnInfo != null) {
                    // Get restrictions array if available
                    jsinterop.base.Any restrictionsList = getProperty(columnInfo, "restrictions");
                    if (restrictionsList != null && isArray(restrictionsList)) {
                        elemental2.core.JsArray<jsinterop.base.Any> restrictions =
                            jsinterop.base.Js.uncheckedCast(restrictionsList);

                        if (restrictions.length > 0) {
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
        } catch (Exception e) {
            JsLog.warn("Failed to parse input table metadata:", e);
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


    private static native boolean isArray(jsinterop.base.Any obj) /*-{
        return Array.isArray(obj);
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

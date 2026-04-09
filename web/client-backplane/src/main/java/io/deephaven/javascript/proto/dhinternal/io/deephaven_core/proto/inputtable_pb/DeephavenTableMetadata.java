//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.inputtable_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.inputtable_pb.DeephavenTableMetadata",
        namespace = JsPackage.GLOBAL)
public class DeephavenTableMetadata {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface InputTableMetadataFieldType {
            @JsOverlay
            static DeephavenTableMetadata.ToObjectReturnType.InputTableMetadataFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<JsArray<Object>> getColumnInfoMap();

            @JsProperty
            void setColumnInfoMap(JsArray<JsArray<Object>> columnInfoMap);

            @JsOverlay
            default void setColumnInfoMap(Object[][] columnInfoMap) {
                setColumnInfoMap(Js.<JsArray<JsArray<Object>>>uncheckedCast(columnInfoMap));
            }
        }

        @JsOverlay
        static DeephavenTableMetadata.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        DeephavenTableMetadata.ToObjectReturnType.InputTableMetadataFieldType getInputTableMetadata();

        @JsProperty
        void setInputTableMetadata(
                DeephavenTableMetadata.ToObjectReturnType.InputTableMetadataFieldType inputTableMetadata);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface InputTableMetadataFieldType {
            @JsOverlay
            static DeephavenTableMetadata.ToObjectReturnType0.InputTableMetadataFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<JsArray<Object>> getColumnInfoMap();

            @JsProperty
            void setColumnInfoMap(JsArray<JsArray<Object>> columnInfoMap);

            @JsOverlay
            default void setColumnInfoMap(Object[][] columnInfoMap) {
                setColumnInfoMap(Js.<JsArray<JsArray<Object>>>uncheckedCast(columnInfoMap));
            }
        }

        @JsOverlay
        static DeephavenTableMetadata.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        DeephavenTableMetadata.ToObjectReturnType0.InputTableMetadataFieldType getInputTableMetadata();

        @JsProperty
        void setInputTableMetadata(
                DeephavenTableMetadata.ToObjectReturnType0.InputTableMetadataFieldType inputTableMetadata);
    }

    public static native DeephavenTableMetadata deserializeBinary(Uint8Array bytes);

    public static native DeephavenTableMetadata deserializeBinaryFromReader(
            DeephavenTableMetadata message, Object reader);

    public static native void serializeBinaryToWriter(DeephavenTableMetadata message, Object writer);

    public static native DeephavenTableMetadata.ToObjectReturnType toObject(
            boolean includeInstance, DeephavenTableMetadata msg);

    public native void clearInputTableMetadata();

    public native InputTableMetadata getInputTableMetadata();

    public native boolean hasInputTableMetadata();

    public native Uint8Array serializeBinary();

    public native void setInputTableMetadata();

    public native void setInputTableMetadata(InputTableMetadata value);

    public native DeephavenTableMetadata.ToObjectReturnType0 toObject();

    public native DeephavenTableMetadata.ToObjectReturnType0 toObject(boolean includeInstance);
}

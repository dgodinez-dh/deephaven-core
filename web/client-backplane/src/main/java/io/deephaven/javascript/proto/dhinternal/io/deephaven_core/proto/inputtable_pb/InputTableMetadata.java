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
        name = "dhinternal.io.deephaven_core.proto.inputtable_pb.InputTableMetadata",
        namespace = JsPackage.GLOBAL)
public class InputTableMetadata {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static InputTableMetadata.ToObjectReturnType create() {
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

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static InputTableMetadata.ToObjectReturnType0 create() {
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

    public static native InputTableMetadata deserializeBinary(Uint8Array bytes);

    public static native InputTableMetadata deserializeBinaryFromReader(
            InputTableMetadata message, Object reader);

    public static native void serializeBinaryToWriter(InputTableMetadata message, Object writer);

    public static native InputTableMetadata.ToObjectReturnType toObject(
            boolean includeInstance, InputTableMetadata msg);

    public native void clearColumnInfoMap();

    public native Object getColumnInfoMap();

    public native Uint8Array serializeBinary();

    public native InputTableMetadata.ToObjectReturnType0 toObject();

    public native InputTableMetadata.ToObjectReturnType0 toObject(boolean includeInstance);
}

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
        name = "dhinternal.io.deephaven_core.proto.inputtable_pb.StringListRestriction",
        namespace = JsPackage.GLOBAL)
public class StringListRestriction {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static StringListRestriction.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getAllowedValuesList();

        @JsProperty
        void setAllowedValuesList(JsArray<String> allowedValuesList);

        @JsOverlay
        default void setAllowedValuesList(String[] allowedValuesList) {
            setAllowedValuesList(Js.<JsArray<String>>uncheckedCast(allowedValuesList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static StringListRestriction.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getAllowedValuesList();

        @JsProperty
        void setAllowedValuesList(JsArray<String> allowedValuesList);

        @JsOverlay
        default void setAllowedValuesList(String[] allowedValuesList) {
            setAllowedValuesList(Js.<JsArray<String>>uncheckedCast(allowedValuesList));
        }
    }

    public static native StringListRestriction deserializeBinary(Uint8Array bytes);

    public static native StringListRestriction deserializeBinaryFromReader(
            StringListRestriction message, Object reader);

    public static native void serializeBinaryToWriter(StringListRestriction message, Object writer);

    public static native StringListRestriction.ToObjectReturnType toObject(
            boolean includeInstance, StringListRestriction msg);

    public native String addAllowedValues(String value, double index);

    public native String addAllowedValues(String value);

    public native void clearAllowedValuesList();

    public native JsArray<String> getAllowedValuesList();

    public native Uint8Array serializeBinary();

    public native void setAllowedValuesList(JsArray<String> value);

    @JsOverlay
    public final void setAllowedValuesList(String[] value) {
        setAllowedValuesList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native StringListRestriction.ToObjectReturnType0 toObject();

    public native StringListRestriction.ToObjectReturnType0 toObject(boolean includeInstance);
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.inputtable_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.inputtable_pb.IntegerRangeRestriction",
        namespace = JsPackage.GLOBAL)
public class IntegerRangeRestriction {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static IntegerRangeRestriction.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getMaxInclusive();

        @JsProperty
        double getMinInclusive();

        @JsProperty
        void setMaxInclusive(double maxInclusive);

        @JsProperty
        void setMinInclusive(double minInclusive);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static IntegerRangeRestriction.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getMaxInclusive();

        @JsProperty
        double getMinInclusive();

        @JsProperty
        void setMaxInclusive(double maxInclusive);

        @JsProperty
        void setMinInclusive(double minInclusive);
    }

    public static native IntegerRangeRestriction deserializeBinary(Uint8Array bytes);

    public static native IntegerRangeRestriction deserializeBinaryFromReader(
            IntegerRangeRestriction message, Object reader);

    public static native void serializeBinaryToWriter(IntegerRangeRestriction message, Object writer);

    public static native IntegerRangeRestriction.ToObjectReturnType toObject(
            boolean includeInstance, IntegerRangeRestriction msg);

    public native void clearMaxInclusive();

    public native void clearMinInclusive();

    public native double getMaxInclusive();

    public native double getMinInclusive();

    public native boolean hasMaxInclusive();

    public native boolean hasMinInclusive();

    public native Uint8Array serializeBinary();

    public native void setMaxInclusive(double value);

    public native void setMinInclusive(double value);

    public native IntegerRangeRestriction.ToObjectReturnType0 toObject();

    public native IntegerRangeRestriction.ToObjectReturnType0 toObject(boolean includeInstance);
}

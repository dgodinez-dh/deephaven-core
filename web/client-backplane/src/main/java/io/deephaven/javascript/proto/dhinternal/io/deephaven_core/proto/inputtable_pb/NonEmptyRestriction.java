//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.inputtable_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.inputtable_pb.NonEmptyRestriction",
        namespace = JsPackage.GLOBAL)
public class NonEmptyRestriction {
    public static native NonEmptyRestriction deserializeBinary(Uint8Array bytes);

    public static native NonEmptyRestriction deserializeBinaryFromReader(
            NonEmptyRestriction message, Object reader);

    public static native void serializeBinaryToWriter(NonEmptyRestriction message, Object writer);

    public static native Object toObject(boolean includeInstance, NonEmptyRestriction msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}

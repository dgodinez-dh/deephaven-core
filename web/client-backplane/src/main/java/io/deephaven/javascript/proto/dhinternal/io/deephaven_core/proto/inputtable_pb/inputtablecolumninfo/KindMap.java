//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.inputtable_pb.inputtablecolumninfo;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.inputtable_pb.InputTableColumnInfo.KindMap",
        namespace = JsPackage.GLOBAL)
public interface KindMap {
    @JsOverlay
    static KindMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "KIND_KEY")
    double getKIND_KEY();

    @JsProperty(name = "KIND_UNKNOWN")
    double getKIND_UNKNOWN();

    @JsProperty(name = "KIND_VALUE")
    double getKIND_VALUE();

    @JsProperty(name = "KIND_KEY")
    void setKIND_KEY(double KIND_KEY);

    @JsProperty(name = "KIND_UNKNOWN")
    void setKIND_UNKNOWN(double KIND_UNKNOWN);

    @JsProperty(name = "KIND_VALUE")
    void setKIND_VALUE(double KIND_VALUE);
}

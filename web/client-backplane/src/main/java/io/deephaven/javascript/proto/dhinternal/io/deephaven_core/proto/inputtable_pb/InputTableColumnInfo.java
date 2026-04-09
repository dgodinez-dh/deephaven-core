//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.inputtable_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.inputtable_pb.inputtablecolumninfo.KindMap;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.inputtable_pb.InputTableColumnInfo",
        namespace = JsPackage.GLOBAL)
public class InputTableColumnInfo {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface RestrictionsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetValueUnionType {
                @JsOverlay
                static InputTableColumnInfo.ToObjectReturnType.RestrictionsListFieldType.GetValueUnionType of(
                        Object o) {
                    return Js.cast(o);
                }

                @JsOverlay
                default String asString() {
                    return Js.asString(this);
                }

                @JsOverlay
                default Uint8Array asUint8Array() {
                    return Js.cast(this);
                }

                @JsOverlay
                default boolean isString() {
                    return (Object) this instanceof String;
                }

                @JsOverlay
                default boolean isUint8Array() {
                    return (Object) this instanceof Uint8Array;
                }
            }

            @JsOverlay
            static InputTableColumnInfo.ToObjectReturnType.RestrictionsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getTypeUrl();

            @JsProperty
            InputTableColumnInfo.ToObjectReturnType.RestrictionsListFieldType.GetValueUnionType getValue();

            @JsProperty
            void setTypeUrl(String typeUrl);

            @JsProperty
            void setValue(
                    InputTableColumnInfo.ToObjectReturnType.RestrictionsListFieldType.GetValueUnionType value);

            @JsOverlay
            default void setValue(String value) {
                setValue(
                        Js.<InputTableColumnInfo.ToObjectReturnType.RestrictionsListFieldType.GetValueUnionType>uncheckedCast(
                                value));
            }

            @JsOverlay
            default void setValue(Uint8Array value) {
                setValue(
                        Js.<InputTableColumnInfo.ToObjectReturnType.RestrictionsListFieldType.GetValueUnionType>uncheckedCast(
                                value));
            }
        }

        @JsOverlay
        static InputTableColumnInfo.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getKind();

        @JsProperty
        JsArray<InputTableColumnInfo.ToObjectReturnType.RestrictionsListFieldType> getRestrictionsList();

        @JsProperty
        void setKind(double kind);

        @JsProperty
        void setRestrictionsList(
                JsArray<InputTableColumnInfo.ToObjectReturnType.RestrictionsListFieldType> restrictionsList);

        @JsOverlay
        default void setRestrictionsList(
                InputTableColumnInfo.ToObjectReturnType.RestrictionsListFieldType[] restrictionsList) {
            setRestrictionsList(
                    Js.<JsArray<InputTableColumnInfo.ToObjectReturnType.RestrictionsListFieldType>>uncheckedCast(
                            restrictionsList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface RestrictionsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetValueUnionType {
                @JsOverlay
                static InputTableColumnInfo.ToObjectReturnType0.RestrictionsListFieldType.GetValueUnionType of(
                        Object o) {
                    return Js.cast(o);
                }

                @JsOverlay
                default String asString() {
                    return Js.asString(this);
                }

                @JsOverlay
                default Uint8Array asUint8Array() {
                    return Js.cast(this);
                }

                @JsOverlay
                default boolean isString() {
                    return (Object) this instanceof String;
                }

                @JsOverlay
                default boolean isUint8Array() {
                    return (Object) this instanceof Uint8Array;
                }
            }

            @JsOverlay
            static InputTableColumnInfo.ToObjectReturnType0.RestrictionsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getTypeUrl();

            @JsProperty
            InputTableColumnInfo.ToObjectReturnType0.RestrictionsListFieldType.GetValueUnionType getValue();

            @JsProperty
            void setTypeUrl(String typeUrl);

            @JsProperty
            void setValue(
                    InputTableColumnInfo.ToObjectReturnType0.RestrictionsListFieldType.GetValueUnionType value);

            @JsOverlay
            default void setValue(String value) {
                setValue(
                        Js.<InputTableColumnInfo.ToObjectReturnType0.RestrictionsListFieldType.GetValueUnionType>uncheckedCast(
                                value));
            }

            @JsOverlay
            default void setValue(Uint8Array value) {
                setValue(
                        Js.<InputTableColumnInfo.ToObjectReturnType0.RestrictionsListFieldType.GetValueUnionType>uncheckedCast(
                                value));
            }
        }

        @JsOverlay
        static InputTableColumnInfo.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getKind();

        @JsProperty
        JsArray<InputTableColumnInfo.ToObjectReturnType0.RestrictionsListFieldType> getRestrictionsList();

        @JsProperty
        void setKind(double kind);

        @JsProperty
        void setRestrictionsList(
                JsArray<InputTableColumnInfo.ToObjectReturnType0.RestrictionsListFieldType> restrictionsList);

        @JsOverlay
        default void setRestrictionsList(
                InputTableColumnInfo.ToObjectReturnType0.RestrictionsListFieldType[] restrictionsList) {
            setRestrictionsList(
                    Js.<JsArray<InputTableColumnInfo.ToObjectReturnType0.RestrictionsListFieldType>>uncheckedCast(
                            restrictionsList));
        }
    }

    public static KindMap Kind;

    public static native InputTableColumnInfo deserializeBinary(Uint8Array bytes);

    public static native InputTableColumnInfo deserializeBinaryFromReader(
            InputTableColumnInfo message, Object reader);

    public static native void serializeBinaryToWriter(InputTableColumnInfo message, Object writer);

    public static native InputTableColumnInfo.ToObjectReturnType toObject(
            boolean includeInstance, InputTableColumnInfo msg);

    public native Object addRestrictions();

    public native Object addRestrictions(Object value, double index);

    public native Object addRestrictions(Object value);

    public native void clearRestrictionsList();

    public native double getKind();

    public native JsArray<Object> getRestrictionsList();

    public native Uint8Array serializeBinary();

    public native void setKind(double value);

    public native void setRestrictionsList(JsArray<Object> value);

    @JsOverlay
    public final void setRestrictionsList(Object[] value) {
        setRestrictionsList(Js.<JsArray<Object>>uncheckedCast(value));
    }

    public native InputTableColumnInfo.ToObjectReturnType0 toObject();

    public native InputTableColumnInfo.ToObjectReturnType0 toObject(boolean includeInstance);
}

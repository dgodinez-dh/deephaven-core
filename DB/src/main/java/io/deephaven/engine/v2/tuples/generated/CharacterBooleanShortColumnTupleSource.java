package io.deephaven.engine.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.util.BooleanUtils;
import io.deephaven.engine.util.tuples.generated.CharByteShortTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.structures.chunk.Attributes;
import io.deephaven.engine.structures.chunk.CharChunk;
import io.deephaven.engine.structures.chunk.Chunk;
import io.deephaven.engine.structures.chunk.ObjectChunk;
import io.deephaven.engine.structures.chunk.ShortChunk;
import io.deephaven.engine.structures.chunk.WritableChunk;
import io.deephaven.engine.structures.chunk.WritableObjectChunk;
import io.deephaven.engine.v2.tuples.AbstractTupleSource;
import io.deephaven.engine.v2.tuples.ThreeColumnTupleSourceFactory;
import io.deephaven.engine.v2.tuples.TupleSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Character, Boolean, and Short.
 * <p>Generated by {@link io.deephaven.engine.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class CharacterBooleanShortColumnTupleSource extends AbstractTupleSource<CharByteShortTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link CharacterBooleanShortColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<CharByteShortTuple, Character, Boolean, Short> FACTORY = new Factory();

    private final ColumnSource<Character> columnSource1;
    private final ColumnSource<Boolean> columnSource2;
    private final ColumnSource<Short> columnSource3;

    public CharacterBooleanShortColumnTupleSource(
            @NotNull final ColumnSource<Character> columnSource1,
            @NotNull final ColumnSource<Boolean> columnSource2,
            @NotNull final ColumnSource<Short> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final CharByteShortTuple createTuple(final long indexKey) {
        return new CharByteShortTuple(
                columnSource1.getChar(indexKey),
                BooleanUtils.booleanAsByte(columnSource2.getBoolean(indexKey)),
                columnSource3.getShort(indexKey)
        );
    }

    @Override
    public final CharByteShortTuple createPreviousTuple(final long indexKey) {
        return new CharByteShortTuple(
                columnSource1.getPrevChar(indexKey),
                BooleanUtils.booleanAsByte(columnSource2.getPrevBoolean(indexKey)),
                columnSource3.getPrevShort(indexKey)
        );
    }

    @Override
    public final CharByteShortTuple createTupleFromValues(@NotNull final Object... values) {
        return new CharByteShortTuple(
                TypeUtils.unbox((Character)values[0]),
                BooleanUtils.booleanAsByte((Boolean)values[1]),
                TypeUtils.unbox((Short)values[2])
        );
    }

    @Override
    public final CharByteShortTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new CharByteShortTuple(
                TypeUtils.unbox((Character)values[0]),
                BooleanUtils.booleanAsByte((Boolean)values[1]),
                TypeUtils.unbox((Short)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final CharByteShortTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) BooleanUtils.byteAsBoolean(tuple.getSecondElement()));
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationIndexKey, tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final CharByteShortTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                BooleanUtils.byteAsBoolean(tuple.getSecondElement()),
                TypeUtils.box(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final CharByteShortTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return BooleanUtils.byteAsBoolean(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final CharByteShortTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return BooleanUtils.byteAsBoolean(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<CharByteShortTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        CharChunk<Attributes.Values> chunk1 = chunks[0].asCharChunk();
        ObjectChunk<Boolean, Attributes.Values> chunk2 = chunks[1].asObjectChunk();
        ShortChunk<Attributes.Values> chunk3 = chunks[2].asShortChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new CharByteShortTuple(chunk1.get(ii), BooleanUtils.booleanAsByte(chunk2.get(ii)), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link CharacterBooleanShortColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<CharByteShortTuple, Character, Boolean, Short> {

        private Factory() {
        }

        @Override
        public TupleSource<CharByteShortTuple> create(
                @NotNull final ColumnSource<Character> columnSource1,
                @NotNull final ColumnSource<Boolean> columnSource2,
                @NotNull final ColumnSource<Short> columnSource3
        ) {
            return new CharacterBooleanShortColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}

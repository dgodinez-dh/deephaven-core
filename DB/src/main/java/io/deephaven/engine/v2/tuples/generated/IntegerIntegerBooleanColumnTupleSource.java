package io.deephaven.engine.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.util.BooleanUtils;
import io.deephaven.engine.util.tuples.generated.IntIntByteTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.structures.chunk.Attributes;
import io.deephaven.engine.structures.chunk.Chunk;
import io.deephaven.engine.structures.chunk.IntChunk;
import io.deephaven.engine.structures.chunk.ObjectChunk;
import io.deephaven.engine.structures.chunk.WritableChunk;
import io.deephaven.engine.structures.chunk.WritableObjectChunk;
import io.deephaven.engine.v2.tuples.AbstractTupleSource;
import io.deephaven.engine.v2.tuples.ThreeColumnTupleSourceFactory;
import io.deephaven.engine.v2.tuples.TupleSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Integer, Integer, and Boolean.
 * <p>Generated by {@link io.deephaven.engine.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class IntegerIntegerBooleanColumnTupleSource extends AbstractTupleSource<IntIntByteTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link IntegerIntegerBooleanColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<IntIntByteTuple, Integer, Integer, Boolean> FACTORY = new Factory();

    private final ColumnSource<Integer> columnSource1;
    private final ColumnSource<Integer> columnSource2;
    private final ColumnSource<Boolean> columnSource3;

    public IntegerIntegerBooleanColumnTupleSource(
            @NotNull final ColumnSource<Integer> columnSource1,
            @NotNull final ColumnSource<Integer> columnSource2,
            @NotNull final ColumnSource<Boolean> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final IntIntByteTuple createTuple(final long indexKey) {
        return new IntIntByteTuple(
                columnSource1.getInt(indexKey),
                columnSource2.getInt(indexKey),
                BooleanUtils.booleanAsByte(columnSource3.getBoolean(indexKey))
        );
    }

    @Override
    public final IntIntByteTuple createPreviousTuple(final long indexKey) {
        return new IntIntByteTuple(
                columnSource1.getPrevInt(indexKey),
                columnSource2.getPrevInt(indexKey),
                BooleanUtils.booleanAsByte(columnSource3.getPrevBoolean(indexKey))
        );
    }

    @Override
    public final IntIntByteTuple createTupleFromValues(@NotNull final Object... values) {
        return new IntIntByteTuple(
                TypeUtils.unbox((Integer)values[0]),
                TypeUtils.unbox((Integer)values[1]),
                BooleanUtils.booleanAsByte((Boolean)values[2])
        );
    }

    @Override
    public final IntIntByteTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new IntIntByteTuple(
                TypeUtils.unbox((Integer)values[0]),
                TypeUtils.unbox((Integer)values[1]),
                BooleanUtils.booleanAsByte((Boolean)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final IntIntByteTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) BooleanUtils.byteAsBoolean(tuple.getThirdElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final IntIntByteTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                TypeUtils.box(tuple.getSecondElement()),
                BooleanUtils.byteAsBoolean(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final IntIntByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return BooleanUtils.byteAsBoolean(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final IntIntByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return BooleanUtils.byteAsBoolean(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<IntIntByteTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        IntChunk<Attributes.Values> chunk1 = chunks[0].asIntChunk();
        IntChunk<Attributes.Values> chunk2 = chunks[1].asIntChunk();
        ObjectChunk<Boolean, Attributes.Values> chunk3 = chunks[2].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new IntIntByteTuple(chunk1.get(ii), chunk2.get(ii), BooleanUtils.booleanAsByte(chunk3.get(ii))));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link IntegerIntegerBooleanColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<IntIntByteTuple, Integer, Integer, Boolean> {

        private Factory() {
        }

        @Override
        public TupleSource<IntIntByteTuple> create(
                @NotNull final ColumnSource<Integer> columnSource1,
                @NotNull final ColumnSource<Integer> columnSource2,
                @NotNull final ColumnSource<Boolean> columnSource3
        ) {
            return new IntegerIntegerBooleanColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}

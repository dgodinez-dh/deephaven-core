package io.deephaven.engine.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.util.BooleanUtils;
import io.deephaven.engine.util.tuples.generated.ShortDoubleByteTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.structures.source.WritableSource;
import io.deephaven.engine.structures.chunk.Attributes;
import io.deephaven.engine.structures.chunk.Chunk;
import io.deephaven.engine.structures.chunk.DoubleChunk;
import io.deephaven.engine.structures.chunk.ObjectChunk;
import io.deephaven.engine.structures.chunk.ShortChunk;
import io.deephaven.engine.structures.chunk.WritableChunk;
import io.deephaven.engine.structures.chunk.WritableObjectChunk;
import io.deephaven.engine.v2.tuples.AbstractTupleSource;
import io.deephaven.engine.v2.tuples.ThreeColumnTupleSourceFactory;
import io.deephaven.engine.structures.source.TupleSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Short, Double, and Boolean.
 * <p>Generated by {@link io.deephaven.engine.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ShortDoubleBooleanColumnTupleSource extends AbstractTupleSource<ShortDoubleByteTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ShortDoubleBooleanColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ShortDoubleByteTuple, Short, Double, Boolean> FACTORY = new Factory();

    private final ColumnSource<Short> columnSource1;
    private final ColumnSource<Double> columnSource2;
    private final ColumnSource<Boolean> columnSource3;

    public ShortDoubleBooleanColumnTupleSource(
            @NotNull final ColumnSource<Short> columnSource1,
            @NotNull final ColumnSource<Double> columnSource2,
            @NotNull final ColumnSource<Boolean> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ShortDoubleByteTuple createTuple(final long indexKey) {
        return new ShortDoubleByteTuple(
                columnSource1.getShort(indexKey),
                columnSource2.getDouble(indexKey),
                BooleanUtils.booleanAsByte(columnSource3.getBoolean(indexKey))
        );
    }

    @Override
    public final ShortDoubleByteTuple createPreviousTuple(final long indexKey) {
        return new ShortDoubleByteTuple(
                columnSource1.getPrevShort(indexKey),
                columnSource2.getPrevDouble(indexKey),
                BooleanUtils.booleanAsByte(columnSource3.getPrevBoolean(indexKey))
        );
    }

    @Override
    public final ShortDoubleByteTuple createTupleFromValues(@NotNull final Object... values) {
        return new ShortDoubleByteTuple(
                TypeUtils.unbox((Short)values[0]),
                TypeUtils.unbox((Double)values[1]),
                BooleanUtils.booleanAsByte((Boolean)values[2])
        );
    }

    @Override
    public final ShortDoubleByteTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ShortDoubleByteTuple(
                TypeUtils.unbox((Short)values[0]),
                TypeUtils.unbox((Double)values[1]),
                BooleanUtils.booleanAsByte((Boolean)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ShortDoubleByteTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
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
    public final Object exportToExternalKey(@NotNull final ShortDoubleByteTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                TypeUtils.box(tuple.getSecondElement()),
                BooleanUtils.byteAsBoolean(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final ShortDoubleByteTuple tuple, int elementIndex) {
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
    public final Object exportElementReinterpreted(@NotNull final ShortDoubleByteTuple tuple, int elementIndex) {
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
        WritableObjectChunk<ShortDoubleByteTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ShortChunk<Attributes.Values> chunk1 = chunks[0].asShortChunk();
        DoubleChunk<Attributes.Values> chunk2 = chunks[1].asDoubleChunk();
        ObjectChunk<Boolean, Attributes.Values> chunk3 = chunks[2].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ShortDoubleByteTuple(chunk1.get(ii), chunk2.get(ii), BooleanUtils.booleanAsByte(chunk3.get(ii))));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ShortDoubleBooleanColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ShortDoubleByteTuple, Short, Double, Boolean> {

        private Factory() {
        }

        @Override
        public TupleSource<ShortDoubleByteTuple> create(
                @NotNull final ColumnSource<Short> columnSource1,
                @NotNull final ColumnSource<Double> columnSource2,
                @NotNull final ColumnSource<Boolean> columnSource3
        ) {
            return new ShortDoubleBooleanColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}

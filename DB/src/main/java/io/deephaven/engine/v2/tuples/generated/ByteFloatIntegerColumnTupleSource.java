package io.deephaven.engine.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.util.tuples.generated.ByteFloatIntTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.structures.source.WritableSource;
import io.deephaven.engine.structures.chunk.Attributes;
import io.deephaven.engine.structures.chunk.ByteChunk;
import io.deephaven.engine.structures.chunk.Chunk;
import io.deephaven.engine.structures.chunk.FloatChunk;
import io.deephaven.engine.structures.chunk.IntChunk;
import io.deephaven.engine.structures.chunk.WritableChunk;
import io.deephaven.engine.structures.chunk.WritableObjectChunk;
import io.deephaven.engine.v2.tuples.AbstractTupleSource;
import io.deephaven.engine.v2.tuples.ThreeColumnTupleSourceFactory;
import io.deephaven.engine.structures.source.TupleSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Byte, Float, and Integer.
 * <p>Generated by {@link io.deephaven.engine.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ByteFloatIntegerColumnTupleSource extends AbstractTupleSource<ByteFloatIntTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ByteFloatIntegerColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ByteFloatIntTuple, Byte, Float, Integer> FACTORY = new Factory();

    private final ColumnSource<Byte> columnSource1;
    private final ColumnSource<Float> columnSource2;
    private final ColumnSource<Integer> columnSource3;

    public ByteFloatIntegerColumnTupleSource(
            @NotNull final ColumnSource<Byte> columnSource1,
            @NotNull final ColumnSource<Float> columnSource2,
            @NotNull final ColumnSource<Integer> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ByteFloatIntTuple createTuple(final long indexKey) {
        return new ByteFloatIntTuple(
                columnSource1.getByte(indexKey),
                columnSource2.getFloat(indexKey),
                columnSource3.getInt(indexKey)
        );
    }

    @Override
    public final ByteFloatIntTuple createPreviousTuple(final long indexKey) {
        return new ByteFloatIntTuple(
                columnSource1.getPrevByte(indexKey),
                columnSource2.getPrevFloat(indexKey),
                columnSource3.getPrevInt(indexKey)
        );
    }

    @Override
    public final ByteFloatIntTuple createTupleFromValues(@NotNull final Object... values) {
        return new ByteFloatIntTuple(
                TypeUtils.unbox((Byte)values[0]),
                TypeUtils.unbox((Float)values[1]),
                TypeUtils.unbox((Integer)values[2])
        );
    }

    @Override
    public final ByteFloatIntTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ByteFloatIntTuple(
                TypeUtils.unbox((Byte)values[0]),
                TypeUtils.unbox((Float)values[1]),
                TypeUtils.unbox((Integer)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ByteFloatIntTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationIndexKey, tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final ByteFloatIntTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                TypeUtils.box(tuple.getSecondElement()),
                TypeUtils.box(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final ByteFloatIntTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ByteFloatIntTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<ByteFloatIntTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ByteChunk<Attributes.Values> chunk1 = chunks[0].asByteChunk();
        FloatChunk<Attributes.Values> chunk2 = chunks[1].asFloatChunk();
        IntChunk<Attributes.Values> chunk3 = chunks[2].asIntChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ByteFloatIntTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ByteFloatIntegerColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ByteFloatIntTuple, Byte, Float, Integer> {

        private Factory() {
        }

        @Override
        public TupleSource<ByteFloatIntTuple> create(
                @NotNull final ColumnSource<Byte> columnSource1,
                @NotNull final ColumnSource<Float> columnSource2,
                @NotNull final ColumnSource<Integer> columnSource3
        ) {
            return new ByteFloatIntegerColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}

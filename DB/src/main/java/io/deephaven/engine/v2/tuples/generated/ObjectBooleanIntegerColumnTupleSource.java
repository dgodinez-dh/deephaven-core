package io.deephaven.engine.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.util.BooleanUtils;
import io.deephaven.engine.util.tuples.generated.ObjectByteIntTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.structures.source.WritableSource;
import io.deephaven.engine.structures.chunk.Attributes;
import io.deephaven.engine.structures.chunk.Chunk;
import io.deephaven.engine.structures.chunk.IntChunk;
import io.deephaven.engine.structures.chunk.ObjectChunk;
import io.deephaven.engine.structures.chunk.WritableChunk;
import io.deephaven.engine.structures.chunk.WritableObjectChunk;
import io.deephaven.engine.v2.tuples.AbstractTupleSource;
import io.deephaven.engine.v2.tuples.ThreeColumnTupleSourceFactory;
import io.deephaven.engine.structures.source.TupleSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Object, Boolean, and Integer.
 * <p>Generated by {@link io.deephaven.engine.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ObjectBooleanIntegerColumnTupleSource extends AbstractTupleSource<ObjectByteIntTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ObjectBooleanIntegerColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ObjectByteIntTuple, Object, Boolean, Integer> FACTORY = new Factory();

    private final ColumnSource<Object> columnSource1;
    private final ColumnSource<Boolean> columnSource2;
    private final ColumnSource<Integer> columnSource3;

    public ObjectBooleanIntegerColumnTupleSource(
            @NotNull final ColumnSource<Object> columnSource1,
            @NotNull final ColumnSource<Boolean> columnSource2,
            @NotNull final ColumnSource<Integer> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ObjectByteIntTuple createTuple(final long indexKey) {
        return new ObjectByteIntTuple(
                columnSource1.get(indexKey),
                BooleanUtils.booleanAsByte(columnSource2.getBoolean(indexKey)),
                columnSource3.getInt(indexKey)
        );
    }

    @Override
    public final ObjectByteIntTuple createPreviousTuple(final long indexKey) {
        return new ObjectByteIntTuple(
                columnSource1.getPrev(indexKey),
                BooleanUtils.booleanAsByte(columnSource2.getPrevBoolean(indexKey)),
                columnSource3.getPrevInt(indexKey)
        );
    }

    @Override
    public final ObjectByteIntTuple createTupleFromValues(@NotNull final Object... values) {
        return new ObjectByteIntTuple(
                values[0],
                BooleanUtils.booleanAsByte((Boolean)values[1]),
                TypeUtils.unbox((Integer)values[2])
        );
    }

    @Override
    public final ObjectByteIntTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ObjectByteIntTuple(
                values[0],
                BooleanUtils.booleanAsByte((Boolean)values[1]),
                TypeUtils.unbox((Integer)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ObjectByteIntTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) tuple.getFirstElement());
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
    public final Object exportToExternalKey(@NotNull final ObjectByteIntTuple tuple) {
        return new SmartKey(
                tuple.getFirstElement(),
                BooleanUtils.byteAsBoolean(tuple.getSecondElement()),
                TypeUtils.box(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final ObjectByteIntTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
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
    public final Object exportElementReinterpreted(@NotNull final ObjectByteIntTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
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
        WritableObjectChunk<ObjectByteIntTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<Object, Attributes.Values> chunk1 = chunks[0].asObjectChunk();
        ObjectChunk<Boolean, Attributes.Values> chunk2 = chunks[1].asObjectChunk();
        IntChunk<Attributes.Values> chunk3 = chunks[2].asIntChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ObjectByteIntTuple(chunk1.get(ii), BooleanUtils.booleanAsByte(chunk2.get(ii)), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ObjectBooleanIntegerColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ObjectByteIntTuple, Object, Boolean, Integer> {

        private Factory() {
        }

        @Override
        public TupleSource<ObjectByteIntTuple> create(
                @NotNull final ColumnSource<Object> columnSource1,
                @NotNull final ColumnSource<Boolean> columnSource2,
                @NotNull final ColumnSource<Integer> columnSource3
        ) {
            return new ObjectBooleanIntegerColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}

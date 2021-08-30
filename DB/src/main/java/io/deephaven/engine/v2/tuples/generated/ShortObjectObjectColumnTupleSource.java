package io.deephaven.engine.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.util.tuples.generated.ShortObjectObjectTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.structures.chunk.Attributes;
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
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Short, Object, and Object.
 * <p>Generated by {@link io.deephaven.engine.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ShortObjectObjectColumnTupleSource extends AbstractTupleSource<ShortObjectObjectTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ShortObjectObjectColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ShortObjectObjectTuple, Short, Object, Object> FACTORY = new Factory();

    private final ColumnSource<Short> columnSource1;
    private final ColumnSource<Object> columnSource2;
    private final ColumnSource<Object> columnSource3;

    public ShortObjectObjectColumnTupleSource(
            @NotNull final ColumnSource<Short> columnSource1,
            @NotNull final ColumnSource<Object> columnSource2,
            @NotNull final ColumnSource<Object> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ShortObjectObjectTuple createTuple(final long indexKey) {
        return new ShortObjectObjectTuple(
                columnSource1.getShort(indexKey),
                columnSource2.get(indexKey),
                columnSource3.get(indexKey)
        );
    }

    @Override
    public final ShortObjectObjectTuple createPreviousTuple(final long indexKey) {
        return new ShortObjectObjectTuple(
                columnSource1.getPrevShort(indexKey),
                columnSource2.getPrev(indexKey),
                columnSource3.getPrev(indexKey)
        );
    }

    @Override
    public final ShortObjectObjectTuple createTupleFromValues(@NotNull final Object... values) {
        return new ShortObjectObjectTuple(
                TypeUtils.unbox((Short)values[0]),
                values[1],
                values[2]
        );
    }

    @Override
    public final ShortObjectObjectTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ShortObjectObjectTuple(
                TypeUtils.unbox((Short)values[0]),
                values[1],
                values[2]
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ShortObjectObjectTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final ShortObjectObjectTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                tuple.getSecondElement(),
                tuple.getThirdElement()
        );
    }

    @Override
    public final Object exportElement(@NotNull final ShortObjectObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        if (elementIndex == 2) {
            return tuple.getThirdElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ShortObjectObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        if (elementIndex == 2) {
            return tuple.getThirdElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<ShortObjectObjectTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ShortChunk<Attributes.Values> chunk1 = chunks[0].asShortChunk();
        ObjectChunk<Object, Attributes.Values> chunk2 = chunks[1].asObjectChunk();
        ObjectChunk<Object, Attributes.Values> chunk3 = chunks[2].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ShortObjectObjectTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ShortObjectObjectColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ShortObjectObjectTuple, Short, Object, Object> {

        private Factory() {
        }

        @Override
        public TupleSource<ShortObjectObjectTuple> create(
                @NotNull final ColumnSource<Short> columnSource1,
                @NotNull final ColumnSource<Object> columnSource2,
                @NotNull final ColumnSource<Object> columnSource3
        ) {
            return new ShortObjectObjectColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}

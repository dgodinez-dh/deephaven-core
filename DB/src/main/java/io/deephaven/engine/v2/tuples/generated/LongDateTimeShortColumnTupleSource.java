package io.deephaven.engine.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.tables.utils.DBTimeUtils;
import io.deephaven.engine.util.tuples.generated.LongLongShortTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.structures.source.WritableSource;
import io.deephaven.engine.structures.chunk.Attributes;
import io.deephaven.engine.structures.chunk.Chunk;
import io.deephaven.engine.structures.chunk.LongChunk;
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
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Long, DBDateTime, and Short.
 * <p>Generated by {@link io.deephaven.engine.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class LongDateTimeShortColumnTupleSource extends AbstractTupleSource<LongLongShortTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link LongDateTimeShortColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<LongLongShortTuple, Long, DBDateTime, Short> FACTORY = new Factory();

    private final ColumnSource<Long> columnSource1;
    private final ColumnSource<DBDateTime> columnSource2;
    private final ColumnSource<Short> columnSource3;

    public LongDateTimeShortColumnTupleSource(
            @NotNull final ColumnSource<Long> columnSource1,
            @NotNull final ColumnSource<DBDateTime> columnSource2,
            @NotNull final ColumnSource<Short> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final LongLongShortTuple createTuple(final long indexKey) {
        return new LongLongShortTuple(
                columnSource1.getLong(indexKey),
                DBTimeUtils.nanos(columnSource2.get(indexKey)),
                columnSource3.getShort(indexKey)
        );
    }

    @Override
    public final LongLongShortTuple createPreviousTuple(final long indexKey) {
        return new LongLongShortTuple(
                columnSource1.getPrevLong(indexKey),
                DBTimeUtils.nanos(columnSource2.getPrev(indexKey)),
                columnSource3.getPrevShort(indexKey)
        );
    }

    @Override
    public final LongLongShortTuple createTupleFromValues(@NotNull final Object... values) {
        return new LongLongShortTuple(
                TypeUtils.unbox((Long)values[0]),
                DBTimeUtils.nanos((DBDateTime)values[1]),
                TypeUtils.unbox((Short)values[2])
        );
    }

    @Override
    public final LongLongShortTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new LongLongShortTuple(
                TypeUtils.unbox((Long)values[0]),
                DBTimeUtils.nanos((DBDateTime)values[1]),
                TypeUtils.unbox((Short)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final LongLongShortTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) DBTimeUtils.nanosToTime(tuple.getSecondElement()));
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationIndexKey, tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final LongLongShortTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                DBTimeUtils.nanosToTime(tuple.getSecondElement()),
                TypeUtils.box(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final LongLongShortTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return DBTimeUtils.nanosToTime(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final LongLongShortTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return DBTimeUtils.nanosToTime(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<LongLongShortTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        LongChunk<Attributes.Values> chunk1 = chunks[0].asLongChunk();
        ObjectChunk<DBDateTime, Attributes.Values> chunk2 = chunks[1].asObjectChunk();
        ShortChunk<Attributes.Values> chunk3 = chunks[2].asShortChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new LongLongShortTuple(chunk1.get(ii), DBTimeUtils.nanos(chunk2.get(ii)), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link LongDateTimeShortColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<LongLongShortTuple, Long, DBDateTime, Short> {

        private Factory() {
        }

        @Override
        public TupleSource<LongLongShortTuple> create(
                @NotNull final ColumnSource<Long> columnSource1,
                @NotNull final ColumnSource<DBDateTime> columnSource2,
                @NotNull final ColumnSource<Short> columnSource3
        ) {
            return new LongDateTimeShortColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}

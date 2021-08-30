package io.deephaven.engine.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.tables.utils.DBTimeUtils;
import io.deephaven.engine.util.tuples.generated.LongLongTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.structures.source.WritableSource;
import io.deephaven.engine.structures.chunk.Attributes;
import io.deephaven.engine.structures.chunk.Chunk;
import io.deephaven.engine.structures.chunk.ObjectChunk;
import io.deephaven.engine.structures.chunk.WritableChunk;
import io.deephaven.engine.structures.chunk.WritableObjectChunk;
import io.deephaven.engine.v2.tuples.AbstractTupleSource;
import io.deephaven.engine.structures.source.TupleSource;
import io.deephaven.engine.v2.tuples.TwoColumnTupleSourceFactory;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types DBDateTime and DBDateTime.
 * <p>Generated by {@link io.deephaven.engine.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class DateTimeDateTimeColumnTupleSource extends AbstractTupleSource<LongLongTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link DateTimeDateTimeColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<LongLongTuple, DBDateTime, DBDateTime> FACTORY = new Factory();

    private final ColumnSource<DBDateTime> columnSource1;
    private final ColumnSource<DBDateTime> columnSource2;

    public DateTimeDateTimeColumnTupleSource(
            @NotNull final ColumnSource<DBDateTime> columnSource1,
            @NotNull final ColumnSource<DBDateTime> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final LongLongTuple createTuple(final long indexKey) {
        return new LongLongTuple(
                DBTimeUtils.nanos(columnSource1.get(indexKey)),
                DBTimeUtils.nanos(columnSource2.get(indexKey))
        );
    }

    @Override
    public final LongLongTuple createPreviousTuple(final long indexKey) {
        return new LongLongTuple(
                DBTimeUtils.nanos(columnSource1.getPrev(indexKey)),
                DBTimeUtils.nanos(columnSource2.getPrev(indexKey))
        );
    }

    @Override
    public final LongLongTuple createTupleFromValues(@NotNull final Object... values) {
        return new LongLongTuple(
                DBTimeUtils.nanos((DBDateTime)values[0]),
                DBTimeUtils.nanos((DBDateTime)values[1])
        );
    }

    @Override
    public final LongLongTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new LongLongTuple(
                DBTimeUtils.nanos((DBDateTime)values[0]),
                DBTimeUtils.nanos((DBDateTime)values[1])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final LongLongTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) DBTimeUtils.nanosToTime(tuple.getFirstElement()));
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) DBTimeUtils.nanosToTime(tuple.getSecondElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final LongLongTuple tuple) {
        return new SmartKey(
                DBTimeUtils.nanosToTime(tuple.getFirstElement()),
                DBTimeUtils.nanosToTime(tuple.getSecondElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final LongLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return DBTimeUtils.nanosToTime(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return DBTimeUtils.nanosToTime(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final LongLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return DBTimeUtils.nanosToTime(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return DBTimeUtils.nanosToTime(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<LongLongTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<DBDateTime, Attributes.Values> chunk1 = chunks[0].asObjectChunk();
        ObjectChunk<DBDateTime, Attributes.Values> chunk2 = chunks[1].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new LongLongTuple(DBTimeUtils.nanos(chunk1.get(ii)), DBTimeUtils.nanos(chunk2.get(ii))));
        }
        destination.setSize(chunkSize);
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link DateTimeDateTimeColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<LongLongTuple, DBDateTime, DBDateTime> {

        private Factory() {
        }

        @Override
        public TupleSource<LongLongTuple> create(
                @NotNull final ColumnSource<DBDateTime> columnSource1,
                @NotNull final ColumnSource<DBDateTime> columnSource2
        ) {
            return new DateTimeDateTimeColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}

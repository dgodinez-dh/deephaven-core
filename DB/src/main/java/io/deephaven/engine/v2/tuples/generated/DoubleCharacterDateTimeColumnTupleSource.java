package io.deephaven.engine.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.tables.utils.DBTimeUtils;
import io.deephaven.engine.util.tuples.generated.DoubleCharLongTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.structures.source.WritableSource;
import io.deephaven.engine.structures.chunk.Attributes;
import io.deephaven.engine.structures.chunk.CharChunk;
import io.deephaven.engine.structures.chunk.Chunk;
import io.deephaven.engine.structures.chunk.DoubleChunk;
import io.deephaven.engine.structures.chunk.ObjectChunk;
import io.deephaven.engine.structures.chunk.WritableChunk;
import io.deephaven.engine.structures.chunk.WritableObjectChunk;
import io.deephaven.engine.v2.tuples.AbstractTupleSource;
import io.deephaven.engine.v2.tuples.ThreeColumnTupleSourceFactory;
import io.deephaven.engine.structures.source.TupleSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Double, Character, and DBDateTime.
 * <p>Generated by {@link io.deephaven.engine.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class DoubleCharacterDateTimeColumnTupleSource extends AbstractTupleSource<DoubleCharLongTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link DoubleCharacterDateTimeColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<DoubleCharLongTuple, Double, Character, DBDateTime> FACTORY = new Factory();

    private final ColumnSource<Double> columnSource1;
    private final ColumnSource<Character> columnSource2;
    private final ColumnSource<DBDateTime> columnSource3;

    public DoubleCharacterDateTimeColumnTupleSource(
            @NotNull final ColumnSource<Double> columnSource1,
            @NotNull final ColumnSource<Character> columnSource2,
            @NotNull final ColumnSource<DBDateTime> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final DoubleCharLongTuple createTuple(final long indexKey) {
        return new DoubleCharLongTuple(
                columnSource1.getDouble(indexKey),
                columnSource2.getChar(indexKey),
                DBTimeUtils.nanos(columnSource3.get(indexKey))
        );
    }

    @Override
    public final DoubleCharLongTuple createPreviousTuple(final long indexKey) {
        return new DoubleCharLongTuple(
                columnSource1.getPrevDouble(indexKey),
                columnSource2.getPrevChar(indexKey),
                DBTimeUtils.nanos(columnSource3.getPrev(indexKey))
        );
    }

    @Override
    public final DoubleCharLongTuple createTupleFromValues(@NotNull final Object... values) {
        return new DoubleCharLongTuple(
                TypeUtils.unbox((Double)values[0]),
                TypeUtils.unbox((Character)values[1]),
                DBTimeUtils.nanos((DBDateTime)values[2])
        );
    }

    @Override
    public final DoubleCharLongTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new DoubleCharLongTuple(
                TypeUtils.unbox((Double)values[0]),
                TypeUtils.unbox((Character)values[1]),
                DBTimeUtils.nanos((DBDateTime)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final DoubleCharLongTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) DBTimeUtils.nanosToTime(tuple.getThirdElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final DoubleCharLongTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                TypeUtils.box(tuple.getSecondElement()),
                DBTimeUtils.nanosToTime(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final DoubleCharLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return DBTimeUtils.nanosToTime(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final DoubleCharLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return DBTimeUtils.nanosToTime(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<DoubleCharLongTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        DoubleChunk<Attributes.Values> chunk1 = chunks[0].asDoubleChunk();
        CharChunk<Attributes.Values> chunk2 = chunks[1].asCharChunk();
        ObjectChunk<DBDateTime, Attributes.Values> chunk3 = chunks[2].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new DoubleCharLongTuple(chunk1.get(ii), chunk2.get(ii), DBTimeUtils.nanos(chunk3.get(ii))));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link DoubleCharacterDateTimeColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<DoubleCharLongTuple, Double, Character, DBDateTime> {

        private Factory() {
        }

        @Override
        public TupleSource<DoubleCharLongTuple> create(
                @NotNull final ColumnSource<Double> columnSource1,
                @NotNull final ColumnSource<Character> columnSource2,
                @NotNull final ColumnSource<DBDateTime> columnSource3
        ) {
            return new DoubleCharacterDateTimeColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}

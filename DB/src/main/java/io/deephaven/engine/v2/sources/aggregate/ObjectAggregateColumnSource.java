package io.deephaven.engine.v2.sources.aggregate;

import io.deephaven.engine.structures.vector.DbArray;
import io.deephaven.engine.v2.dbarrays.DbArrayColumnWrapper;
import io.deephaven.engine.v2.dbarrays.DbPrevArrayColumnWrapper;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.structures.chunk.Attributes.Values;
import io.deephaven.engine.structures.chunk.ObjectChunk;
import io.deephaven.engine.structures.chunk.WritableChunk;
import io.deephaven.engine.structures.chunk.WritableObjectChunk;
import io.deephaven.engine.structures.rowset.Index;
import io.deephaven.engine.structures.rowsequence.OrderedKeys;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnSource} implementation for aggregation result Object columns.
 */
public final class ObjectAggregateColumnSource<COMPONENT_TYPE> extends BaseAggregateColumnSource<DbArray, COMPONENT_TYPE> {

    ObjectAggregateColumnSource(@NotNull final ColumnSource<COMPONENT_TYPE> aggregatedSource,
                                @NotNull final ColumnSource<Index> indexSource) {
        super(DbArray.class, aggregatedSource, indexSource);
    }

    @Override
    public final DbArray<COMPONENT_TYPE> get(final long index) {
        if (index == Index.NULL_KEY) {
            return null;
        }
        return new DbArrayColumnWrapper<>(aggregatedSource, indexSource.get(index));
    }

    @Override
    public final DbArray<COMPONENT_TYPE> getPrev(final long index) {
        if (index == Index.NULL_KEY) {
            return null;
        }
        return new DbPrevArrayColumnWrapper<>(aggregatedSource, indexSource.getPrev(index));
    }

    @Override
    public final void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final ObjectChunk<Index, ? extends Values> indexChunk = indexSource.getChunk(((AggregateFillContext) context).indexGetContext, orderedKeys).asObjectChunk();
        final WritableObjectChunk<DbArray<COMPONENT_TYPE>, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = orderedKeys.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new DbArrayColumnWrapper<>(aggregatedSource, indexChunk.get(di)));
        }
        typedDestination.setSize(size);
    }

    @Override
    public final void fillPrevChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final ObjectChunk<Index, ? extends Values> indexChunk = indexSource.getPrevChunk(((AggregateFillContext) context).indexGetContext, orderedKeys).asObjectChunk();
        final WritableObjectChunk<DbArray<COMPONENT_TYPE>, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = orderedKeys.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new DbPrevArrayColumnWrapper<>(aggregatedSource, indexChunk.get(di)));
        }
        typedDestination.setSize(size);
    }
}

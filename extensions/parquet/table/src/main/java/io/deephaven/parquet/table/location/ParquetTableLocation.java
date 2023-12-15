/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.location;

import io.deephaven.api.SortColumn;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.dataindex.LocationDataIndex;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedPageStore;
import io.deephaven.parquet.base.ColumnChunkReader;
import io.deephaven.parquet.base.ParquetFileReader;
import io.deephaven.parquet.base.RowGroupReader;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetSchemaReader;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.parquet.table.metadata.ColumnTypeInfo;
import io.deephaven.parquet.table.metadata.DataIndexInfo;
import io.deephaven.parquet.table.metadata.GroupingColumnInfo;
import io.deephaven.parquet.table.metadata.TableInfo;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.IntStream;

import static io.deephaven.parquet.table.ParquetTableWriter.INDEX_COL_NAME;

public class ParquetTableLocation extends AbstractTableLocation {

    private static final String IMPLEMENTATION_NAME = ParquetColumnLocation.class.getSimpleName();

    private final ParquetInstructions readInstructions;
    private final List<SortColumn> sortingColumns;
    private final ParquetFileReader parquetFileReader;
    private final int[] rowGroupIndices;

    private final RowGroup[] rowGroups;
    private final RegionedPageStore.Parameters regionParameters;
    private final Map<String, String[]> parquetColumnNameToPath;
    private final Map<String, GroupingColumnInfo> groupingColumns;
    private final List<DataIndexInfo> dataIndexes;
    private final Map<String, ColumnTypeInfo> columnTypes;
    private final TableInfo tableInfo;

    private final String version;

    private volatile RowGroupReader[] rowGroupReaders;

    public ParquetTableLocation(@NotNull final TableKey tableKey,
            @NotNull final ParquetTableLocationKey tableLocationKey,
            @NotNull final ParquetInstructions readInstructions) {
        super(tableKey, tableLocationKey, false);
        this.readInstructions = readInstructions;
        final ParquetMetadata parquetMetadata;
        // noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (tableLocationKey) {
            parquetFileReader = tableLocationKey.getFileReader();
            parquetMetadata = tableLocationKey.getMetadata();
            rowGroupIndices = tableLocationKey.getRowGroupIndices();
        }

        final int rowGroupCount = rowGroupIndices.length;
        rowGroups = IntStream.of(rowGroupIndices)
                .mapToObj(rgi -> parquetFileReader.fileMetaData.getRow_groups().get(rgi))
                .sorted(Comparator.comparingInt(RowGroup::getOrdinal))
                .toArray(RowGroup[]::new);
        final long maxRowCount = Arrays.stream(rowGroups).mapToLong(RowGroup::getNum_rows).max().orElse(0L);
        regionParameters = new RegionedPageStore.Parameters(
                RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, rowGroupCount, maxRowCount);

        parquetColumnNameToPath = new HashMap<>();
        for (final ColumnDescriptor column : parquetFileReader.getSchema().getColumns()) {
            final String[] path = column.getPath();
            if (path.length > 1) {
                parquetColumnNameToPath.put(path[0], path);
            }
        }

        // TODO (https://github.com/deephaven/deephaven-core/issues/958):
        // When/if we support _metadata files for Deephaven-written Parquet tables, we may need to revise this
        // in order to read *this* file's metadata, rather than inheriting file metadata from the _metadata file.
        // Obvious issues included grouping table paths, codecs, etc.
        // Presumably, we could store per-file instances of the metadata in the _metadata file's map.
        tableInfo =
                ParquetSchemaReader.parseMetadata(parquetMetadata.getFileMetaData().getKeyValueMetaData())
                        .orElse(TableInfo.builder().build());
        groupingColumns = tableInfo.groupingColumnMap();
        columnTypes = tableInfo.columnTypeMap();
        version = tableInfo.version();
        dataIndexes = tableInfo.dataIndexes();
        sortingColumns = tableInfo.sortingColumns();

        handleUpdate(computeIndex(), tableLocationKey.getFile().lastModified());
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public void refresh() {}

    File getParquetFile() {
        return ((ParquetTableLocationKey) getKey()).getFile();
    }

    ParquetInstructions getReadInstructions() {
        return readInstructions;
    }

    SeekableChannelsProvider getChannelProvider() {
        return parquetFileReader.getChannelsProvider();
    }

    RegionedPageStore.Parameters getRegionParameters() {
        return regionParameters;
    }

    public Map<String, GroupingColumnInfo> getGroupingColumns() {
        return groupingColumns;
    }

    public Map<String, ColumnTypeInfo> getColumnTypes() {
        return columnTypes;
    }

    private RowGroupReader[] getRowGroupReaders() {
        RowGroupReader[] local;
        if ((local = rowGroupReaders) != null) {
            return local;
        }
        synchronized (this) {
            if ((local = rowGroupReaders) != null) {
                return local;
            }
            return rowGroupReaders = IntStream.of(rowGroupIndices)
                    .mapToObj(idx -> parquetFileReader.getRowGroup(idx, version))
                    .sorted(Comparator.comparingInt(rgr -> rgr.getRowGroup().getOrdinal()))
                    .toArray(RowGroupReader[]::new);
        }
    }

    @NotNull
    @Override
    public List<SortColumn> getSortedColumns() {
        return sortingColumns;
    }

    @NotNull
    @Override
    protected ParquetColumnLocation<Values> makeColumnLocation(@NotNull final String columnName) {
        final String parquetColumnName = readInstructions.getParquetColumnNameFromColumnNameOrDefault(columnName);
        final String[] columnPath = parquetColumnNameToPath.get(parquetColumnName);
        final List<String> nameList =
                columnPath == null ? Collections.singletonList(parquetColumnName) : Arrays.asList(columnPath);
        final ColumnChunkReader[] columnChunkReaders = Arrays.stream(getRowGroupReaders())
                .map(rgr -> rgr.getColumnChunk(nameList)).toArray(ColumnChunkReader[]::new);
        final boolean exists = Arrays.stream(columnChunkReaders).anyMatch(ccr -> ccr != null && ccr.numRows() > 0);
        return new ParquetColumnLocation<>(this, columnName, parquetColumnName,
                exists ? columnChunkReaders : null,
                exists && groupingColumns.containsKey(parquetColumnName));
    }

    private RowSet computeIndex() {
        final RowSetBuilderSequential sequentialBuilder = RowSetFactory.builderSequential();

        for (int rgi = 0; rgi < rowGroups.length; ++rgi) {
            final long subRegionSize = rowGroups[rgi].getNum_rows();
            final long subRegionFirstKey = (long) rgi << regionParameters.regionMaskNumBits;
            final long subRegionLastKey = subRegionFirstKey + subRegionSize - 1;
            sequentialBuilder.appendRange(subRegionFirstKey, subRegionLastKey);
        }
        return sequentialBuilder.build();
    }

    @Override
    @NotNull
    public List<String[]> getDataIndexColumns() {
        List<String[]> dataIndexColumns = new ArrayList<>();
        // Add the data indexes to the list.
        dataIndexes.stream().map(di -> di.columns().toArray(String[]::new)).forEach(dataIndexColumns::add);
        // Add grouping columns to the list.
        groupingColumns.keySet().stream().map(colName -> new String[] {colName}).forEach(dataIndexColumns::add);
        return dataIndexColumns;
    }

    @Override
    public boolean hasDataIndex(@NotNull final String... columns) {
        // Check if the column name matches any of the grouping columns
        if (columns.length == 1 && groupingColumns.containsKey(columns[0])) {
            // Validate the index file exists (without loading and parsing it).
            ParquetTools.IndexFileMetaData metaData = ParquetTools.getIndexFileMetaData(
                    getParquetFile(),
                    tableInfo,
                    columns);
            return metaData != null && Files.exists(Path.of(metaData.filename));
        }
        // Check if the column names match any of the data indexes
        for (final DataIndexInfo dataIndex : dataIndexes) {
            if (dataIndex.matchesColumns(columns)) {
                // Validate the index file exists (without loading and parsing it).
                ParquetTools.IndexFileMetaData metaData = ParquetTools.getIndexFileMetaData(
                        getParquetFile(),
                        tableInfo,
                        columns);
                return metaData != null && Files.exists(Path.of(metaData.filename));
            }
        }
        return false;
    }

    @Nullable
    @Override
    public BasicDataIndex loadDataIndex(@NotNull final String... columns) {
        // Create a new index from the parquet table.
        final Table table = ParquetTools.readDataIndexTable(getParquetFile(), tableInfo, columns);
        if (table == null) {
            return null;
        }
        return LocationDataIndex.from(table, columns, INDEX_COL_NAME);
    }
}

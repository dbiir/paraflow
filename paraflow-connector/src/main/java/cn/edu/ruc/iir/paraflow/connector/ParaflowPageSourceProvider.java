/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.ruc.iir.paraflow.connector;

import cn.edu.ruc.iir.paraflow.connector.exception.ParaflowSplitNotOpenException;
import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowColumnHandle;
import com.facebook.presto.hive.parquet.HdfsParquetDataSource;
import com.facebook.presto.hive.parquet.ParquetDataSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static cn.edu.ruc.iir.paraflow.connector.Types.checkType;
import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class ParaflowPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TypeManager typeManager;
    private final FSFactory fsFactory;
    private Logger log = Logger.get(ParaflowPageSourceProvider.class.getName());

    @Inject
    public ParaflowPageSourceProvider(TypeManager typeManager, FSFactory fsFactory)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fsFactory = requireNonNull(fsFactory, "fsFactory is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                                ConnectorSplit split, List<ColumnHandle> columns)
    {
        List<ParaflowColumnHandle> hdfsColumns = columns.stream()
                .map(col -> (ParaflowColumnHandle) col)
                .collect(Collectors.toList());
        ParaflowSplit paraflowSplit = checkType(split, ParaflowSplit.class, "hdfs split");
        Path path = new Path(paraflowSplit.getPath());

        Optional<ConnectorPageSource> pageSource = createParaflowPageSource(
                path,
                paraflowSplit.getStart(),
                paraflowSplit.getLen(),
                hdfsColumns);
        if (pageSource.isPresent()) {
            return pageSource.get();
        }
        throw new RuntimeException("Could not find a file reader for split " + paraflowSplit);
    }

    private Optional<ConnectorPageSource> createParaflowPageSource(
            Path path,
            long start,
            long length,
            List<ParaflowColumnHandle> columns)
    {
        Optional<FileSystem> fileSystemOptional = fsFactory.getFileSystem();
        FileSystem fileSystem;
        ParquetDataSource dataSource;
        if (fileSystemOptional.isPresent()) {
            fileSystem = fileSystemOptional.get();
        }
        else {
            throw new RuntimeException("Could not find filesystem for path " + path);
        }
        try {
            dataSource = buildHdfsParquetDataSource(fileSystem, path, start, length);
            // default length is file size, which means whole file is a split
            length = dataSource.getSize();
            ParquetMetadata parquetMetadata = ParquetMetadataReader.readFooter(fileSystem, path);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            List<Type> fields = columns.stream()
                    .filter(column -> column.getColType() != ParaflowColumnHandle.ColumnType.NOTVALID)
                    .map(column -> getParquetType(column, fileSchema))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            MessageType requestedSchema = new MessageType(fileSchema.getName(), fields);

            List<BlockMetaData> blocks = new ArrayList<>();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= start && firstDataPage < start + length) {
                    blocks.add(block);
                }
            }

            ParquetReader parquetReader = new ParquetReader(
                    fileSchema,
                    requestedSchema,
                    blocks,
                    dataSource,
                    typeManager);
            return Optional.of(new ParaflowPageSource(
                    parquetReader,
                    dataSource,
                    fileSchema,
                    requestedSchema,
                    length,
                    columns,
                    typeManager));
        }
        catch (IOException e) {
            log.error(e);
            return Optional.empty();
        }
    }

    private HdfsParquetDataSource buildHdfsParquetDataSource(FileSystem fileSystem, Path path, long start, long length)
    {
        try {
            long size = fileSystem.getFileStatus(path).getLen();
            FSDataInputStream inputStream = fileSystem.open(path);
            return new HdfsParquetDataSource(path, size, inputStream);
        }
        catch (IOException e) {
            throw new ParaflowSplitNotOpenException(path);
        }
    }

    private Type getParquetType(ParaflowColumnHandle column, MessageType messageType)
    {
        if (messageType.containsField(column.getName())) {
            return messageType.getType(column.getName());
        }
        // parquet is case-insensitive, all hdfs-columns get converted to lowercase
        for (Type type : messageType.getFields()) {
            if (type.getName().equalsIgnoreCase(column.getName())) {
                return type;
            }
        }
        return null;
    }
}

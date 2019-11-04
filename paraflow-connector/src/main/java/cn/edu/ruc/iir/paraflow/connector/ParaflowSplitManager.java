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

import cn.edu.ruc.iir.paraflow.commons.ParaflowFiberPartitioner;
import cn.edu.ruc.iir.paraflow.commons.utils.BytesUtils;
import cn.edu.ruc.iir.paraflow.connector.exception.TableNotFoundException;
import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowTableHandle;
import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowTableLayoutHandle;
import cn.edu.ruc.iir.paraflow.connector.impl.ParaflowMetaDataReader;
import cn.edu.ruc.iir.paraflow.connector.impl.ParaflowPrestoConfig;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static cn.edu.ruc.iir.paraflow.connector.Types.checkType;
import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class ParaflowSplitManager
        implements ConnectorSplitManager
{
    private static final Logger logger = Logger.get(ParaflowSplitManager.class.getName());
    private final ParaflowConnectorId connectorId;
    private final ParaflowMetaDataReader metaDataQuery;
    private final FSFactory fsFactory;
    private final boolean fiberEnabled;

    @Inject
    public ParaflowSplitManager(
            ParaflowConnectorId connectorId,
            ParaflowMetaDataReader metaDataQuer,
            FSFactory fsFactory, ParaflowPrestoConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.metaDataQuery = requireNonNull(metaDataQuer, "metaServer is null");
        this.fsFactory = requireNonNull(fsFactory, "fsFactory is null");
        this.fiberEnabled = config.getFiberEnable();
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session,
                                          ConnectorTableLayoutHandle layoutHandle,
                                          SplitSchedulingStrategy splitSchedulingStrategy)
    {
        ParaflowTableLayoutHandle layout = checkType(layoutHandle, ParaflowTableLayoutHandle.class, "layoutHandle");
        Optional<ParaflowTableHandle> tableHandle =
                metaDataQuery.getTableHandle(connectorId.getConnectorId(),
                        layout.getSchemaTableName().getSchemaName(),
                        layout.getSchemaTableName().getTableName());
        if (!tableHandle.isPresent()) {
            throw new TableNotFoundException(layout.getSchemaTableName().toString());
        }

        String tablePath = tableHandle.get().getPath();
        String dbName = tableHandle.get().getSchemaName();
        String tblName = tableHandle.get().getTableName();
        String partitionerName = layout.getFiberPartitioner();
        Optional<TupleDomain<ColumnHandle>> predicatesOptional = layout.getPredicates();

        List<ConnectorSplit> splits = new ArrayList<>();
        List<Path> files;

        if (this.fiberEnabled) {
            logger.info("Step One: go into fiber config");
            if (predicatesOptional.isPresent()) {
                TupleDomain<ColumnHandle> predicates = predicatesOptional.get();
                ColumnHandle fiberCol = layout.getFiberColumn();
                ColumnHandle timeCol = layout.getTimestampColumn();
                Optional<Map<ColumnHandle, Domain>> domains = predicates.getDomains();
                if (!domains.isPresent()) {
                    files = fsFactory.listFiles(new Path(tablePath));
                }
                else {
                    int fiber = -1;
                    int fiberId = -1;
                    long timeLow = -1L;
                    long timeHigh = -1L;
                    if (domains.get().containsKey(fiberCol)) {
                        // parse fiber domains
                        Domain fiberDomain = domains.get().get(fiberCol);
                        ValueSet fiberValueSet = fiberDomain.getValues();
                        if (fiberValueSet instanceof SortedRangeSet) {
                            if (fiberValueSet.isSingleValue()) {
                                Object valueObj = fiberValueSet.getSingleValue();
                                if (valueObj instanceof Integer) {
                                    fiber = (int) valueObj;
                                }
                                if (valueObj instanceof Long) {
                                    fiber = ((Long) valueObj).intValue();
                                }
                                if (valueObj instanceof Slice) {
                                    Slice fiberSlice = (Slice) fiberValueSet.getSingleValue();
                                    fiber = Integer.parseInt(fiberSlice.toStringUtf8());
                                }
                                ParaflowFiberPartitioner partitioner = parsePartitioner(partitionerName);
                                if (partitioner != null) {
                                    fiberId = partitioner.getFiberId(BytesUtils.toBytes(fiber)); // mod fiberNum
                                }
                            }
                        }
                    }
                    if (domains.get().containsKey(timeCol)) {
                        // parse time domains
                        Domain timeDomain = domains.get().get(timeCol);
                        ValueSet timeValueSet = timeDomain.getValues();
                        if (timeValueSet instanceof SortedRangeSet) {
                            // todo list all ranges
                            List<Range> ranges = ((SortedRangeSet) timeValueSet).getOrderedRanges();
                            Range range = ranges.get(0);
                            logger.info("[Step 0] range:" + range.toString(session));
                            for (int i = 1; i < ranges.size(); i++) {
                                range = range.span(ranges.get(i));
                            }
                            logger.info("[Step 1] range:" + range.toString(session));
                            Marker low = range.getLow();
                            Marker high = range.getHigh();
                            logger.info("[Step 0] range:" + range.toString(session));
                            if (!low.isLowerUnbounded()) {
                                timeLow = (Long) low.getValue();
                            }
                            if (!high.isUpperUnbounded()) {
                                timeHigh = (Long) high.getValue();
                            }
                        }
                    }
                    logger.info("[Param] fiber:" + fiber + " ,timeLow:" + timeLow + " ,timeHigh:" + timeHigh);
//                if (fiber == -1 && timeLow == -1L && timeHigh == -1L) {
//                    files = fsFactory.listFiles(new Path(tablePath));
//                }
//                else {
                    files = metaDataQuery.filterBlocks(
                            dbName,
                            tblName,
                            fiberId,
                            timeLow,
                            timeHigh)
                            .stream().map(Path::new).collect(Collectors.toList());         // filter file paths with fiber domains and time domains using meta server
//                }
                }
            }
            else {
                files = fsFactory.listFiles(new Path(tablePath));
            }

            logger.info("[File_0 Num Begin]:" + files.size());
            int count = 0;
            for (int i = files.size() - 1; i >= 0; i--) {
                Path item = files.get(i);
                if (item.toString().contains("shm")) {
                    files.remove(item);
                    count++;
                }
            }
            logger.info("[File_0 Num] Exclude:" + count);
            logger.info("[File_0 Num] End:" + files.size());

            files.forEach(file -> splits.add(new ParaflowSplit(connectorId,
                    tableHandle.get().getSchemaTableName(),
                    file.toString(), 0, -1,
                    fsFactory.getBlockLocations(file, 0, Long.MAX_VALUE))));
        }
        else {
            logger.info("Step Two: go into no fiber config");
            files = fsFactory.listFiles(new Path(tablePath));
            logger.info("[File_1 Num Begin]:" + files.size());
            final int[] count = {0};
            files.forEach(file -> {
                List<HostAddress> list = fsFactory.getBlockLocations(file, 0, Long.MAX_VALUE);
                if (list.size() > 0) {
                    splits.add(new ParaflowSplit(connectorId,
                            tableHandle.get().getSchemaTableName(),
                            file.toString(), 0, -1, list));
                }
                else {
                    count[0]++;
                }
            });
            logger.info("[File_1 Num] Exclude:" + count[0]);
            logger.info("[File_1 Num] End:" + files.size());
        }

//        splits.forEach(split -> logger.info(split.toString()));
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }

    private ParaflowFiberPartitioner parsePartitioner(String partitionerName)
    {
//        Class clazz = Class.forName(partitionerName);
//        Constructor c = clazz.getConstructor(int.class);
//        ParaflowFiberPartitioner partitioner = c.newInstance(partitionNum);
        try {
            Class clazz = ParaflowMetaDataReader.class.getClassLoader().loadClass(partitionerName);
            return (ParaflowFiberPartitioner) clazz.newInstance();
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
        return null;
    }
}

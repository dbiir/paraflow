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

import cn.edu.ruc.iir.paraflow.connector.impl.ParaflowPrestoConfig;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static cn.edu.ruc.iir.paraflow.connector.exception.ParaflowErrorCode.PARAFLOW_HDFS_FILE_ERROR;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public final class FSFactory
{
    private final Logger log = Logger.get(FSFactory.class.getName());
    private final Configuration config = new Configuration();
    private FileSystem fileSystem = null;

    @Inject
    public FSFactory(ParaflowPrestoConfig prestoConfig)
    {
        config.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        config.set("fs.file.impl", LocalFileSystem.class.getName());
        try {
            this.fileSystem = FileSystem.get(new URI(prestoConfig.getHDFSWarehouse()), config);
        }
        catch (IOException | URISyntaxException e) {
            this.fileSystem = null;
        }
    }

    public Optional<FileSystem> getFileSystem()
    {
        return fileSystem == null ? Optional.empty() : Optional.of(fileSystem);
    }

    public List<Path> listFiles(Path dirPath)
    {
        List<Path> files = new ArrayList<>();
        FileStatus[] fileStatuses;
        if (this.fileSystem == null) {
            return ImmutableList.of();
        }
        try {
            fileStatuses = this.fileSystem.listStatus(dirPath);
            if (fileStatuses != null) {
                for (FileStatus f : fileStatuses) {
                    //avoid add empty file
                    if (f.isFile() && f.getLen() > 0) {
                        files.add(f.getPath());
                    }
                }
            }
        }
        catch (IOException e) {
            log.error(e);
            throw new PrestoException(PARAFLOW_HDFS_FILE_ERROR, e);
        }

        return files;
    }

    // assume that a file contains only a block
    public List<HostAddress> getBlockLocations(Path file, long start, long len)
    {
        Set<HostAddress> addresses = new HashSet<>();
        BlockLocation[] locations = new BlockLocation[0];
        try {
            locations = this.fileSystem.getFileBlockLocations(file, start, len);
        }
        catch (IOException e) {
            log.error(e);
        }
        assert locations.length <= 1;
        for (BlockLocation location : locations) {
            try {
                addresses.addAll(toHostAddress(location.getHosts()));
            }
            catch (IOException e) {
                log.error(e);
            }
        }
        return new ArrayList<>(addresses);
    }

    private List<HostAddress> toHostAddress(String[] hosts)
    {
        ImmutableList.Builder<HostAddress> builder = ImmutableList.builder();
        for (String host : hosts) {
            builder.add(HostAddress.fromString(host));
        }
        return builder.build();
    }
}

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

import cn.edu.ruc.iir.paraflow.commons.utils.ConfigFactory;
import cn.edu.ruc.iir.paraflow.connector.exception.ConfigurationException;
import cn.edu.ruc.iir.paraflow.connector.impl.ParaflowPrestoConfig;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static cn.edu.ruc.iir.paraflow.connector.exception.ParaflowErrorCode.PARAFLOW_CONFIG_ERROR;
import static cn.edu.ruc.iir.paraflow.connector.exception.ParaflowErrorCode.PARAFLOW_HDFS_FILE_ERROR;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public final class FSFactory
{
    private Configuration conf = new Configuration();
    private FileSystem fileSystem;
    private final ParaflowPrestoConfig config;
    private final Logger log = Logger.get(FSFactory.class.getName());

    @Inject
    public FSFactory(ParaflowPrestoConfig config)
    {
        this.config = config;
        Configuration hdfsConfig = new Configuration(false);
        ConfigFactory configFactory = config.getFactory();
        File hdfsConfigDir = new File(configFactory.getProperty("hdfs.config.dir"));
        hdfsConfig.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        hdfsConfig.set("fs.file.impl", LocalFileSystem.class.getName());
        try
        {
            if (hdfsConfigDir.exists() && hdfsConfigDir.isDirectory())
            {
                File[] hdfsConfigFiles = hdfsConfigDir.listFiles((file, s) -> s.endsWith("core-site.xml") || s.endsWith("hdfs-site.xml"));
                if (hdfsConfigFiles != null && hdfsConfigFiles.length == 2)
                {
                    hdfsConfig.addResource(hdfsConfigFiles[0].toURI().toURL());
                    hdfsConfig.addResource(hdfsConfigFiles[1].toURI().toURL());
                }
            } else
            {
                log.error("can not read hdfs configuration file in paraflow connector. hdfs.config.dir=" + hdfsConfigDir);
                throw new PrestoException(PARAFLOW_HDFS_FILE_ERROR, new ConfigurationException());
            }
            fileSystem = FileSystem.get(hdfsConfig);
        } catch (IOException e)
        {
            log.error(e);
            throw new PrestoException(PARAFLOW_CONFIG_ERROR, e);
        }
    }

    public Optional<FileSystem> getFileSystem()
    {
        return Optional.of(this.fileSystem);
    }

    public List<Path> listFiles(Path dirPath)
    {
        List<Path> files = new ArrayList<>();
        FileStatus[] fileStatuses = null;
        try
        {
            fileStatuses = this.fileSystem.listStatus(dirPath);
            if (fileStatuses != null)
            {
                for (FileStatus f : fileStatuses)
                {
                    if (f.isFile())
                    {
                        files.add(f.getPath());
                    }
                }
            }
        } catch (IOException e)
        {
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

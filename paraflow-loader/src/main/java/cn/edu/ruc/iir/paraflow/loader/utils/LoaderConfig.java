package cn.edu.ruc.iir.paraflow.loader.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.utils.ParaFlowConfig;

import java.util.Properties;

public class LoaderConfig
{
    private ParaFlowConfig paraflowConfig;

    private LoaderConfig()
    {
    }

    public static final LoaderConfig INSTANCE()
    {
        return MetaConfigHolder.instance;
    }

    public void init() throws ConfigFileNotFoundException
    {
        paraflowConfig = new ParaFlowConfig("loader.conf");
        paraflowConfig.build();
    }

    public Properties getProperties()
    {
        return paraflowConfig.getProperties();
    }

    public boolean contains(String key)
    {
        return paraflowConfig.getProperties().containsKey(key);
    }

    public String getLoaderId()
    {
        return paraflowConfig.getProperty("loader.id");
    }

    public int getLoaderParallelism()
    {
        if (paraflowConfig.getProperty("loader.parallelism") == null) {
            return -1;
        }
        return Integer.parseInt(paraflowConfig.getProperty("loader.parallelism"));
    }

    public long getLoaderLifetime()
    {
        return Long.parseLong(paraflowConfig.getProperty("loader.lifetime"));
    }

    public int getPullerParallelism()
    {
        return Integer.parseInt(paraflowConfig.getProperty("puller.parallelism"));
    }

    public int getSorterCompactorCapacity()
    {
        return Integer.parseInt(paraflowConfig.getProperty("sorterCompactor.capacity"));
    }

    public int getSortedBufferCapacity()
    {
        return Integer.parseInt(paraflowConfig.getProperty("sortedBuffer.capacity"));
    }

    public int getContainerCapacity()
    {
        return Integer.parseInt(paraflowConfig.getProperty("container.capacity"));
    }

    public int getFlushingCapacity()
    {
        return Integer.parseInt(paraflowConfig.getProperty("flushing.capacity"));
    }

    public int getCompactorThreshold()
    {
        return Integer.parseInt(paraflowConfig.getProperty("compactor.threshold"));
    }

    public String getTransformerClass()
    {
        return paraflowConfig.getProperty("transformer.class");
    }

    public int getMetaServerPort()
    {
        return Integer.parseInt(paraflowConfig.getProperty("meta.server.port"));
    }

    public String getMetaServerHost()
    {
        return paraflowConfig.getProperty("meta.server.host");
    }

    public String getMemoryWarehouse()
    {
        if (paraflowConfig.getProperty("memory.warehouse").endsWith("/")) {
            return paraflowConfig.getProperty("memory.warehouse");
        }
        else {
            return paraflowConfig.getProperty("memory.warehouse") + "/";
        }
    }

    public String getHDFSWarehouse()
    {
        if (paraflowConfig.getProperty("hdfs.warehouse").endsWith("/")) {
            return paraflowConfig.getProperty("hdfs.warehouse");
        }
        else {
            return paraflowConfig.getProperty("hdfs.warehouse") + "/";
        }
    }

    public long getOrcFileStripeSize()
    {
        return Long.parseLong(paraflowConfig.getProperty("orc.file.stripe.size"));
    }

    public int getOrcFileBufferSize()
    {
        return Integer.parseInt(paraflowConfig.getProperty("orc.file.buffer.size"));
    }

    public long getOrcFileBlockSize()
    {
        return Long.parseLong(paraflowConfig.getProperty("orc.file.block.size"));
    }

    public String getParquetCompressionCodec()
    {
        return paraflowConfig.getProperty("parquet.compression.codec");
    }

    public int getParquetBlockSize()
    {
        return Integer.parseInt(paraflowConfig.getProperty("parquet.block.size"));
    }

    public int getParquetPageSize()
    {
        return Integer.parseInt(paraflowConfig.getProperty("parquet.page.size"));
    }

    public int getParquetDictionaryPageSize()
    {
        return Integer.parseInt(paraflowConfig.getProperty("parquet.dictionary.page.size"));
    }

    public boolean isParquetDictionaryEnabled()
    {
        return paraflowConfig.getProperty("parquet.dictionary.enable").equalsIgnoreCase("true");
    }

    public boolean isParquetValidating()
    {
        return paraflowConfig.getProperty("parquet.validating").equalsIgnoreCase("true");
    }

    public boolean isMetricEnabled()
    {
        return paraflowConfig.getProperty("metric.enabled").equalsIgnoreCase("true");
    }

    public String getGateWayUrl()
    {
        return paraflowConfig.getProperty("gateway.url");
    }

    private static class MetaConfigHolder
    {
        private static final LoaderConfig instance = new LoaderConfig();
    }
}

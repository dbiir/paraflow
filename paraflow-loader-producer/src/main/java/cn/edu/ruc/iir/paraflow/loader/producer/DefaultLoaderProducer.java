package cn.edu.ruc.iir.paraflow.loader.producer;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import cn.edu.ruc.iir.paraflow.loader.producer.utils.ProducerConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * paraflow
 *
 * @author guodong
 */
public class DefaultLoaderProducer implements LoaderProducer
{
    private MetaClient metaClient;
    private Map<String, List<Function<Message, Boolean>>> filtersMap;

    public DefaultLoaderProducer()
    {
        metaClient = new MetaClient(ProducerConfig.INSTANCE().getServerHost(),
                ProducerConfig.INSTANCE().getServerPort());
        filtersMap = new HashMap<>();
    }

    @Override
    public int send(String database, String table, Message message)
    {
        return 0;
    }

    @Override
    public StatusProto.ResponseStatus setTblTopicMapping(String database, String table, String host, String topic)
    {
        return null;
    }

    @Override
    public StatusProto.ResponseStatus createDatabase(String databaseName, String userName, String locationUrl)
    {
        return metaClient.createDatabase(databaseName, userName, locationUrl);
    }

    @Override
    public StatusProto.ResponseStatus createRegularTable(String dbName, String tblName, String userName, String locationUrl, String storageFormatName, ArrayList<String> columnName, ArrayList<String> columnType, ArrayList<String> dataType)
    {
        return metaClient.createRegularTable(dbName, tblName, userName, locationUrl, storageFormatName, columnName, columnType, dataType);
    }

    @Override
    public StatusProto.ResponseStatus createFiberTable(String dbName, String tblName, String userName, String storageFormatName, String fiberColName, String fiberFuncName, ArrayList<String> columnName, ArrayList<String> columnType, ArrayList<String> dataType)
    {
        return metaClient.createFiberTable(dbName, tblName, userName, storageFormatName, fiberColName, fiberFuncName, columnName, columnType, dataType);
    }

    @Override
    public StatusProto.ResponseStatus createFiberTable(String dbName, String tblName, String userName, String locationUrl, String storageFormatName, String fiberColName, String fiberFuncName, ArrayList<String> columnName, ArrayList<String> columnType, ArrayList<String> dataType)
    {
        return metaClient.createFiberTable(dbName, tblName, userName, storageFormatName, fiberColName, fiberFuncName, columnName, columnType, dataType);
    }

    @Override
    public StatusProto.ResponseStatus createFiberFunc(String funcName, Function<Long, Integer> func) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput objOutput = new ObjectOutputStream(bos);
        objOutput.writeObject(func);
        objOutput.flush();
        return metaClient.createFiberFunc(funcName, bos.toByteArray());
    }

    @Override
    public int registerFilter(String database, String table, Function<Message, Boolean> filterFunc)
    {
        String key = database + "." + table;
        if (filtersMap.containsKey(key)) {
            filtersMap.get(key).add(filterFunc);
            return 1;
        }
        else {
            List<Function<Message, Boolean>> funcList = new ArrayList<>();
            funcList.add(filterFunc);
            filtersMap.put(key, funcList);
            return 1;
        }
    }
}

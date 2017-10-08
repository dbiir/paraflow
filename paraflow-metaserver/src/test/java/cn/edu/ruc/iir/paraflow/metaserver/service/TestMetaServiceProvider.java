package cn.edu.ruc.iir.paraflow.metaserver.service;

//import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
//import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
//import cn.edu.ruc.iir.paraflow.metaserver.utils.ColType;
//import org.junit.Test;
//
//import java.util.ArrayList;
//
//import static org.junit.Assert.assertEquals;

/**
 * paraflow
 *
 * @author guodong
 */
public class TestMetaServiceProvider
{
    private MetaServiceProvider serviceProvider = new MetaServiceProvider();

//    @Test
//    public void step06_ClientCreateRegularTableTest()
//    {
//        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
//        ArrayList<String> columnName = new ArrayList<>();
//        columnName.add("smell");
//        columnName.add("color");
//        columnName.add("feel");
//        ArrayList<String> columnType = new ArrayList<>();
//        columnType.add("regular");
//        columnType.add("regular");
//        columnType.add("regular");
//        ArrayList<String> dataType = new ArrayList<>();
//        dataType.add("varchar(20)");
//        dataType.add("varchar(20)");
//        dataType.add("varchar(20)");
//        ArrayList<MetaProto.ColParam> columns = new ArrayList<>();
//        for (int i = 0; i < columnName.size(); i++) {
//            MetaProto.ColParam column = MetaProto.ColParam.newBuilder()
//                    .setColIndex(i)
//                    .setDbName("food")
//                    .setTblName("rice")
//                    .setColName(columnName.get(i))
//                    .setColType(ColType.REGULAR.getColTypeId())
//                    .setDataType(dataType.get(i))
//                    .build();
//            columns.add(column);
//        }
//        MetaProto.ColListType colList = MetaProto.ColListType.newBuilder()
//                .addAllColumn(columns)
//                .build();
//        MetaProto.TblParam table = MetaProto.TblParam.newBuilder()
//                .setDbName("food")
//                .setTblName("rice")
//                .setUserName("alice")
//                .setTblType(0)
//                .setLocationUrl("hdfs:/127.0.0.1/:5432/metadata/food/rice")
//                .setStorageFormatName("StorageFormatName")
//                .setFuncName("none")
//                .setFiberColId(-1)
//                .setColList(colList)
//                .build();
//        StatusProto.ResponseStatus status = serviceProvider.createTable(table);
//        assertEquals(expect, status);
//    }
}

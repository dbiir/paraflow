package cn.edu.ruc.iir.paraflow.metaserver.service;

import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaGrpc;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import io.grpc.stub.StreamObserver;
/**
 * ParaFlow
 *
 * @author guodong
 */
public class MetaService extends MetaGrpc.MetaImplBase
{
    @Override
    public void listDatabases(MetaProto.NoneType none, StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder().addStr("test").addStr("default").build();
        responseStreamObserver.onNext(stringList);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void listTables(MetaProto.DbNameParam dbNameParam, StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder().addStr("employee").addStr("student").addStr("book").build();
        responseStreamObserver.onNext(stringList);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void getDatabase(MetaProto.DbNameParam dbNameParam, StreamObserver<MetaProto.DbModel> responseStreamObserver)
    {
        //MetaProto.UserModel user = MetaProto.UserModel.newBuilder().setUserName("Alice").setUserPass("123456").setRoleName("admin").setCreationTime(20170807).setLastVisitTime(20170807).build();
        MetaProto.DbModel dbModel = MetaProto.DbModel.newBuilder().setDbName("default").setLocationUrl("hdfs:/127.0.0.1:9000/warehouse/default").setUserId(1).build();
        responseStreamObserver.onNext(dbModel);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void getTable(MetaProto.DbTblParam dbTblParam, StreamObserver<MetaProto.TblModel> responseStreamObserver)
    {
        //MetaProto.UserModel user = MetaProto.UserModel.newBuilder().setUserName("Alice").setUserPass("123456").setRoleName("admin").setCreationTime(20170807).setLastVisitTime(20170807).build();
        //MetaProto.DbModel dbModel = MetaProto.DbModel.newBuilder().setDbName("default").setLocationUrl("hdfs:/127.0.0.1:9000/warehouse/default").setUserId(2016100862).build();
        //MetaProto.ColModel column0 = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("name").setDataType("varchar(20)").setColIndex(0).build();
        //MetaProto.ColModel column1 = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("age").setDataType("integer").setColIndex(1).build();
        //MetaProto.ColModel column2 = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("salary").setDataType("double").setColIndex(2).build();
        //MetaProto.ColModel column3 = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("check-in").setDataType("timestamp").setColIndex(3).build();
        //MetaProto.ColModel column4 = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("comment").setDataType("char(10)").setColIndex(4).build();
        //MetaProto.ColumnListType columns = MetaProto.ColumnListType.newBuilder().addColumn(0, column0).addColumn(1, column1).addColumn(2, column2).addColumn(3, column3).addColumn(4, column4).build();
        MetaProto.TblModel tblModel = MetaProto.TblModel.newBuilder().setDbId(1).setCreateTime(20170807).setLastAccessTime(20170807).setUserId(1).setTblName("employee").setTblType(0).setFiberColId(-1).setLocationUrl("hdfs:/127.0.0.1:9000/warehouse/default/employee").setStorageFormat(1).setFiberFuncId(1).build();
        responseStreamObserver.onNext(tblModel);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void getColumn(MetaProto.DbTblColParam dbTblColParam, StreamObserver<MetaProto.ColModel> responseStreamObserver)
    {
        MetaProto.ColModel column = MetaProto.ColModel.newBuilder().setTblId(1).setColName("name").setColType("regular").setDataType("varchar(20)").setColIndex(0).build();
        responseStreamObserver.onNext(column);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createDatabase(MetaProto.DbModel dbModel, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createTable(MetaProto.TblModel tblModel, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void deleteDatabase(MetaProto.DbNameParam dbNameParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void deleteTable(MetaProto.DbTblParam dbTblParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void renameDatabase(MetaProto.RenameDbParam renameDbModel, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void renameTable(MetaProto.RenameTblParam renameTblModel, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void renameColumn(MetaProto.RenameColParam renameColumn, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createFiber(MetaProto.FiberModel fiber, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void listFiberValues(MetaProto.FiberModel fiber, StreamObserver<MetaProto.LongListType> responseStreamObserver)
    {
        MetaProto.LongListType longList = MetaProto.LongListType.newBuilder().addLon(123456).addLon(234567).addLon(345678).build();
        responseStreamObserver.onNext(longList);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void addBlockIndex(MetaProto.AddBlockIndexParam addBlockIndex, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void filterBlockPathsByTime(MetaProto.FilterBlockPathsByTimeParam filterBlockPathsByTime, StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder().addStr(" hdfs://127.0.0.1:9000/warehouse/default/employee/20170807123456").addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/20170807234567").build();
        responseStreamObserver.onNext(stringList);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void filterBlockPaths(MetaProto.FilterBlockPathsParam filterBlockPaths, StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder().addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/123456").addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/234567").build();
        responseStreamObserver.onNext(stringList);
        responseStreamObserver.onCompleted();
    }
}

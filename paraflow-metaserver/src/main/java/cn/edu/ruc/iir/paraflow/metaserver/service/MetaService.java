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
    public void getDatabase(MetaProto.DbNameParam dbNameParam, StreamObserver<MetaProto.DbParam> responseStreamObserver)
    {
        //MetaProto.UserParam user = MetaProto.UserParam.newBuilder().setUserName("Alice").setUserPass("123456").setRoleName("admin").setCreationTime(20170807).setLastVisitTime(20170807).build();
        MetaProto.DbParam dbParam = MetaProto.DbParam.newBuilder().setDbName("default").setLocationUrl("hdfs:/127.0.0.1:9000/warehouse/default").setUserName("Alice").build();
        responseStreamObserver.onNext(dbParam);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void getTable(MetaProto.DbTblParam dbTblParam, StreamObserver<MetaProto.TblParam> responseStreamObserver)
    {
        //MetaProto.UserParam user = MetaProto.UserParam.newBuilder().setUserName("Alice").setUserPass("123456").setRoleName("admin").setCreationTime(20170807).setLastVisitTime(20170807).build();
        //MetaProto.DbParam dbParam = MetaProto.DbParam.newBuilder().setDbName("default").setLocationUrl("hdfs:/127.0.0.1:9000/warehouse/default").setUserId(2016100862).build();
        //MetaProto.ColParam column0 = MetaProto.ColParam.newBuilder().setDatabasename("default").setTablename("employee").setColName("name").setDataType("varchar(20)").setColIndex(0).build();
        //MetaProto.ColParam column1 = MetaProto.ColParam.newBuilder().setDatabasename("default").setTablename("employee").setColName("age").setDataType("integer").setColIndex(1).build();
        //MetaProto.ColParam column2 = MetaProto.ColParam.newBuilder().setDatabasename("default").setTablename("employee").setColName("salary").setDataType("double").setColIndex(2).build();
        //MetaProto.ColParam column3 = MetaProto.ColParam.newBuilder().setDatabasename("default").setTablename("employee").setColName("check-in").setDataType("timestamp").setColIndex(3).build();
        //MetaProto.ColParam column4 = MetaProto.ColParam.newBuilder().setDatabasename("default").setTablename("employee").setColName("comment").setDataType("char(10)").setColIndex(4).build();
        //MetaProto.ColumnListType columns = MetaProto.ColumnListType.newBuilder().addColumn(0, column0).addColumn(1, column1).addColumn(2, column2).addColumn(3, column3).addColumn(4, column4).build();
        MetaProto.TblParam tblParam = MetaProto.TblParam.newBuilder().setDbName("default").setTblName("employee").setTblType(0).setUserName("Alice").setCreateTime(20170807).setLastAccessTime(20170807).setLocationUrl("hdfs:/127.0.0.1:9000/warehouse/default/employee").setStorageFormatId(1).setTblType(0).setFiberColId(-1).setFiberFuncId(1).build();
        responseStreamObserver.onNext(tblParam);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void getColumn(MetaProto.DbTblColParam dbTblColParam, StreamObserver<MetaProto.ColParam> responseStreamObserver)
    {
        MetaProto.ColParam column = MetaProto.ColParam.newBuilder().setColIndex(0).setTblName("employee").setColName("name").setColType("regular").setDataType("varchar(20)").build();
        responseStreamObserver.onNext(column);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createDatabase(MetaProto.DbParam dbParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createTable(MetaProto.TblParam tblParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
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
    public void renameDatabase(MetaProto.RenameDbParam renameDbParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void renameTable(MetaProto.RenameTblParam renameTblParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
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
    public void createDbParam(MetaProto.CreateDbParamParam createDbParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createTblParam(MetaProto.CreateTblParamParam createTblParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createTblPriv(MetaProto.CreateTblPrivParam createTblPriv, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createStorageFormat(MetaProto.CreateStorageFormatParam createStorageFormat, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createFiberFunc(MetaProto.CreateFiberFuncParam createFiberFunc, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createBlockIndex(MetaProto.CreateBlockIndexParam createBlockIndex, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createUser(MetaProto.CreateUserParam createUser, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void filterBlockIndex(MetaProto.FilterBlockIndexParam filterBlockPathsByTime, StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder().addStr(" hdfs://127.0.0.1:9000/warehouse/default/employee/20170807123456").addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/20170807234567").build();
        responseStreamObserver.onNext(stringList);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void filterBlockIndexByFiber(MetaProto.FilterBlockIndexByFiberParam filterBlockPaths, StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder().addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/123456").addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/234567").build();
        responseStreamObserver.onNext(stringList);
        responseStreamObserver.onCompleted();
    }
}

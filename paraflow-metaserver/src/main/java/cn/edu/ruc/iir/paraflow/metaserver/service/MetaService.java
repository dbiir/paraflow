package cn.edu.ruc.iir.paraflow.metaserver.service;

import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaGrpc;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import io.grpc.stub.StreamObserver;


/**
 * ParaFlow
 *
 * @author guodong
 */
public class MetaService extends MetaGrpc.MetaImplBase {
    @Override
    public void listDatabases(MetaProto.NoneType none, StreamObserver<MetaProto.StringListType> responseStreamObserver) {
        MetaProto.StringListType string_list = MetaProto.StringListType.newBuilder().addStr("test").addStr("default").build();
        responseStreamObserver.onNext(string_list);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void listTables(MetaProto.DbNameParam dbNameParam, StreamObserver<MetaProto.StringListType> responseStreamObserver) {
        MetaProto.StringListType string_list = MetaProto.StringListType.newBuilder().addStr("employee").addStr("student").addStr("book").build();
        responseStreamObserver.onNext(string_list);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void getDatabase(MetaProto.DbNameParam dbNameParam, StreamObserver<MetaProto.DbModel> responseStreamObserver) {
        MetaProto.UserModel user = MetaProto.UserModel.newBuilder().setUserName("Alice").setUserPass("123456").setRoleName("admin").setCreationTime(20170807).setLastVisitTime(20170807).build();
        MetaProto.DbModel DbModel = MetaProto.DbModel.newBuilder().setName("default").setLocationUri("hdfs:/127.0.0.1:9000/warehouse/default").setUser(user).build();
        responseStreamObserver.onNext(DbModel);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void getTable(MetaProto.DbTblParam dbTblParam, StreamObserver<MetaProto.TblModel> responseStreamObserver) {
        MetaProto.UserModel user = MetaProto.UserModel.newBuilder().setUserName("Alice").setUserPass("123456").setRoleName("admin").setCreationTime(20170807).setLastVisitTime(20170807).build();
        MetaProto.DbModel DbModel = MetaProto.DbModel.newBuilder().setName("default").setLocationUri("hdfs:/127.0.0.1:9000/warehouse/default").setUser(user).build();
        MetaProto.ColModel column0 = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("name").setDataType("varchar(20)").setColIndex(0).build();
        MetaProto.ColModel column1 = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("age").setDataType("integer").setColIndex(1).build();
        MetaProto.ColModel column2 = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("salary").setDataType("double").setColIndex(2).build();
        MetaProto.ColModel column3 = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("check-in").setDataType("timestamp").setColIndex(3).build();
        MetaProto.ColModel column4 = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("comment").setDataType("char(10)").setColIndex(4).build();
        MetaProto.ColumnListType columns = MetaProto.ColumnListType.newBuilder().addColumn(0,column0).addColumn(1,column1).addColumn(2,column2).addColumn(3,column3).addColumn(4,column4).build();
        MetaProto.TblModel TblModel = MetaProto.TblModel.newBuilder().setDatabase(DbModel).setCreationTime(20170807).setLastAccessTime(20170807).setOwner(user).setTableName("employee").setTableLocationUri("hdfs:/127.0.0.1:9000/warehouse/default/employee").setColumns(columns).build();
        responseStreamObserver.onNext(TblModel);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void getColumn(MetaProto.DbTblColParam dbTblColParam, StreamObserver<MetaProto.ColModel> responseStreamObserver) {
        MetaProto.ColModel column = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("name").setDataType("varchar(20)").setColIndex(0).build();
        responseStreamObserver.onNext(column);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createDatabase(MetaProto.DbModel DbModel, StreamObserver<MetaProto.StatusType> responseStreamObserver) {
        MetaProto.StatusType StatusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(StatusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createTable(MetaProto.TblModel TblModel, StreamObserver<MetaProto.StatusType> responseStreamObserver) {
        MetaProto.StatusType StatusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(StatusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void deleteDatabase(MetaProto.DbNameParam DbNameParam, StreamObserver<MetaProto.StatusType> responseStreamObserver) {
        MetaProto.StatusType StatusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(StatusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void deleteTable(MetaProto.DbTblParam DbTblParam, StreamObserver<MetaProto.StatusType> responseStreamObserver) {
        MetaProto.StatusType StatusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(StatusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void renameDatabase(MetaProto.RenameDbParam renameDbModel, StreamObserver<MetaProto.StatusType> responseStreamObserver) {
        MetaProto.StatusType StatusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(StatusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void renameTable(MetaProto.RenameTblParam renameTblModel, StreamObserver<MetaProto.StatusType> responseStreamObserver) {
        MetaProto.StatusType StatusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(StatusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void renameColumn(MetaProto.RenameColParam renameColumn, StreamObserver<MetaProto.StatusType> responseStreamObserver) {
        MetaProto.StatusType StatusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(StatusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createFiber(MetaProto.FiberModel fiber, StreamObserver<MetaProto.StatusType> responseStreamObserver) {
        MetaProto.StatusType StatusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(StatusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void listFiberValues(MetaProto.FiberModel fiber, StreamObserver<MetaProto.LongListType> responseStreamObserver) {
        MetaProto.LongListType long_list = MetaProto.LongListType.newBuilder().addLon(123456).addLon(234567).addLon(345678).build();
        responseStreamObserver.onNext(long_list);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void addBlockIndex(MetaProto.AddBlockIndexParam addBlockIndex, StreamObserver<MetaProto.StatusType> responseStreamObserver) {
        MetaProto.StatusType StatusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(StatusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void filterBlockPathsByTime(MetaProto.FilterBlockPathsByTimeParam filterBlockPathsByTime, StreamObserver<MetaProto.StringListType> responseStreamObserver) {
        MetaProto.StringListType string_list = MetaProto.StringListType.newBuilder().addStr(" hdfs://127.0.0.1:9000/warehouse/default/employee/20170807123456").addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/20170807234567").build();
        responseStreamObserver.onNext(string_list);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void filterBlockPaths(MetaProto.FilterBlockPathsParam filterBlockPaths, StreamObserver<MetaProto.StringListType> responseStreamObserver) {
        MetaProto.StringListType string_list = MetaProto.StringListType.newBuilder().addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/123456").addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/234567").build();
        responseStreamObserver.onNext(string_list);
        responseStreamObserver.onCompleted();
    }
}
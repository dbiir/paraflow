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
    public void listDatabases(MetaProto.None none, StreamObserver<MetaProto.StringList> responseStreamObserver) {
        MetaProto.StringList string_list = MetaProto.StringList.newBuilder().setStr(0,"test").setStr(1,"default").build();
        responseStreamObserver.onNext(string_list);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void listTables(MetaProto.DatabaseName database, StreamObserver<MetaProto.StringList> responseStreamObserver) {
        MetaProto.StringList string_list = MetaProto.StringList.newBuilder().setStr(0,"employee").setStr(1,"student").setStr(2,"book").build();
        responseStreamObserver.onNext(string_list);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void getDatabase(MetaProto.DatabaseName databasename, StreamObserver<MetaProto.Database> responseStreamObserver) {
        MetaProto.User user = MetaProto.User.newBuilder().setUserName("Alice").setUserPass("123456").setRoleName("admin").setCreationTime(20170807).setLastVisitTime(20170807).build();
        MetaProto.Database database = MetaProto.Database.newBuilder().setName("default").setLocationUri("hdfs:/127.0.0.1:9000/warehouse/default").setUser(user).build();
        responseStreamObserver.onNext(database);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void getTable(MetaProto.DatabaseTable databasetable, StreamObserver<MetaProto.Table> responseStreamObserver) {
        MetaProto.User user = MetaProto.User.newBuilder().setUserName("Alice").setUserPass("123456").setRoleName("admin").setCreationTime(20170807).setLastVisitTime(20170807).build();
        MetaProto.Database database = MetaProto.Database.newBuilder().setName("default").setLocationUri("hdfs:/127.0.0.1:9000/warehouse/default").setUser(user).build();
        MetaProto.Column column0 = MetaProto.Column.newBuilder().setDatabasename("default").setTablename("employee").setColName("name").setDataType("varchar(20)").setColIndex(0).build();
        MetaProto.Column column1 = MetaProto.Column.newBuilder().setDatabasename("default").setTablename("employee").setColName("age").setDataType("integer").setColIndex(1).build();
        MetaProto.Column column2 = MetaProto.Column.newBuilder().setDatabasename("default").setTablename("employee").setColName("salary").setDataType("double").setColIndex(2).build();
        MetaProto.Column column3 = MetaProto.Column.newBuilder().setDatabasename("default").setTablename("employee").setColName("check-in").setDataType("timestamp").setColIndex(3).build();
        MetaProto.Column column4 = MetaProto.Column.newBuilder().setDatabasename("default").setTablename("employee").setColName("comment").setDataType("char(10)").setColIndex(0).build();
        MetaProto.Columns columns = MetaProto.Columns.newBuilder().setColumn(0,column0).setColumn(1,column1).setColumn(2,column2).setColumn(3,column3).setColumn(4,column4).build();
        MetaProto.Table table = MetaProto.Table.newBuilder().setDatabase(database).setCreationTime(20170807).setLastAccessTime(20170807).setOwner(user).setTableName("employee").setTableLocationUri("hdfs:/127.0.0.1:9000/warehouse/default/employee").setColumns(columns).build();
        responseStreamObserver.onNext(table);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void getColumn(MetaProto.DatabaseTableColumn databasetablecolumn, StreamObserver<MetaProto.Column> responseStreamObserver) {
        MetaProto.Column column = MetaProto.Column.newBuilder().setDatabasename("default").setTablename("employee").setColName("name").setDataType("varchar(20)").setColIndex(0).build();
        responseStreamObserver.onNext(column);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createDatabase(MetaProto.Database database, StreamObserver<MetaProto.Status> responseStreamObserver) {
        MetaProto.Status status = MetaProto.Status.newBuilder().;
        responseStreamObserver.onNext(status);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createTable(MetaProto.Table table, StreamObserver<MetaProto.Status> responseStreamObserver) {
        MetaProto.Status status = MetaProto.Status.newBuilder().build();
        responseStreamObserver.onNext(status);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void deleteDatabase(MetaProto.DatabaseName databasename, StreamObserver<MetaProto.Status> responseStreamObserver) {
        MetaProto.Status status = MetaProto.Status.newBuilder().build();
        responseStreamObserver.onNext(status);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void deleteTable(MetaProto.DatabaseTable databasetable, StreamObserver<MetaProto.Status> responseStreamObserver) {
        MetaProto.Status status = MetaProto.Status.newBuilder().build();
        responseStreamObserver.onNext(status);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void renameDatabase(MetaProto.RenameDatabase renamedatabase, StreamObserver<MetaProto.Status> responseStreamObserver) {
        MetaProto.Status status = MetaProto.Status.newBuilder().build();
        responseStreamObserver.onNext(status);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void renameTable(MetaProto.RenameTable renametable, StreamObserver<MetaProto.Status> responseStreamObserver) {
        MetaProto.Status status = MetaProto.Status.newBuilder().build();
        responseStreamObserver.onNext(status);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void renameColumn(MetaProto.RenameColumn renamecolumn, StreamObserver<MetaProto.Status> responseStreamObserver) {
        MetaProto.Status status = MetaProto.Status.newBuilder().build();
        responseStreamObserver.onNext(status);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createFiber(MetaProto.Fiber fiber, StreamObserver<MetaProto.Status> responseStreamObserver) {
        MetaProto.Status status = MetaProto.Status.newBuilder().build();
        responseStreamObserver.onNext(status);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void listFiberValues(MetaProto.Fiber fiber, StreamObserver<MetaProto.LongList> responseStreamObserver) {
        MetaProto.LongList long_list = MetaProto.LongList.newBuilder().build();
        responseStreamObserver.onNext(long_list);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void addBlockIndex(MetaProto.AddBlockIndex addblockindex, StreamObserver<MetaProto.Status> responseStreamObserver) {
        MetaProto.Status status = MetaProto.Status.newBuilder().build();
        responseStreamObserver.onNext(status);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void filterBlockPathsByTime(MetaProto.FilterBlockPathsByTime filterblockpathsbytime, StreamObserver<MetaProto.StringList> responseStreamObserver) {
        MetaProto.StringList string_list = MetaProto.StringList.newBuilder().build();
        responseStreamObserver.onNext(string_list);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void filterBlockPaths(MetaProto.FilterBlockPaths filterblockpaths, StreamObserver<MetaProto.StringList> responseStreamObserver) {
        MetaProto.StringList string_list = MetaProto.StringList.newBuilder().build();
        responseStreamObserver.onNext(string_list);
        responseStreamObserver.onCompleted();
    }
}
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
package cn.edu.ruc.iir.paraflow.metaserver.service;

import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;

import io.grpc.stub.StreamObserver;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class SqlGenerator
{
    public String listDatabases()
    {
        String sql = "SELECT * FROM DbModel;";
        return sql;
    }

    public String findDatabaseId(String dbName)
    {
        String sql = "SELECT dbId FROM DbModel WHERE dbName = '" + dbName + "';";
        return sql;
    }

    public String listTables(int dbId)
    {
        String sql = "SELECT * FROM TblModel WHERE dbId = '" + dbId + "';";
        return sql;
    }

    public String getDatabase(String dbName)
    {
        String sql = "SELECT * FROM DbModel WHERE dbName = '" + dbName + "';";
        return sql;
    }

    public String getTable(int dbId, String tblName)
    {
        String sql = "SELECT * FROM TblModel WHERE dbId = '" + dbId + "' AND tblName = '" + tblName + "';";
        return sql;
    }

    public String findTableId(String tblName)
    {
        String sql = "SELECT tblId FROM TblModel WHERE tblName = " + tblName + "';";
        return sql;
    }

    public String getColumn(int tblId, String colName)
    {
        String sql = "SELECT * FROM ColModel WHERE tblId = '" + tblId + "' AND colName = '" + colName + "';";
        return sql;
    }

    public String createDatabase(int dbId, String dbName, String locationUrl, int userId)
    {
        String sql = "INSERT INTO DbModel VALUES('" + dbId + "','" + dbName + "','" + locationUrl + "','" + userId + "');";
        return sql;
    }

    public String createTable(int tblId, int dbId, int createTime, int lastAccessTime, int userId, String tblName, int tblType, int fiberColId, String locationUrl, int storageFormat, int fiberFuncId)
    {
        String sql = "INSERT INTO TblModel VALUES('" + tblId + "','"
                + dbId + "','" + createTime + "','" + lastAccessTime + "','"
                + userId + "','" + tblName + "','" + tblType + "','" + fiberColId + "','"
                + locationUrl + "','" + storageFormat + "','" + fiberFuncId + "');";
        return sql;
    }

    public String deleteDatabase(String dbName)
    {
        String sql = "DELETE FROM Dbmodel WHERE dbName = '" + dbName + "';";
        return sql;
    }

    public String deleteTable(int dbId, int tblId)
    {
        String sql = "DELETE FROM TblModel WHERE dbId = '" + dbId + "' AND tblId = '" + tblId + "';";
        return sql;
    }

    public String renameDatabase(String oldName, String newName)
    {
        String sql = "UPDATE DbModel SET dbName = '" + newName + "' WHERE dbName = '" + oldName + "';";
        return sql;
    }

    public String renameTable(int dbId, String oldName, String newName)
    {
        String sql = "UPDATE TblModel SET tblName = '" + newName + "' WHERE dbId = '" + dbId + "' AND tblName = '" + oldName + "';";
        return sql;
    }

    public String renameColumn(int dbId, int tblId, String oldName, String newName)
    {
        String sql = "UPDATA ColModel SET colName = '" + newName + "' WHERE dbId = '" + dbId + "' AND tblId = '" + tblId + "' AND colName = '" + oldName + "';";
        return sql;
    }

    public void addBlockIndex(MetaProto.CreateBlockIndexParam addBlockIndex, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
    }

//    public void filterBlockIndex(MetaProto.FilterBlockIndex filterBlockPathsByTime, StreamObserver<MetaProto.StringListType> responseStreamObserver)
//    {
//    }
//
//    public void filterBlockPaths(MetaProto.FilterBlockPathsParam filterBlockPaths, StreamObserver<MetaProto.StringListType> responseStreamObserver)
//    {
//    }
}

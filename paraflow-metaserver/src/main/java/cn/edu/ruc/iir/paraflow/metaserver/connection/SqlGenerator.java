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
package cn.edu.ruc.iir.paraflow.metaserver.connection;

import com.google.protobuf.ByteString;

/**
 */
public class SqlGenerator
{
    public String createUser(String userName, String password, long createTime, long lastVisitTime)
    {
        String sql = String.format("INSERT INTO usermodel (username,password,createtime,lastvisittime) VALUES('%s','%s','%d','%d');", userName, password, createTime, lastVisitTime);
        return sql;
    }

    public String listDatabases()
    {
        String sql = "SELECT dbname FROM dbmodel;";
        return sql;
    }

    public String findDbId(String dbName)
    {
        String sql = String.format("SELECT dbid FROM dbmodel WHERE dbname = '%s';", dbName);
        return sql;
    }

    public String listTables(int dbId)
    {
        String sql = String.format("SELECT tblname FROM tblmodel WHERE dbid = '%d';", dbId);
        return sql;
    }

    public String getDatabase(String dbName)
    {
        String sql = String.format("SELECT dbname,locationurl,userid FROM dbmodel WHERE dbname = '%s';", dbName);
        return sql;
    }

    public String findUserName(int userId)
    {
        String sql = String.format("SELECT username FROM usermodel WHERE userid = '%d';", userId);
        return sql;
    }

    public String getTable(int dbId, String tblName)
    {
        String sql = String.format("SELECT tbltype,userid,createtime,lastaccesstime,locationurl,storageformatid,fibercolid,fiberfuncid FROM tblmodel WHERE dbid = '%d' AND tblname = '%s';", dbId, tblName);
        return sql;
    }

    public String findTblId(int dbId, String tblName)
    {
        String sql = String.format("SELECT tblid FROM tblmodel WHERE dbid = '%d' AND tblname = '%s';", dbId, tblName);
        return sql;
    }

    public String getColumn(int tblId, String colName)
    {
        String sql = String.format("SELECT colindex,coltype,datatype FROM colmodel WHERE tblid = '%d' AND colname = '%s';", tblId, colName);
        return sql;
    }
//    public String findTblName(int tblId)
//    {
//        String sql = String.format("SELECT tblname FROM tblmodel WHERE tblid = '%d';", tblId);
//        return sql;
//    }

    public String findUserId(String userName)
    {
        String sql = String.format("SELECT userid FROM usermodel WHERE username = '%s';", userName);
        return sql;
    }

    public String createDatabase(String dbName, int userId, String locationUrl)
    {
        String sql = String.format("INSERT INTO dbmodel (dbname,userid,locationurl) VALUES('%s','%d','%s');", dbName, userId, locationUrl);
        return sql;
    }

    public String createTable(int dbId, String tblName, int tblType, int userId, long createTime, long lastAccessTime, String locationUrl, int storageFormatId, long fiberColId, long fiberFuncId)
    {
        String sql = String.format("INSERT INTO tblmodel (dbid,tblname,tbltype,userid,createtime,lastaccesstime,locationurl,storageformatid,fibercolid,fiberfuncid) VALUES('%d','%s','%d','%d','%d','%d','%s','%d','%d','%d');", dbId, tblName, tblType, userId, createTime, lastAccessTime, locationUrl, storageFormatId, fiberColId, fiberFuncId);
        return sql;
    }

    public String createColumn(int colIndex, int dbId, String colName, int tblId, String colType, String dataType)
    {
        String sql = String.format("INSERT INTO colmodel (colindex,dbid,colname,tblid,coltype,dataType) VALUES('%d','%d','%s','%d','%s','%s');", colIndex, dbId, colName, tblId, colType, dataType);
        return sql;
    }

    public String renameColumn(int dbId, int tblId, String oldName, String newName)
    {
        String sql = String.format("UPDATE colmodel SET colname = '%s' WHERE dbid = '%d' AND tblid = '%d' AND colname = '%s';", newName, dbId, tblId, oldName);
        return sql;
    }

    public String renameTable(int dbId, String oldName, String newName)
    {
        String sql = String.format("UPDATE tblmodel SET tblname = '%s' WHERE dbid = '%d' AND tblname = '%s';", newName, dbId, oldName);
        return sql;
    }

    public String renameDatabase(String oldName, String newName)
    {
        String sql = String.format("UPDATE dbmodel SET dbname = '%s' WHERE dbname = '%s';", newName, oldName);
        return sql;
    }

    public String deleteTblColumn(int dbId, int tblId)
    {
        String sql = String.format("DELETE FROM colmodel WHERE dbid = '%d' AND tblid = '%d';", dbId, tblId);
        return sql;
    }

    public String deleteDbColumn(int dbId)
    {
        String sql = String.format("DELETE FROM colmodel WHERE dbid = '%d';", dbId);
        return sql;
    }

    public String deleteTable(int dbId, String tblName)
    {
        String sql = String.format("DELETE FROM tblmodel WHERE dbid = '%d' AND tblname = '%s';", dbId, tblName);
        return sql;
    }

    public String deleteDbTable(int dbId)
    {
        String sql = String.format("DELETE FROM tblmodel WHERE dbid = '%d';", dbId);
        return sql;
    }

    public String deleteDatabase(String dbName)
    {
        String sql = String.format("DELETE FROM dbmodel WHERE dbname = '%s';", dbName);
        return sql;
    }

    public String createDbParam(int dbId, String paramKey, String paramValue)
    {
        String sql = String.format("INSERT INTO dbparammodel (dbid,paramkey,paramvalue) VALUES('%d','%s','%s');", dbId, paramKey, paramValue);
        return sql;
    }

    public String createTblParam(int tblId, String paramKey, String paramValue)
    {
        String sql = String.format("INSERT INTO tblparammodel (tblid,paramkey,paramvalue) VALUES('%d','%s','%s');", tblId, paramKey, paramValue);
        return sql;
    }

    public String createTblPriv(int tblId, int userId, int privType, long grantTime)
    {
        String sql = String.format("INSERT INTO tblprivmodel (tblid,userid,privtype,granttime) VALUES('%d','%d','%d','%d');", tblId, userId, privType, grantTime);
        return sql;
    }

    public String createStorageFormat(String storageFormatName, String compression, String serialFormat)
    {
        String sql = String.format("INSERT INTO storageformatmodel (storageformatname,compression,serialformat) VALUES('%s','%s','%s');", storageFormatName, compression, serialFormat);
        return sql;
    }

    public String createFiberFunc(String fiberFuncName, ByteString fiberFuncContent)
    {
        String sql = String.format("INSERT INTO fiberfuncmodel (fiberfuncname,fiberfunccontent) VALUES('%s','%s');", fiberFuncName, fiberFuncContent);
        return sql;
    }

    public String createBlockIndex(int tblId, long value, long timeBegin, long timeEnd, String timeZone, String blockPath)
    {
        String sql = String.format("INSERT INTO blockindex (tblid,fibervalue,timebegin,timeend,timezone,blockpath) VALUES('%d','%d','%d','%d','%s','%s');", tblId, value, timeBegin, timeEnd, timeZone, blockPath);
        return sql;
    }

    public String filterBlockIndexBeginEnd(int tblId, long timeBegin, long timeEnd)
    {
        String sql = String.format("SELECT blockPath FROM BlockIndex WHERE tblid = '%d' AND (timeBegin < '%d' OR timeEnd > '%d');", tblId, timeEnd, timeBegin);
        return sql;
    }

    public String filterBlockIndexBegin(int tblId, long timeBegin)
    {
        String sql = String.format("SELECT blockPath FROM BlockIndex WHERE tblid = '%d' AND timeEnd > '%d';", tblId, timeBegin);
        return sql;
    }

    public String filterBlockIndexEnd(int tblId, long timeEnd)
    {
        String sql = String.format("SELECT blockPath FROM BlockIndex WHERE tblid = '%d' AND timeBegin < '%d';", tblId, timeEnd);
        return sql;
    }

    public String filterBlockIndex(int tblId)
    {
        String sql = String.format("SELECT blockPath FROM BlockIndex WHERE tblid = '%d';", tblId);
        return sql;
    }

    public String filterBlockIndexByFiberBeginEnd(int tblId, long value, long timeBegin, long timeEnd)
    {
        String sql = String.format("SELECT blockPath FROM BlockIndex WHERE tblid = '%d' AND fiberValue = '%d' AND (timeBegin < '%d' OR timeEnd > '%d');", tblId, value, timeEnd, timeBegin);
        return sql;
    }

    public String filterBlockIndexByFiberBegin(int tblId, long value, long timeBegin)
    {
        String sql = String.format("SELECT blockPath FROM BlockIndex WHERE tblid = '%d' AND fiberValue = '%d' AND timeEnd > '%d';", tblId, value, timeBegin);
        return sql;
    }

    public String filterBlockIndexByFiberEnd(int tblId, long value, long timeEnd)
    {
        String sql = String.format("SELECT blockPath FROM BlockIndex WHERE tblid = '%d' AND fiberValue = '%d' AND timeBegin < '%d';", tblId, value, timeEnd);
        return sql;
    }

    public String filterBlockIndexByFiber(int tblId, long value)
    {
        String sql = String.format("SELECT blockPath FROM BlockIndex WHERE tblid = '%d' AND fiberValue = '%d';", tblId, value);
        return sql;
    }
}

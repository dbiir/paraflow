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
        return String.format("INSERT INTO usermodel (username,password,createtime,lastvisittime) VALUES('%s','%s',%d,%d);", userName, password, createTime, lastVisitTime);
    }

    public String listDatabases()
    {
        return "SELECT dbname FROM dbmodel;";
    }

    public String findDbId(String dbName)
    {
        return String.format("SELECT dbid FROM dbmodel WHERE dbname = '%s';", dbName);
    }

    public String listTables(int dbId)
    {
        return String.format("SELECT tblname FROM tblmodel WHERE dbid = %d;", dbId);
    }

    public String getDatabase(String dbName)
    {
        return String.format("SELECT dbname,locationurl,userid FROM dbmodel WHERE dbname = '%s';", dbName);
    }

    public String findUserName(int userId)
    {
        return String.format("SELECT username FROM usermodel WHERE userid = %d;", userId);
    }

    public String getTable(int dbId, String tblName)
    {
        return String.format("SELECT tbltype,userid,createtime,lastaccesstime,locationurl,storageformatid,fibercolid,fiberfuncid FROM tblmodel WHERE dbid = %d AND tblname = '%s';", dbId, tblName);
    }

    public String findTblId(int dbId, String tblName)
    {
        return String.format("SELECT tblid FROM tblmodel WHERE dbid = %d AND tblname = '%s';", dbId, tblName);
    }

    public String findTblIdWithoutName(int dbId)
    {
        return String.format("SELECT tblid FROM tblmodel WHERE dbid = %d;", dbId);
    }

    public String getColumn(int tblId, String colName)
    {
        return String.format("SELECT colindex,coltype,datatype FROM colmodel WHERE tblid = %d AND colname = '%s';", tblId, colName);
    }
//    public String findTblName(int tblId)
//    {
//        String sql = String.format("SELECT tblname FROM tblmodel WHERE tblid = %d;", tblId);
//        return sql;
//    }

    public String findUserId(String userName)
    {
        return String.format("SELECT userid FROM usermodel WHERE username = '%s';", userName);
    }

    public String createDatabase(String dbName, int userId, String locationUrl)
    {
        return String.format("INSERT INTO dbmodel (dbname,userid,locationurl) VALUES('%s',%d,'%s');", dbName, userId, locationUrl);
    }

    public String createTable(int dbId, String tblName, int tblType, int userId, long createTime, long lastAccessTime, String locationUrl, int storageFormatId, long fiberColId, long fiberFuncId)
    {
        return String.format("INSERT INTO tblmodel (dbid,tblname,tbltype,userid,createtime,lastaccesstime,locationurl,storageformatid,fibercolid,fiberfuncid) VALUES(%d,'%s',%d,%d,%d,%d,'%s',%d,%d,%d);", dbId, tblName, tblType, userId, createTime, lastAccessTime, locationUrl, storageFormatId, fiberColId, fiberFuncId);
    }

    public String createColumn(int colIndex, int dbId, String colName, int tblId, String colType, String dataType)
    {
        return String.format("INSERT INTO colmodel (colindex,dbid,colname,tblid,coltype,dataType) VALUES(%d,%d,'%s',%d,'%s','%s');", colIndex, dbId, colName, tblId, colType, dataType);
    }

    public String renameColumn(int dbId, int tblId, String oldName, String newName)
    {
        return String.format("UPDATE colmodel SET colname = '%s' WHERE dbid = %d AND tblid = %d AND colname = '%s';", newName, dbId, tblId, oldName);
    }

    public String renameTable(int dbId, String oldName, String newName)
    {
        return String.format("UPDATE tblmodel SET tblname = '%s' WHERE dbid = %d AND tblname = '%s';", newName, dbId, oldName);
    }

    public String renameDatabase(String oldName, String newName)
    {
        return String.format("UPDATE dbmodel SET dbname = '%s' WHERE dbname = '%s';", newName, oldName);
    }

    public String deleteTblColumn(int dbId, int tblId)
    {
        return String.format("DELETE FROM colmodel WHERE dbid = %d AND tblid = %d;", dbId, tblId);
    }

    public String deleteDbColumn(int dbId)
    {
        return String.format("DELETE FROM colmodel WHERE dbid = %d;", dbId);
    }

    public String deleteTable(int dbId, String tblName)
    {
        return String.format("DELETE FROM tblmodel WHERE dbid = %d AND tblname = '%s';", dbId, tblName);
    }

    public String deleteDbTable(int dbId)
    {
        return String.format("DELETE FROM tblmodel WHERE dbid = %d;", dbId);
    }

    public String deleteDatabase(String dbName)
    {
        return String.format("DELETE FROM dbmodel WHERE dbname = '%s';", dbName);
    }

    public String createDbParam(int dbId, String paramKey, String paramValue)
    {
        return String.format("INSERT INTO dbparammodel (dbid,paramkey,paramvalue) VALUES(%d,'%s','%s');", dbId, paramKey, paramValue);
    }

    public String createTblParam(int tblId, String paramKey, String paramValue)
    {
        return String.format("INSERT INTO tblparammodel (tblid,paramkey,paramvalue) VALUES(%d,'%s','%s');", tblId, paramKey, paramValue);
    }

    public String createTblPriv(int tblId, int userId, int privType, long grantTime)
    {
        return String.format("INSERT INTO tblprivmodel (tblid,userid,privtype,granttime) VALUES(%d,%d,%d,%d);", tblId, userId, privType, grantTime);
    }

    public String createStorageFormat(String storageFormatName, String compression, String serialFormat)
    {
        return String.format("INSERT INTO storageformatmodel (storageformatname,compression,serialformat) VALUES('%s','%s','%s');", storageFormatName, compression, serialFormat);
    }

    public String createFiberFunc(String fiberFuncName, ByteString fiberFuncContent)
    {
        return String.format("INSERT INTO fiberfuncmodel (fiberfuncname,fiberfunccontent) VALUES('%s','%s');", fiberFuncName, fiberFuncContent);
    }

    public String createBlockIndex(int tblId, long value, long timeBegin, long timeEnd, String timeZone, String blockPath)
    {
        return String.format("INSERT INTO blockindex (tblid,fibervalue,timebegin,timeend,timezone,blockpath) VALUES(%d,%d,%d,%d,'%s','%s');", tblId, value, timeBegin, timeEnd, timeZone, blockPath);
    }

    public String findTblParamKey(int tblId)
    {
        return String.format("SELECT paramkey FROM tblparammodel WHERE tblid = %d;", tblId);
    }

    public String deleteTblParam(int tblId)
    {
        return String.format("DELETE FROM tblparammodel WHERE tblid = %d;", tblId);
    }

    public String findTblPriv(int tblId)
    {
        return String.format("SELECT tblprivid FROM tblprivmodel WHERE tblid = %d;", tblId);
    }

    public String deleteTblPriv(int tblId)
    {
        return String.format("DELETE FROM tblprivmodel WHERE tblid = %d;", tblId);
    }

    public String findBlockIndex(int tblId)
    {
        return String.format("SELECT blockindexid FROM blockindex WHERE tblid = %d;", tblId);
    }

    public String deleteBlockIndex(int tblId)
    {
        return String.format("DELETE FROM BlockIndex WHERE tblid = %d;", tblId);
    }

    public String findDbParamKey(int dbId)
    {
        return String.format("SELECT paramkey FROM dbparammodel WHERE dbid = %d;", dbId);
    }

    public String deleteDbParam(int dbId)
    {
        return String.format("DELETE FROM dbparammodel WHERE dbid = %d;", dbId);
    }

    public String filterBlockIndexBeginEnd(int tblId, long timeBegin, long timeEnd)
    {
        return String.format("SELECT blockPath FROM BlockIndex WHERE tblid = %d AND (timeBegin < %d OR timeEnd > %d);", tblId, timeEnd, timeBegin);
    }

    public String filterBlockIndexBegin(int tblId, long timeBegin)
    {
        return String.format("SELECT blockPath FROM BlockIndex WHERE tblid = %d AND timeEnd > %d;", tblId, timeBegin);
    }

    public String filterBlockIndexEnd(int tblId, long timeEnd)
    {
        return String.format("SELECT blockPath FROM BlockIndex WHERE tblid = %d AND timeBegin < %d;", tblId, timeEnd);
    }

    public String filterBlockIndex(int tblId)
    {
        return String.format("SELECT blockPath FROM BlockIndex WHERE tblid = %d;", tblId);
    }

    public String filterBlockIndexByFiberBeginEnd(int tblId, long value, long timeBegin, long timeEnd)
    {
        return String.format("SELECT blockPath FROM BlockIndex WHERE tblid = %d AND fiberValue = %d AND (timeBegin < %d OR timeEnd > %d);", tblId, value, timeEnd, timeBegin);
    }

    public String filterBlockIndexByFiberBegin(int tblId, long value, long timeBegin)
    {
        return String.format("SELECT blockPath FROM BlockIndex WHERE tblid = %d AND fiberValue = %d AND timeEnd > %d;", tblId, value, timeBegin);
    }

    public String filterBlockIndexByFiberEnd(int tblId, long value, long timeEnd)
    {
        return String.format("SELECT blockPath FROM BlockIndex WHERE tblid = %d AND fiberValue = %d AND timeBegin < %d;", tblId, value, timeEnd);
    }

    public String filterBlockIndexByFiber(int tblId, long value)
    {
        return String.format("SELECT blockPath FROM BlockIndex WHERE tblid = %d AND fiberValue = %d;", tblId, value);
    }

    public String findFiberFuncId(String fiberColName)
    {
        return String.format("SELECT fiberfuncid FROM fiberfuncmodel WHERE fiberfuncname = '%s';", fiberColName);
    }

    public String findStorageFormatId(String storageFormatName)
    {
        return String.format("SELECT storageformatid FROM storageformatmodel WHERE storageformatname = '%s';", storageFormatName);
    }

    public String findFiberFuncName(int fiberColId)
    {
        return String.format("SELECT fiberfuncname FROM fiberfuncmodel WHERE fiberfuncid = '%d';", fiberColId);
    }

    public String findStorageFormatName(int storageFormatId)
    {
        return String.format("SELECT storageformatname FROM storageformatmodel WHERE storageformatid = '%d';", storageFormatId);
    }

    public String insertVerModel(String verName)
    {
        return String.format("INSERT INTO vermodel (vername) VALUES('%s)");
    }
}

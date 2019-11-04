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
package cn.edu.ruc.iir.paraflow.metaserver.utils;

public class SQLTemplate
{
    private SQLTemplate()
    {
    }

    public static String createUser(String userName, String password, long createTime, long lastVisitTime)
    {
        return String.format("INSERT INTO meta_usermodel (username, password, createtime, lastvisittime) VALUES('%s','%s',%d,%d);",
                userName,
                password,
                createTime,
                lastVisitTime);
    }

    public static String listDatabases()
    {
        return "SELECT dbname FROM meta_dbmodel;";
    }

    public static String findDbId(String dbName)
    {
        return String.format("SELECT dbid FROM meta_dbmodel WHERE dbname = '%s';", dbName);
    }

    public static String listTables(long dbId)
    {
        return String.format("SELECT tblname FROM meta_tblmodel WHERE dbid = %d;", dbId);
    }

    public static String listColumns(long dbId, long tblId)
    {
        return String.format("SELECT colName FROM meta_colmodel WHERE dbid = %d AND tblid = %d ORDER BY colindex;", dbId, tblId);
    }

    public static String listColumnsId(long dbId, long tblId)
    {
        return String.format("SELECT colid FROM meta_colmodel WHERE dbid = %d AND tblid = %d ORDER BY colindex;", dbId, tblId);
    }

    public static String listColumnsDataType(long dbId, long tblId)
    {
        return String.format("SELECT dataType FROM meta_colmodel WHERE dbid = %d AND tblid = %d ORDER BY colindex;", dbId, tblId);
    }

    public static String getDatabase(String dbName)
    {
        return String.format("SELECT dbname, locationurl, userid FROM meta_dbmodel WHERE dbname = '%s';",
                dbName);
    }

    public static String findUserName(long userId)
    {
        return String.format("SELECT username FROM meta_usermodel WHERE userid = %d;", userId);
    }

    public static String getTable(long dbId, String tblName)
    {
        return String.format("SELECT tblid,userid,createtime,lastaccesstime,locationurl,storageformat,fibercolid,timecolid,fiberfunc FROM meta_tblmodel WHERE dbid = %d AND tblname = '%s';",
                dbId,
                tblName);
    }

    public static String findTblId(long dbId, String tblName)
    {
        return String.format("SELECT tblid FROM meta_tblmodel WHERE dbid = %d AND tblname = '%s';", dbId, tblName);
    }

    public static String findTblIdWithoutName(long dbId)
    {
        return String.format("SELECT tblid FROM meta_tblmodel WHERE dbid = %d;", dbId);
    }

    public static String getColumn(long tblId, String colName)
    {
        return String.format("SELECT colindex, coltype, datatype FROM meta_colmodel WHERE tblid = %d AND colname = '%s';",
                tblId,
                colName);
    }

    public static String getColumnName(long dbId, long tblId, long colId)
    {
        return String.format("SELECT colname FROM meta_colmodel WHERE dbid = %d AND tblid = %d AND colindex = %d;",
                dbId,
                tblId,
                colId);
    }

    public static String findUserId(String userName)
    {
        return String.format("SELECT userid FROM meta_usermodel WHERE username = '%s';", userName);
    }

    public static String createDatabase(String dbName, long userId, String locationUrl)
    {
        return String.format("INSERT INTO meta_dbmodel (dbname, userid, locationurl) VALUES('%s',%d,'%s');",
                dbName,
                userId,
                locationUrl);
    }

    public static String createTable(long dbId,
                                     String tblName,
                                     long userId,
                                     long createTime,
                                     long lastAccessTime,
                                     String locationUrl,
                                     String storageFormatName,
                                     long fiberColId,
                                     long timeColId,
                                     String partitionerName)
    {
        return String.format("INSERT INTO meta_tblmodel (dbid, tblname, userid, createtime, lastaccesstime, locationurl, storageformat, fibercolid, timecolid, fiberfunc) VALUES(%d,'%s',%d,%d,%d,'%s','%s',%d,%d,'%s');",
                dbId,
                tblName,
                userId,
                createTime,
                lastAccessTime,
                locationUrl,
                storageFormatName,
                fiberColId,
                timeColId,
                partitionerName);
    }

    public static String createColumn(int colIndex, long dbId, String colName, long tblId, int colType, String dataType)
    {
        return String.format("INSERT INTO meta_colmodel (colindex, dbid, colname, tblid, coltype, dataType) VALUES(%d,%d,'%s',%d,'%d','%s');",
                colIndex,
                dbId,
                colName,
                tblId,
                colType,
                dataType);
    }

    public static String renameColumn(long dbId, long tblId, String oldName, String newName)
    {
        return String.format("UPDATE meta_colmodel SET colname = '%s' WHERE dbid = %d AND tblid = %d AND colname = '%s';",
                newName,
                dbId,
                tblId,
                oldName);
    }

    public static String renameTable(long dbId, String oldName, String newName)
    {
        return String.format("UPDATE meta_tblmodel SET tblname = '%s' WHERE dbid = %d AND tblname = '%s';",
                newName,
                dbId,
                oldName);
    }

    public static String renameDatabase(String oldName, String newName)
    {
        return String.format("UPDATE meta_dbmodel SET dbname = '%s' WHERE dbname = '%s';", newName, oldName);
    }

    public static String deleteTblColumn(long dbId, long tblId)
    {
        return String.format("DELETE FROM meta_colmodel WHERE dbid = %d AND tblid = %d;", dbId, tblId);
    }

    public static String findDbColumn(long dbId)
    {
        return String.format("SELECT * FROM meta_colmodel WHERE dbid = %d;", dbId);
    }

    public static String deleteDbColumn(long dbId)
    {
        return String.format("DELETE FROM meta_colmodel WHERE dbid = %d;", dbId);
    }

    public static String deleteTable(long dbId, String tblName)
    {
        return String.format("DELETE FROM meta_tblmodel WHERE dbid = %d AND tblname = '%s';", dbId, tblName);
    }

    public static String findDbTable(long dbId)
    {
        return String.format("SELECT * FROM meta_tblmodel WHERE dbid = %d;", dbId);
    }

    public static String deleteDbTable(long dbId)
    {
        return String.format("DELETE FROM meta_tblmodel WHERE dbid = %d;", dbId);
    }

    public static String deleteDatabase(String dbName)
    {
        return String.format("DELETE FROM meta_dbmodel WHERE dbname = '%s';", dbName);
    }

    public static String createDbParam(long dbId, String paramKey, String paramValue)
    {
        return String.format("INSERT INTO meta_dbparammodel (dbid, paramkey, paramvalue) VALUES(%d,'%s','%s');",
                dbId,
                paramKey,
                paramValue);
    }

    public static String createTblParam(long tblId, String paramKey, String paramValue)
    {
        return String.format("INSERT INTO meta_tblparammodel (tblid, paramkey, paramvalue) VALUES(%d,'%s','%s');",
                tblId,
                paramKey,
                paramValue);
    }

    public static String createTblPriv(long tblId, long userId, int privType, long grantTime)
    {
        return String.format("INSERT INTO meta_tblprivmodel (tblid, userid, privtype, granttime) VALUES(%d,%d,%d,%d);",
                tblId,
                userId,
                privType,
                grantTime);
    }

    public static String createBlockIndex(long tblId, long value, long timeBegin, long timeEnd, String timeZone, String blockPath)
    {
        return String.format("INSERT INTO meta_blockindex (tblid,fibervalue,timebegin,timeend,timezone,blockpath) VALUES(%d,%d,%d,%d,'%s','%s');",
                tblId,
                value,
                timeBegin,
                timeEnd,
                timeZone,
                blockPath);
    }

    public static String updateBlockPath(String originPath, String newPath)
    {
        return String.format("UPDATE meta_blockindex SET blockpath='%s' WHERE blockpath='%s'",
                newPath, originPath);
    }

    public static String findTblParamKey(long tblId)
    {
        return String.format("SELECT paramkey FROM meta_tblparammodel WHERE tblid = %d;", tblId);
    }

    public static String deleteTblParam(long tblId)
    {
        return String.format("DELETE FROM meta_tblparammodel WHERE tblid = %d;", tblId);
    }

    public static String findTblPriv(long tblId)
    {
        return String.format("SELECT tblprivid FROM meta_tblprivmodel WHERE tblid = %d;", tblId);
    }

    public static String deleteTblPriv(long tblId)
    {
        return String.format("DELETE FROM meta_tblprivmodel WHERE tblid = %d;", tblId);
    }

    public static String findBlockIndex(long tblId)
    {
        return String.format("SELECT meta_blockindexid FROM meta_blockindex WHERE tblid = %d;", tblId);
    }

    public static String deleteBlockIndex(long tblId)
    {
        return String.format("DELETE FROM meta_blockindex WHERE tblid = %d;", tblId);
    }

    public static String findDbParamKey(long dbId)
    {
        return String.format("SELECT paramkey FROM meta_dbparammodel WHERE dbid = %d;", dbId);
    }

    public static String deleteDbParam(long dbId)
    {
        return String.format("DELETE FROM meta_dbparammodel WHERE dbid = %d;", dbId);
    }

    public static String filterBlockIndexBeginEnd(long tblId, long timeBegin, long timeEnd)
    {
        return String.format("SELECT DISTINCT blockPath FROM meta_blockindex WHERE tblid = %d AND (timeBegin < %d AND timeEnd > %d);", tblId, timeEnd, timeBegin);
    }

    public static String filterBlockIndexBegin(long tblId, long timeBegin)
    {
        return String.format("SELECT DISTINCT blockPath FROM meta_blockindex WHERE tblid = %d AND timeEnd > %d;", tblId, timeBegin);
    }

    public static String filterBlockIndexEnd(long tblId, long timeEnd)
    {
        return String.format("SELECT DISTINCT blockPath FROM meta_blockindex WHERE tblid = %d AND timeBegin < %d;", tblId, timeEnd);
    }

    public static String filterBlockIndex(long tblId)
    {
        return String.format("SELECT DISTINCT blockPath FROM meta_blockindex WHERE tblid = %d;", tblId);
    }

    public static String filterBlockIndexByFiberBeginEnd(long tblId, long value, long timeBegin, long timeEnd)
    {
        return String.format("SELECT DISTINCT blockPath FROM meta_blockindex WHERE tblid = %d AND fiberValue = %d AND (timeBegin < %d AND timeEnd > %d);", tblId, value, timeEnd, timeBegin);
    }

    public static String filterBlockIndexByFiberBegin(long tblId, long value, long timeBegin)
    {
        return String.format("SELECT DISTINCT blockPath FROM meta_blockindex WHERE tblid = %d AND fiberValue = %d AND timeEnd > %d;", tblId, value, timeBegin);
    }

    public static String filterBlockIndexByFiberEnd(long tblId, long value, long timeEnd)
    {
        return String.format("SELECT DISTINCT blockPath FROM meta_blockindex WHERE tblid = %d AND fiberValue = %d AND timeBegin < %d;", tblId, value, timeEnd);
    }

    public static String filterBlockIndexByFiber(long tblId, long value)
    {
        return String.format("SELECT DISTINCT blockPath FROM meta_blockindex WHERE tblid = %d AND fiberValue = %d;", tblId, value);
    }

    public static String createTblFunc(long tblId, long funcId)
    {
        return String.format("INSERT INTO meta_funcmodel (tblid,funcid) VALUES('%d','%d');", tblId, funcId);
    }
}

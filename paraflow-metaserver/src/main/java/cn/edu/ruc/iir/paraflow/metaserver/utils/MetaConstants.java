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

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

public final class MetaConstants
{
    private MetaConstants()
    {}
    public static final int metaTableNum = 11;
    public static final MetaVersion currentVersion = MetaVersion.ONE_ALPHA_ONE;
    public static final StatusProto.ResponseStatus OKStatus = StatusProto.ResponseStatus.newBuilder()
            .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
            .build();

    public static String createVerModelSql = "CREATE TABLE meta_vermodel (vername varchar(50), constraint unique_ver unique(vername));";
    public static String createUserModelSql = "CREATE TABLE meta_usermodel (userid SERIAL primary key, username varchar(50), password varchar(50), createtime bigint, lastvisittime bigint, constraint unique_user unique(username));";
    public static String createDbModelSql = "CREATE TABLE meta_dbmodel (dbid SERIAL primary key, dbname varchar(20), userid int REFERENCES meta_usermodel(userid), locationurl varchar(200), constraint unique_db unique(dbname));";
    public static String createStorageFormatModelSql = "CREATE TABLE meta_storageformatmodel (storageformatid SERIAL primary key, storageformatname varchar(50), compression varchar(50), serialformat varchar(50), constraint unique_storageformat unique(storageformatname));";
    public static String createFuncModelSql = "CREATE TABLE meta_funcmodel (funcid SERIAL primary key, funcname varchar(50), funccontent bytea, constraint unique_func unique(funcname));";
//    public static String createTblFuncModelSql = "CREATE TABLE meta_tblfuncmodel (tblid int REFERENCES meta_tblmodel(tblid),funcid int REFERENCES meta_funcmodel(funcid),constraint unique_storageformat unique(tblid,funcid));";
    public static String createTblModelSql = "CREATE TABLE meta_tblmodel (tblid SERIAL primary key, dbid int REFERENCES meta_dbmodel(dbid), tblname varchar(50), tbltype int, userid int REFERENCES meta_usermodel(userid), createtime bigint, lastaccesstime bigint, locationUrl varchar(100), storageformatid int REFERENCES meta_storageformatmodel(storageformatid), fiberColId int, fiberfuncid int REFERENCES meta_funcmodel(funcid), constraint unique_tbl unique(dbid,tblname));";
    public static String createColModelSql = "CREATE TABLE meta_colmodel (colid SERIAL primary key, colIndex int, dbid int REFERENCES meta_dbmodel(dbid), tblid int REFERENCES meta_tblmodel(tblid), colName varchar(50), colType int, dataType varchar(50), constraint unique_col unique(colIndex,tblid,colName), constraint unique_col2 unique(tblid,colName));";
    public static String createDbParamModelSql = "CREATE TABLE meta_dbparammodel (dbid int REFERENCES meta_dbmodel(dbid), paramkey varchar(100), paramvalue varchar(200), constraint unique_dbparam unique(dbid,paramkey));";
    public static String createTblParamModelSql = "CREATE TABLE meta_tblparammodel (tblid int REFERENCES meta_tblmodel(tblid), paramkey varchar(100), paramvalue varchar(200), constraint unique_tblparam unique(tblid,paramkey));";
    public static String createTblPrivModelSql = "CREATE TABLE meta_tblprivmodel (tblprivid SERIAL primary key, tblid int REFERENCES meta_tblmodel(tblid), userid int REFERENCES meta_usermodel(userid), privtype int, granttime bigint, constraint unique_tblpriv unique(tblid,userid,privtype));";
    public static String createBlockIndexSql = "CREATE TABLE meta_blockindex (meta_blockindexid SERIAL primary key, tblid int REFERENCES meta_tblmodel(tblid), fibervalue int, timebegin bigint, timeend bigint, timezone varchar(50), blockpath varchar(1024));";

    public static String getMetaTablesSql = "SELECT tablename FROM pg_tables WHERE tablename LIKE 'meta_%' ORDER BY tablename;";

    public static String initVerTableSql = String.format("INSERT INTO meta_vermodel (vername) VALUES('%s');", currentVersion.getVersionId());
    public static String initFuncSql = "INSERT INTO meta_funcmodel(funcname, funccontent) VALUES('none','none');";
    public static String getInitVerTableSql = "SELECT vername FROM meta_vermodel;";
    public static String getInitFiberFuncSql = "SELECT funcname FROM meta_funcmodel WHERE funcname='none';";
}

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
    // macros
    public static final int metaTableNum = 9;
    public static final MetaVersion currentVersion = MetaVersion.ONE_ALPHA_ONE;
    public static final StatusProto.ResponseStatus OKStatus = StatusProto.ResponseStatus.newBuilder()
            .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
            .build();
    // default values
    public static final String defaultUserName = "default";
    // create table sql
    public static final String createVerModelSql =
            "CREATE TABLE meta_vermodel (vername varchar(50), constraint unique_ver unique(vername));";
    public static final String createUserModelSql =
            "CREATE TABLE meta_usermodel (userid SERIAL primary key, username varchar(50), password varchar(50), createtime bigint, lastvisittime bigint, constraint unique_user unique(username));";
    public static final String createDbModelSql =
            "CREATE TABLE meta_dbmodel (dbid SERIAL primary key, dbname varchar(20), userid int REFERENCES meta_usermodel(userid), locationurl varchar(200), constraint unique_db unique(dbname));";
    public static final String createTblModelSql =
            "CREATE TABLE meta_tblmodel (tblid SERIAL primary key, dbid int REFERENCES meta_dbmodel(dbid), tblname varchar(50), userid int REFERENCES meta_usermodel(userid), createtime bigint, lastaccesstime bigint, locationUrl varchar(100), storageformat varchar(20), fiberColId int, timeColId int, fiberfunc varchar(500), constraint unique_tbl unique(dbid,tblname));";
    public static final String createColModelSql =
            "CREATE TABLE meta_colmodel (colid SERIAL primary key, colIndex int, dbid int REFERENCES meta_dbmodel(dbid), tblid int REFERENCES meta_tblmodel(tblid), colName varchar(50), colType int, dataType varchar(50), constraint unique_col unique(colIndex,tblid), constraint unique_col2 unique(tblid,colName));";
    private static final String defaultUserPass = "";
    public static String createDbParamModelSql =
            "CREATE TABLE meta_dbparammodel (dbid int REFERENCES meta_dbmodel(dbid), paramkey varchar(100), paramvalue varchar(200), constraint unique_dbparam unique(dbid,paramkey));";
    public static String createTblParamModelSql =
            "CREATE TABLE meta_tblparammodel (tblid int REFERENCES meta_tblmodel(tblid), paramkey varchar(100), paramvalue varchar(200), constraint unique_tblparam unique(tblid,paramkey));";
    public static String createTblPrivModelSql =
            "CREATE TABLE meta_tblprivmodel (tblprivid SERIAL primary key, tblid int REFERENCES meta_tblmodel(tblid), userid int REFERENCES meta_usermodel(userid), privtype int, granttime bigint, constraint unique_tblpriv unique(tblid,userid,privtype));";
    public static String createBlockIndexSql =
            "CREATE TABLE meta_blockindex (meta_blockindexid SERIAL primary key, tblid int REFERENCES meta_tblmodel(tblid), fibervalue int, timebegin bigint, timeend bigint, timezone varchar(50), blockpath varchar(1024));";
    public static String getMetaTablesSql =
            "SELECT tablename FROM pg_tables WHERE tablename LIKE 'meta_%' ORDER BY tablename;";
    // init sql templates
    public static String initVerTableSql =
            String.format("INSERT INTO meta_vermodel(vername) VALUES('%s');", currentVersion.getVersionId());
    public static String initUserTableSql =
            String.format("INSERT INTO meta_usermodel(username, password, createtime, lastvisittime) VALUES('%s', '%s', %d, %d)", defaultUserName, defaultUserPass, System.currentTimeMillis(), System.currentTimeMillis());
    public static String getInitVerTableSql = "SELECT vername FROM meta_vermodel;";

    private MetaConstants()
    {
    }
}

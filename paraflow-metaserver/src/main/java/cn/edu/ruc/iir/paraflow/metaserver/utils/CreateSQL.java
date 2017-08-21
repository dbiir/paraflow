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

/**
 * @author jelly.guodong.jin@gmail.com
 */
public final class CreateSQL
{
    private CreateSQL()
    {
    }
    public static String createVerModelSql = "CREATE TABLE vermodel (verid varchar(50),constraint unique_ver unique(verid));";
    public static String createUserModelSql = "CREATE TABLE usermodel (userid SERIAL primary key,username varchar(50),password varchar(50),createtime bigint,lastvisittime bigint,constraint unique_user unique(username));";
    public static String createDbModelSql = "CREATE TABLE dbmodel (dbid SERIAL primary key,dbname varchar(20),userid int REFERENCES usermodel(userid),locationurl varchar(200),constraint unique_db unique(dbname));";
    public static String createTblModelSql = "CREATE TABLE tblmodel (tblid SERIAL primary key,dbid int REFERENCES dbmodel(dbid),tblname varchar(50),tbltype int,userid int REFERENCES usermodel(userid),createtime bigint,lastaccesstime bigint,locationUrl varchar(100),storageformatid int,fiberColId int,fiberfuncid int,constraint unique_tbl unique(dbid,tblname));";
    public static String createColModelSql = "CREATE TABLE colmodel (colid SERIAL primary key,colIndex int,dbid int REFERENCES dbmodel(dbid),tblid int REFERENCES tblmodel(tblid),colName varchar(50),colType varchar(50),dataType varchar(50),constraint unique_col unique(colIndex,tblid,colName),constraint unique_col unique(tblid,colName));";
    public static String createDbParamModelSql = "CREATE TABLE dbparammodel (dbid int REFERENCES dbmodel(dbid),paramkey varchar(100),paramvalue varchar(200),constraint unique_dbparam unique(dbid,paramkey));";
    public static String createTblParamModelSql = "CREATE TABLE tblparammodel (tblid int REFERENCES tblmodel(tblid),paramkey varchar(100),paramvalue varchar(200),constraint unique_tblparam unique(tblid,paramkey));";
    public static String createTblPrivModelSql = "CREATE TABLE tblprivmodel (tblprivid SERIAL primary key,tblid int REFERENCES tblmodel(tblid),userid int REFERENCES usermodel(userid),privtype int,granttime bigint,constraint unique_tblpriv unique(tblid,userid,privtype));";
    public static String createStorageFormatModelSql = "CREATE TABLE storageformatmodel (storageformatid SERIAL primary key,storageformatname varchar(50),compression varchar(50),serialformat varchar(50),constraint unique_storageformat unique(storageformatname));";
    public static String createFiberFuncModelSql = "CREATE TABLE fiberfuncmodel (fiberfuncid SERIAL primary key,fiberfuncname varchar(50),fiberfunccontent bytea,constraint unique_fiberfunc unique(fiberfuncname));";
    public static String createBlockIndexSql = "CREATE TABLE blockindex (blockindexid SERIAL primary key,tblid int REFERENCES tblmodel(tblid),fibervalue int,timebegin bigint,timeend bigint,timezone varchar(50),blockpath varchar(100));";
}

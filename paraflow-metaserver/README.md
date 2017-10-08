 #### 1. VerModel (metadata version corresponding to MetaServer)
 | NAME              | TYPE             | COMMENT               |
 |------------------|------------------|-----------------------|
 | verName          | string            | version of MetaServer|
 
 #### 2. UserModel
 | NAME           | TYPE             | COMMENT                                               |
 |----------------|------------------|-------------------------------------------------------|
 | userId         | int64            | all recorded users //Auto-add Primary Key             |   
 | userName       | string           | user name                                   ///unique |
 | createTime     | int64            | creation time                                         |
 | lastVisitTime  | int64            | last visit time                                       |
 
 #### 3. DbModel (database information)
 | NAME           | TYPE             | COMMENT                                                |
 |----------------|------------------|--------------------------------------------------------|
 | dbId           | int64            | unique database id //Auto-add Primary Key              |   
 | dbName         | string           | unique database name                        ///unique  |
 | userId         | int64            | owner id //Forign Key：UserModel：userId                |
 | locationUrl    | string           | path in filesystem                                     |
 
 #### 4. TblModel (table information)
 | NAME           | TYPE             | COMMENT                                                            |
 |----------------|------------------|--------------------------------------------------------------------|
 | tblId          | int64            | unique table id //Auto-add Primary Key                             | 
 | dbId           | int64            | database belonging to //Forign Key：DbModel：dbId    ///unique      |   
 | tblName        | string           | table name                                          ///unique      |
 | tblType        | int32            | 0 represents regular, 1 represents fiber                           |
 | userId         | int64            | user_id is owner's  //Forign Key：UserModel：userId                 |  
 | createTime     | int64            | timestamp stored as long                                           |
 | lastAccessTime | int64            | timestamp stored as long                                           |
 | locationUrl    | string           | path in filesystem                                                 |
 | storageFormatId| int32            |            //Forign Key：StorageFormatModel：StorageFormatId        |
 | fiberColId     | int64            | -1 means no fiber column                                           |
 | fiberFuncId    | int64            | table partition function //Forign Key：FiberFuncModel：FiberFuncId  |
 
 #### 5. ColModel (column information)
 | NAME           | TYPE             | COMMENT                                                   |
 |----------------|------------------|-----------------------------------------------------------|
 | colId          | int64            |//Auto-add Primary Key                                     |   
 | colIndex       | int32            | index of column in table                      ///unique   |
 | dbId           | int64            | database id //Forign Key：DbModel：dbId                    |   
 | tblId          | int64            | table id //Forign Key：TblModel：tblId         ///unique   |   
 | colName        | string           | column name                                   ///unique   |
 | colType        | string           | column type: regular \| fiber \| timestamp                |
 | dataType       | string           | data type: integer \| char(x) \| float                    |
 
 #### 6. DbParamModel (user specified parameters related to database) [optional]
 | NAME           | TYPE             | COMMENT                                                        |
 |----------------|------------------|----------------------------------------------------------------|
 | dbId           | int64            | referenced database id //Forign Key：DbModel：dbId    ///unique |   
 | paramKey       | string           | key as string                                        ///unique |
 | paramValue     | string           | value as string                                                |
 
 #### [useless]__7. DbPrivsModel(database privileges)__       
 | NAME            | TYPE             | COMMENT                                                 |
 |-----------------|------------------|---------------------------------------------------------|
 | dbPrivId        | int64            | unique sequential id //Auto-add Primary Key             |
 | dbId            | int64            | database id                                   ///unique |
 | userId          | int64            | user id                                       ///unique |
 | privType        | int32            |                                               ///unique |
 | grantTime       | int64            | grant time                                              |
 
 Available privilege types:    
 1: read, 2: write, 3: read + write   [useless]__5: read + grant read to other__
 
 #### 8. TblParamModel (user specified parameters related to table) [optional]
 | NAME           | TYPE             | COMMENT                                                  |
 |----------------|------------------|----------------------------------------------------------|
 | tblId          | int64            | unique table id //Forign Key：TblModel：tblId   ///unique | 
 | paramKey       | string           | key as string                                  ///unique |
 | paramValue     | string           | value as string                                          |
 
 #### 9. TblPrivModel(table privileges)
 | NAME           | TYPE             | COMMENT                                    |
 |----------------|------------------|--------------------------------------------|
 | tblPrivId      | int64            |//Auto-add Primary Key                      |   
 | tblId          | int64            |//Forign Key：TblModel：tblId      ///unique |   
 | userId         | int64            |//Forign Key：UserModel：userId    ///unique |   
 | privType       | int32            |                                  ///unique |
 | grantTime      | int64            | grant time                                 |
 
 #### 10. StorageFormatModel (storage format information)
 | NAME             | TYPE             | COMMENT                                    |
 |------------------|------------------|--------------------------------------------|
 | storageFormatId  | int32            |//Auto-add Primary Key                      |
 | storageFormatName| string           |                                  ///unique |
 | compression      | string           | uncompressed \| snappy \| etc              |
 | serialFormat     | string           | serial class name                          |
 
 #### 11. FuncModel
 | NAME               | TYPE             | COMMENT                                         |
 |----------------    |------------------|-------------------------------------------------|
 | funcId             | int64            | function id  //Auto-add Primary Key             |
 | funcName           | string           | function name                         ///unique |
 | funcContent        | bytes            | function template id                            |
 
 #### 13. BlockIndex
 | NAME            | TYPE             | COMMENT                               |
 |-----------------|------------------|---------------------------------------|
 | blockIndexId    | int64            | block id //Auto-add Primary Key       |   
 | tblId           | int64            | table id //Forign Key：TblModel：tblId |   
 | fiberValue      | int64            | fiber value                           |
 | timeBegin       | int64            | block begin timestamp                 |
 | timeEnd         | int64            | block end timestamp                   |
 | timeZone        | string           | time zone                             |
 | blockPath       | string           | block file path                       |

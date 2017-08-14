 #### 1. VerModel (metadata version corresponding to MetaServer)
 | NAME           | TYPE             | COMMENT              |
 |----------------|------------------|----------------------|
 | verId          | int32            | version of MetaServer
 
 #### 2. DbModel (database information)
 | NAME           | TYPE             | COMMENT              |
 |----------------|------------------|----------------------|
 | dbId           | int64            | unique database id   |   //自增主键
 | dbName         | string           | unique database name |
 | userId         | int64            | owner        id      |   //外键UserModel：userId
 | locationUrl    | string           | path in filesystem   |
 
 #### 3. DbParamModel (user specified parameters related to database) [optional]
 | NAME           | TYPE             | COMMENT               |
 |----------------|------------------|-----------------------|
 | dbId           | int64            | referenced database id|   //外键DbModel：dbId
 | paramKey       | string           | key as string         |
 | paramValue     | string           | value as string       |
 
 [useless]#### __4. DbPrivsModel(database privileges)__       
 | NAME            | TYPE             | COMMENT               |
 |-----------------|------------------|-----------------------|
 | dbPrivId        | int64            | unique sequential id  |   //自增主键
 | dbId            | int64            | database id           |
 | userId          | int64            | user id               |
 | privType        | int32            |                       |
 | grantTime       | int64            | grant time            |
 
 Available privilege types:    
 1: read, 2: write, 3: read + write   [useless]__5: read + grant read to other__
 
 #### 5. TblModel (table information)
 | NAME           | TYPE             | COMMENT                                 |
 |----------------|------------------|-----------------------------------------|
 | tblId          | int64            | unique table id                         |   //自增主键
 | dbId           | int64            | database belonging to                   |   //外键DbModel：dbId
 | tblName        | string           | table name                              |
 | tblType        | int32            | 0 represents regular, 1 represents fiber|
 | userId         | int64            | user_id is owner's                      |   //外键UserModel：userId
 | createTime     | int64            | timestamp stored as long                |
 | lastAccessTime | int64            | timestamp stored as long                |
 | locationUrl    | string           | path in filesystem                      |
 | storageFormatId| int32            |                                         |
 | fiberColId     | int64            | -1 means no fiber column                |
 | fiberFuncId    | int64            | table partition function                |
 
 #### 6. TblParamModel (user specified parameters related to table) [optional]
 | NAME           | TYPE             | COMMENT              |
 |----------------|------------------|----------------------|
 | tblId          | int64            | unique table id      |   //外键TblModel：tblId
 | paramKey       | string           | key as string        |
 | paramValue     | string           | value as string      |
 
 #### 7. TblPrivModel(table privileges)
 | NAME           | TYPE             | COMMENT               |
 |----------------|------------------|-----------------------|
 | tblPrivId      | int64            |                       |   //自增主键
 | tblId          | int64            |                       |   //外键TblModel：tblId
 | userId         | int64            |                       |   //外键UserModel：userId
 | privType       | int32            |                       |
 | grantTime      | int64            | grant time            |
 
 #### 8. StorageFormatModel (storage format information)
 | NAME             | TYPE             | COMMENT                      |
 |------------------|------------------|------------------------------|
 | storageFormatId  | int32            |                              |   //自增主键
 | storageFormatName| string           |                              |
 | compression      | string           | uncompressed \| snappy \| etc|
 | serialFormat     | string           | serial class name            |

 #### 9. ColModel (column information)
 | NAME           | TYPE             | COMMENT                                   |
 |----------------|------------------|-------------------------------------------|
 | colId          | int64            |                                           |   //自增主键
 | colIndex       | int32            | index of column in table                  |
 | dbId           | int64            | database id                               |   //外键DbModel：dbId
 | tblId          | int64            | table id                                  |   //外键UserModel：userId
 | colName        | string           | column name                               |
 | colType        | string           | column type: regular \| fiber \| timestamp|
 | dataType       | string           | data type: integer \| char(x) \| float    |
 
 #### 10. FiberFuncModel
 | NAME               | TYPE             | COMMENT              |
 |----------------    |------------------|----------------------|
 | fiberFuncId        | int64            | function id          |   //自增主键
 | fiberFuncName      | string           | function name        |
 | fiberFuncContent   | bytes            | function template id |
 
 #### 11. BlockIndex
 | NAME            | TYPE             | COMMENT              |
 |-----------------|------------------|----------------------|
 | blockIndexId    | int64            | block id             |   //自增主键
 | tblId           | int64            | table id             |   //外键TblModel：tblId
 | fiberValue      | int64            | fiber value          |
 | timeBegin       | int64            | block begin timestamp|
 | timeEnd         | int64            | block end timestamp  |
 | timeZone        | string           | time zone            |
 | blockPath       | string           | block file path      |
 
 #### 12. UserModel
 | NAME           | TYPE             | COMMENT               |
 |----------------|------------------|-----------------------|
 | userId         | int64            | all recorded users    |   //自增主键
 | userName       | string           | user name             |
 | createTime     | int64            | creation time         |
 | lastVisitTime  | int64            | last visit time       |

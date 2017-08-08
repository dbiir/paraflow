### MetaServer Models
#### 1. VERSION (metadata version corresponding to MetaServer)
| NAME           | TYPE             | COMMENT              |
|----------------|------------------|----------------------|
| VER_ID         | INT              | version of MetaServer|

#### 2. DBS (database information)
| NAME           | TYPE             | COMMENT              |
|----------------|------------------|----------------------|
| DB_ID          | BIGINT           | unique database id   |
| DB_NAME        | VARCHAR(128)     | unique database name |
| LOCATION_URI   | VARCHAR(768)     | path in filesystem   |
| USER_ID        | BIGINT           | owner        id      |

#### 3. DB_PARAMS (user specified parameters related to database) [optional]
| NAME           | TYPE             | COMMENT               |
|----------------|------------------|-----------------------|
| DB_ID          | BIGINT           | referenced database id|
| PARAM_KEY      | VARCHAR(128)     | key as string         |
| PARAM_VALUE    | VARCHAR(256)     | value as string       |

#### 4. DB_PRIVS(database privileges)
| NAME            | TYPE             | COMMENT               |
|-----------------|------------------|-----------------------|
| DB_PRIV_ID      | BIGINT           | unique sequential id  |
| GRANT_TIME      | TIMESTAMP        | grant time            |
| USER_ID         | BIGINT           | user id               |
| PRIV_TYPE       | INT              |                       |
| DB_ID           | BIGINT           | database id           |

Available privilege types:    
1: read, 2: write, 3: read + write, 5: read + grant read to other,
6: write + grant write to other, 7: read + write + grant read/write to other.
(4: grant current privilege to other users)

#### 5. TBLS (table information)
| NAME           | TYPE             | COMMENT                                 |
|----------------|------------------|-----------------------------------------|
| TBL_ID         | BIGINT           | unique table id                         |
| DB_ID          | BIGINT           | database belonging to                   |
| CREATE_TIME    | BIGINT           | timestamp stored as long                |
| LAST_ACCESS_TIME| BIGINT          | timestamp stored as long                |
| OWNER_ID       | BIGINT           | owner's user_id                         |
| TBL_NAME       | VARCHAR(128)     | table name                              |
| TBL_TYPE       | INT              | 0 represents regular, 1 represents fiber|
| FIBER_COL_ID   | BIGINT           | -1 means no fiber column                |
| LOCATION_URI   | VARCHAR(1024)    | path in filesystem                      |
| STORAGE_FORMAT | INT              |                                         |
| FIBER_FUNC_ID  | BIGINT           | table partition function                |

#### 6. TBL_PARAMS (user specified parameters related to table) [optional]
| NAME           | TYPE             | COMMENT              |
|----------------|------------------|----------------------|
| TBL_ID         | BIGINT           | unique table id      |
| PARAM_KEY      | VARCHAR(128)     | key as string        |
| PARAM_VALUE    | VARCHAR(128)     | value as string      |

#### 7. TBL_PRIVS(table privileges)
| NAME           | TYPE             | COMMENT               |
|----------------|------------------|-----------------------|
| TBL_PRIV_ID    | BIGINT           |                       |
| GRANT_TIME     | TIMESTAMP        | grant time            |
| ROLE_ID        | BIGINT           |                       |
| PRIV_TYPE      | INT              |                       |
| TBL_ID         | BIGINT           |                       |

#### 8. SFS (storage format information)
| NAME           | TYPE             | COMMENT                      |
|----------------|------------------|------------------------------|
| SF_ID          | INT              |                              |
| COMPRESSION    | VARCHAR(20)      | uncompressed \| snappy \| etc|
| SERIAL_FORMAT  | VARCHAR(512)     | serial class name            |

#### 9. COLS (column information)
| NAME           | TYPE             | COMMENT                                   |
|----------------|------------------|-------------------------------------------|
| COL_ID         | BIGINT           |                                           |
| TBL_ID         | BIGINT           | table id                                  |
| COL_NAME       | VARCHAR(128)     | column name                               |
| COL_TYPE       | VARCHAR(10)      | column type: regular \| fiber \| timestamp|
| DATA_TYPE      | VARCHAR(20)      | data type: integer \| char(x) \| float    |
| COL_INDEX      | INT              | index of column in table                  |

#### 10. FIBER_FUNCS
| NAME               | TYPE             | COMMENT              |
|----------------    |------------------|----------------------|
| FIBER_FUNC_ID      | BIGINT           | function id          |
| FIBER_FUNC_NAME    | VARCHAR(20)      | function name        |
| FIBER_FUNC_CONTENT | BLOB             | function template id |

#### 11. BLOCK_INDEX
| NAME            | TYPE             | COMMENT              |
|-----------------|------------------|----------------------|
| BLOCK_INDEX_ID  | BIGINT           | block id             |
| TABLE_ID        | BIGINT           | table id             |
| FIBER_VALUE     | BIGINT           | fiber value          |
| TIME_BEGIN      | BIGINT           | block begin timestamp|
| TIME_END        | BIGINT           | block end timestamp  |
| TIME_ZONE       | VARCHAR(20)      | time zone            |
| BLOCK_PATH      | VARCHAR(512)     | block file path      |

#### 12. USERS
| NAME           | TYPE             | COMMENT               |
|----------------|------------------|-----------------------|
| USER_ID        | BIGINT           | all recorded users    |
| CREATE_TIME    | TIMESTAMP        | creation time         |
| USER_NAME      | VARCHAR(128)     | user name             |
| LAST_VISIT_TIME| TIMESTAMP        | last visit time       |

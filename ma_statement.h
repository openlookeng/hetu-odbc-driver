/************************************************************************************
   Copyright (C) 2013, 2019 MariaDB Corporation AB
   Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
   
   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.
   
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.
   
   You should have received a copy of the GNU Library General Public
   License along with this library; if not see <http://www.gnu.org/licenses>
   or write to the Free Software Foundation, Inc., 
   51 Franklin St., Fifth Floor, Boston, MA 02110, USA
*************************************************************************************/
#ifndef _ma_statement_h_
#define _ma_statement_h_

#define MADB_FETCH_TYPE_FETCH       1
#define MADB_FETCH_TYPE_BULK        2
#define MADB_FETCH_TYPE_SETPOS      3


struct st_ma_stmt_methods
{
  SQLRETURN (*Prepare)(MADB_Stmt *Stmt, char *StatementText, SQLINTEGER TextLength, BOOL ExecDirect);
  SQLRETURN (*Execute)(MADB_Stmt *Stmt, BOOL ExecDirect);
  SQLRETURN (*Fetch)(MADB_Stmt *Stmt);
  SQLRETURN (*BindColumn)(MADB_Stmt *Stmt, SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
                     SQLPOINTER TargetValuePtr, SQLLEN BufferLength, SQLLEN *StrLen_or_Ind);
  SQLRETURN (*BindParam)(MADB_Stmt *Stmt,  SQLUSMALLINT ParameterNumber, SQLSMALLINT InputOutputType, SQLSMALLINT ValueType,
                        SQLSMALLINT ParameterType, SQLULEN ColumnSize, SQLSMALLINT DecimalDigits, SQLPOINTER ParameterValuePtr,
                        SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr);
  SQLRETURN (*ExecDirect)(MADB_Stmt *Stmt, char *StatementText, SQLINTEGER TextLength);
  SQLRETURN (*GetData)(SQLHSTMT StatementHandle, SQLUSMALLINT Col_or_Param_Num, SQLSMALLINT TargetType,
                       SQLPOINTER TargetValuePtr, SQLLEN BufferLength, SQLLEN * StrLen_or_IndPtr, BOOL InternalUse);
  SQLRETURN (*RowCount)(MADB_Stmt *Stmt, SQLLEN *RowCountPtr);
  SQLRETURN (*ParamCount)(MADB_Stmt *Stmt, SQLSMALLINT *ParamCountPtr);
  SQLRETURN (*ColumnCount)(MADB_Stmt *Stmt, SQLSMALLINT *ColumnCountPtr);
  SQLRETURN (*GetAttr)(MADB_Stmt *Stmt, SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER BufferLength,
                       SQLINTEGER *StringLength);
  SQLRETURN (*SetAttr)(MADB_Stmt *Stmt, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength);
  SQLRETURN (*StmtFree)(MADB_Stmt *Stmt, SQLUSMALLINT Option);
  SQLRETURN (*ColAttribute)(MADB_Stmt *Stmt, SQLUSMALLINT ColumnNumber, SQLUSMALLINT FieldIdentifier, SQLPOINTER CharacterAttributePtr,
             SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr, SQLLEN *NumericAttributePtr,
             my_bool isWchar);
  SQLRETURN (*ColumnPrivileges)(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                                char *SchemaName, SQLSMALLINT NameLength2, char *TableName,
                                SQLSMALLINT NameLength3, char *ColumnName, SQLSMALLINT NameLength4);
  SQLRETURN (*TablePrivileges)(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                               char *SchemaName, SQLSMALLINT NameLength2,
                               char *TableName, SQLSMALLINT NameLength3);
  SQLRETURN (*Tables)(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                      char *SchemaName, SQLSMALLINT NameLength2, char *TableName,
                      SQLSMALLINT NameLength3, char *TableType, SQLSMALLINT NameLength4);
  SQLRETURN (*Statistics)(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                          char *SchemaName, SQLSMALLINT NameLength2,
                          char *TableName, SQLSMALLINT NameLength3,
                          SQLUSMALLINT Unique, SQLUSMALLINT Reserved);
  SQLRETURN (*Columns)(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                       char *SchemaName, SQLSMALLINT NameLength2,
                       char *TableName, SQLSMALLINT NameLength3,
                       char *ColumnName, SQLSMALLINT NameLength4);
  SQLRETURN (*ProcedureColumns)(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                                char *SchemaName, SQLSMALLINT NameLength2, char *ProcName,
                                SQLSMALLINT NameLength3, char *ColumnName, SQLSMALLINT NameLength4);
  SQLRETURN (*PrimaryKeys)(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                               char *SchemaName, SQLSMALLINT NameLength2, char *TableName,
                               SQLSMALLINT NameLength3);
  SQLRETURN (*SpecialColumns)(MADB_Stmt *Stmt, SQLUSMALLINT IdentifierType,
                              char *CatalogName, SQLSMALLINT NameLength1, 
                              char *SchemaName, SQLSMALLINT NameLength2,
                              char *TableName, SQLSMALLINT NameLength3,
                              SQLUSMALLINT Scope, SQLUSMALLINT Nullable);
  SQLRETURN (*Procedures)(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                          char *SchemaName, SQLSMALLINT NameLength2, char *ProcName,
                          SQLSMALLINT NameLength3);
  SQLRETURN (*ForeignKeys)(MADB_Stmt *Stmt, char *PKCatalogName, SQLSMALLINT NameLength1,
                           char *PKSchemaName, SQLSMALLINT NameLength2, char *PKTableName,
                           SQLSMALLINT NameLength3, char *FKCatalogName, SQLSMALLINT NameLength4,
                           char *FKSchemaName, SQLSMALLINT NameLength5,  char *FKTableName,
                           SQLSMALLINT NameLength6);
  SQLRETURN (*DescribeCol)(MADB_Stmt *Stmt, SQLUSMALLINT ColumnNumber, void *ColumnName,
                         SQLSMALLINT BufferLength, SQLSMALLINT *NameLengthPtr,
                         SQLSMALLINT *DataTypePtr, SQLULEN *ColumnSizePtr, SQLSMALLINT *DecimalDigitsPtr,
                         SQLSMALLINT *NullablePtr, my_bool isWChar);
  SQLRETURN (*SetCursorName)(MADB_Stmt *Stmt, char *Buffer, SQLINTEGER BufferLength);
  SQLRETURN (*GetCursorName)(MADB_Stmt *Stmt, void *CursorName, SQLSMALLINT BufferLength, 
                             SQLSMALLINT *NameLengthPtr, my_bool isWChar);
  SQLRETURN (*SetPos)(MADB_Stmt *Stmt, SQLSETPOSIROW Row, SQLUSMALLINT Operation,
                      SQLUSMALLINT LockType, int ArrayOffset);
  SQLRETURN (*FetchScroll)(MADB_Stmt *Stmt, SQLSMALLINT FetchOrientation,
                           SQLLEN FetchOffset);
  SQLRETURN (*ParamData)(MADB_Stmt *Stmt, SQLPOINTER *ValuePtrPtr);
  SQLRETURN (*PutData)(MADB_Stmt *Stmt, SQLPOINTER DataPtr, SQLLEN StrLen_or_Ind);
  SQLRETURN (*BulkOperations)(MADB_Stmt *Stmt, SQLSMALLINT Operation);
  SQLRETURN (*RefreshDynamicCursor)(MADB_Stmt *Stmt);
  SQLRETURN (*RefreshRowPtrs)(MADB_Stmt *Stmt);
  SQLRETURN (*GetOutParams)(MADB_Stmt *Stmt, int CurrentOffset);
};

SQLRETURN    MADB_StmtInit          (MADB_Dbc *Connection, SQLHANDLE *pHStmt);
SQLUSMALLINT MapColAttributeDescType(SQLUSMALLINT FieldIdentifier);
MYSQL_RES*   FetchMetadata          (MADB_Stmt *Stmt);
SQLRETURN    MADB_DoExecute(MADB_Stmt *Stmt, BOOL ExecDirect);

#define MADB_MAX_CURSOR_NAME 64 * 3 + 1
#define MADB_CHECK_STMT_HANDLE(a,b)\
  if (!(a) || !(a)->b)\
    return SQL_INVALID_HANDLE

#define MADB_STMT_COLUMN_COUNT(aStmt) (aStmt)->Ird->Header.Count
#define MADB_RESET_COLUMT_COUNT(aStmt) (aStmt)->Ird->Header.Count= 0
#define MADB_STMT_PARAM_COUNT(aStmt)  (aStmt)->ParamCount
#define MADB_POSITIONED_COMMAND(aStmt) ((aStmt)->PositionedCommand && (aStmt)->PositionedCursor)
/* So far we always use all fields for index. Once that is changed, this should be changed as well */
#define MADB_POS_COMM_IDX_FIELD_COUNT(aStmt) MADB_STMT_COLUMN_COUNT((aStmt)->PositionedCursor)
#define MADB_STMT_FORGET_NEXT_POS(aStmt) (aStmt)->Cursor.Next= NULL
#define MADB_STMT_RESET_CURSOR(aStmt) (aStmt)->Cursor.Position= -1; MADB_STMT_FORGET_NEXT_POS(aStmt)
#define MADB_STMT_CLOSE_STMT(aStmt)   mysql_stmt_close((aStmt)->stmt);(aStmt)->stmt= NULL

#define MADB_OCTETS_PER_CHAR 2

#define MADB_TRANSFER_OCTET_LENGTH(TYPE_DEF_COL_NAME)\
  "@tol:=CAST(CASE @dt"\
  "  WHEN " XSTR(SQL_BIT) " THEN 1 "\
  "  WHEN " XSTR(SQL_TINYINT) " THEN 1 "\
  "  WHEN " XSTR(SQL_SMALLINT) " THEN 2 "\
  "  WHEN " XSTR(SQL_INTEGER) " THEN IF(DATA_TYPE='mediumint',3,4) "\
  "  WHEN " XSTR(SQL_BIGINT) " THEN 20 "\
  "  WHEN " XSTR(SQL_REAL) " THEN 4 "\
  "  WHEN " XSTR(SQL_FLOAT) " THEN 8 "\
  "  WHEN " XSTR(SQL_DOUBLE) " THEN 8 "\
  "  WHEN " XSTR(SQL_DATE) " THEN 6 "\
  "  WHEN " XSTR(SQL_TYPE_DATE) " THEN 6 "\
  "  WHEN " XSTR(SQL_TIME) " THEN 6 "\
  "  WHEN " XSTR(SQL_TYPE_TIME) " THEN 6 "\
  "  WHEN " XSTR(SQL_TIMESTAMP) " THEN 16 "\
  "  WHEN " XSTR(SQL_TYPE_TIMESTAMP) " THEN 16 "\
  "  WHEN " XSTR(SQL_GUID) " THEN 16 "\
  "  WHEN " XSTR(SQL_DECIMAL) " THEN @ColSize + IF(" #TYPE_DEF_COL_NAME " LIKE '%%unsigned',1,2) "\
  "  WHEN " XSTR(SQL_BINARY) " THEN IF(DATA_TYPE='bit', CAST(((NUMERIC_PRECISION + 7) / 8) AS SIGNED), CHARACTER_OCTET_LENGTH) "\
  "  WHEN " XSTR(SQL_VARBINARY) " THEN CHARACTER_OCTET_LENGTH "\
  "  WHEN " XSTR(SQL_LONGVARBINARY) " THEN CHARACTER_OCTET_LENGTH "\
  "  ELSE CHARACTER_MAXIMUM_LENGTH*%u "\
  "END AS SIGNED)"

/* CASE for DATA_TYPE glued in 2 parts for ODBC v2 or v3 */
#define MADB_SQL_DATATYPEp1\
  "CASE DATA_TYPE"\
  "  WHEN 'bit' THEN @dt:= IF(NUMERIC_PRECISION=1," XSTR(SQL_BIT) ", " XSTR(SQL_BINARY) ")"\
  "  WHEN 'tinyint' THEN @dt:=" XSTR(SQL_TINYINT)\
  "  WHEN 'smallint' THEN @dt:=" XSTR(SQL_SMALLINT)\
  "  WHEN 'year' THEN @dt:= " XSTR(SQL_SMALLINT)\
  "  WHEN 'mediumint' THEN @dt:=" XSTR(SQL_INTEGER)\
  "  WHEN 'int' THEN @dt:=" XSTR(SQL_INTEGER)\
  "  WHEN 'bigint' THEN @dt:=" XSTR(SQL_BIGINT)\
  "  WHEN 'blob' THEN @dt:=" XSTR(SQL_LONGVARBINARY)\
  "  WHEN 'tinyblob' THEN @dt:=" XSTR(SQL_LONGVARBINARY)\
  "  WHEN 'mediumblob' THEN @dt:=" XSTR(SQL_LONGVARBINARY)\
  "  WHEN 'longblob' THEN @dt:=" XSTR(SQL_LONGVARBINARY)\
  "  WHEN 'decimal' THEN @dt:=" XSTR(SQL_DECIMAL)\
  "  WHEN 'float' THEN @dt:=IF(NUMERIC_SCALE IS NULL," XSTR(SQL_REAL) ", "  XSTR(SQL_DECIMAL) ")"\
  "  WHEN 'double' THEN @dt:=IF(NUMERIC_SCALE IS NULL," XSTR(SQL_DOUBLE) ", "  XSTR(SQL_DECIMAL) ")"\
  "  WHEN 'binary' THEN @dt:=" XSTR(SQL_BINARY)\
  "  WHEN 'varbinary' THEN @dt:=" XSTR(SQL_VARBINARY)

#define MADB_SQL_DATATYPEp1U\
  "  WHEN 'text' THEN @dt:=" XSTR(SQL_WLONGVARCHAR)\
  "  WHEN 'tinytext' THEN @dt:=" XSTR(SQL_WLONGVARCHAR)\
  "  WHEN 'mediumtext' THEN @dt:=" XSTR(SQL_WLONGVARCHAR)\
  "  WHEN 'longtext' THEN @dt:=" XSTR(SQL_WLONGVARCHAR)\
  "  WHEN 'char' THEN @dt:=" XSTR(SQL_WCHAR)\
  "  WHEN 'enum' THEN @dt:=" XSTR(SQL_WCHAR)\
  "  WHEN 'set' THEN @dt:=" XSTR(SQL_WCHAR)\
  "  WHEN 'varchar' THEN @dt:=" XSTR(SQL_WVARCHAR)

#define MADB_SQL_DATATYPEp1A\
  "  WHEN 'text' THEN @dt:=" XSTR(SQL_LONGVARCHAR)\
  "  WHEN 'tinytext' THEN @dt:=" XSTR(SQL_LONGVARCHAR)\
  "  WHEN 'mediumtext' THEN @dt:=" XSTR(SQL_LONGVARCHAR)\
  "  WHEN 'longtext' THEN @dt:=" XSTR(SQL_LONGVARCHAR)\
  "  WHEN 'char' THEN @dt:=" XSTR(SQL_CHAR)\
  "  WHEN 'enum' THEN @dt:=" XSTR(SQL_CHAR)\
  "  WHEN 'set' THEN @dt:=" XSTR(SQL_CHAR)\
  "  WHEN 'varchar' THEN @dt:=" XSTR(SQL_VARCHAR)

#define MADB_SQL_DATATYPEp2_ODBC3\
  "  WHEN 'date' THEN @dt:=" XSTR(SQL_TYPE_DATE)\
  "  WHEN 'time' THEN @dt:=" XSTR(SQL_TYPE_TIME)\
  "  WHEN 'datetime' THEN @dt:=" XSTR(SQL_TYPE_TIMESTAMP)\
  "  WHEN 'timestamp' THEN @dt:=" XSTR(SQL_TYPE_TIMESTAMP)\
  "  ELSE @dt:=" XSTR(SQL_LONGVARBINARY)\
  "END AS DATA_TYPE"

#define MADB_SQL_DATATYPEp2_ODBC2\
  "  WHEN 'date' THEN @dt:=" XSTR(SQL_DATE)\
  "  WHEN 'time' THEN @dt:=" XSTR(SQL_TIME)\
  "  WHEN 'datetime' THEN @dt:=" XSTR(SQL_TIMESTAMP)\
  "  WHEN 'timestamp' THEN @dt:=" XSTR(SQL_TIMESTAMP)\
  "  ELSE @dt:=" XSTR(SQL_LONGVARBINARY)\
  "END AS DATA_TYPE"

#define MADB_SQL_DATATYPE_ODBC3U MADB_SQL_DATATYPEp1 MADB_SQL_DATATYPEp1U MADB_SQL_DATATYPEp2_ODBC3
#define MADB_SQL_DATATYPE_ODBC3A MADB_SQL_DATATYPEp1 MADB_SQL_DATATYPEp1A MADB_SQL_DATATYPEp2_ODBC3
#define MADB_SQL_DATATYPE_ODBC2U MADB_SQL_DATATYPEp1 MADB_SQL_DATATYPEp1U MADB_SQL_DATATYPEp2_ODBC2
#define MADB_SQL_DATATYPE_ODBC2A MADB_SQL_DATATYPEp1 MADB_SQL_DATATYPEp1A MADB_SQL_DATATYPEp2_ODBC2

#define MADB_SQL_DATATYPE(StmtHndl) (StmtHndl->Connection->Environment->OdbcVersion >= SQL_OV_ODBC3 ?\
 (StmtHndl->Connection->IsAnsi ? MADB_SQL_DATATYPE_ODBC3A : MADB_SQL_DATATYPE_ODBC3U) :\
 (StmtHndl->Connection->IsAnsi ? MADB_SQL_DATATYPE_ODBC2A : MADB_SQL_DATATYPE_ODBC2U))

/************** End of DATA_TYPE *************/

/************** SQLColumns       *************/
#define MADB_COLUMN_SIZE\
    "CAST(CASE" \
    "  WHEN DATA_TYPE = 'bit' THEN @ColSize:=((NUMERIC_PRECISION + 7) / 8) "\
    "  WHEN DATA_TYPE in ('tinyint', 'smallint', 'mediumint', 'int',"\
                         "'bigint', 'decimal') THEN @ColSize:=NUMERIC_PRECISION "\
    "  WHEN DATA_TYPE = 'float' THEN if(NUMERIC_SCALE IS NULL, @ColSize:=7, @ColSize:=NUMERIC_PRECISION)"\
    "  WHEN DATA_TYPE = 'double' THEN if(NUMERIC_SCALE IS NULL, @ColSize:=15, @ColSize:=NUMERIC_PRECISION)"\
    "  WHEN DATA_TYPE = 'date' THEN @ColSize:=10"\
    "  WHEN DATA_TYPE = 'time' THEN @ColSize:=8"\
    "  WHEN DATA_TYPE = 'year' THEN @ColSize:=4"\
    "  WHEN DATA_TYPE in ('timestamp', 'datetime') THEN @ColSize:=19 "\
    "  ELSE @ColSize:=CHARACTER_MAXIMUM_LENGTH "\
  "END AS UNSIGNED)"

#define MADB_CATALOG_COLUMNSp1 "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, "
#define MADB_CATALOG_COLUMNSp3 ", UCASE(IF(COLUMN_TYPE LIKE '%%(%%)%%',  CONCAT(SUBSTRING(COLUMN_TYPE,1, LOCATE('(',COLUMN_TYPE) - 1 ), SUBSTRING(COLUMN_TYPE,1+locate(')',COLUMN_TYPE))), COLUMN_TYPE )) AS TYPE_NAME, "\
  MADB_COLUMN_SIZE " AS COLUMN_SIZE,"\
  MADB_TRANSFER_OCTET_LENGTH(COLUMN_TYPE) " AS BUFFER_LENGTH, "\
  "NUMERIC_SCALE DECIMAL_DIGITS, IF(CHARACTER_OCTET_LENGTH IS NOT NULL, NULL, 10) AS NUM_PREC_RADIX,"\
  "IF(DATA_TYPE='timestamp', 1, IF(IS_NULLABLE='YES',1,IF(EXTRA='auto_increment', 1, 0))) AS NULLABLE, "\
  "COLUMN_COMMENT AS REMARKS,"
#define MADB_DEFAULT_COLUMN_OLD  "IF(COLLATION_NAME IS NOT NULL AND COLUMN_DEFAULT IS NOT NULL, CONCAT(CHAR(39), COLUMN_DEFAULT, CHAR(39)), COLUMN_DEFAULT)"
#define MADB_DEFAULT_COLUMN_NEW  "COLUMN_DEFAULT"
#define MADB_DEFAULT_COLUMN(DbcHndl) (MADB_ServerSupports(DbcHndl,MADB_ENCLOSES_COLUMN_DEF_WITH_QUOTES) ? MADB_DEFAULT_COLUMN_NEW : MADB_DEFAULT_COLUMN_OLD)
#define MADB_CATALOG_TYPE_SUB "CAST(CASE"\
  "  WHEN DATA_TYPE = 'date' THEN " XSTR(SQL_DATETIME)\
  "  WHEN DATA_TYPE = 'time' THEN " XSTR(SQL_DATETIME)\
  "  WHEN DATA_TYPE = 'datetime' THEN " XSTR(SQL_DATETIME)\
  "  WHEN DATA_TYPE = 'timestamp' THEN " XSTR(SQL_DATETIME)\
  " ELSE @dt "\
  "END AS SIGNED) SQL_DATA_TYPE,"\
  "CAST(CASE"\
  "  WHEN DATA_TYPE = 'date' THEN " XSTR(SQL_CODE_DATE)\
  "  WHEN DATA_TYPE = 'time' THEN " XSTR(SQL_CODE_TIME)\
  "  WHEN DATA_TYPE = 'datetime' THEN " XSTR(SQL_CODE_TIMESTAMP)\
  "  WHEN DATA_TYPE = 'timestamp' THEN " XSTR(SQL_CODE_TIMESTAMP)\
  " ELSE NULL "\
  "END AS SIGNED) SQL_DATETIME_SUB,"
#define MADB_CATALOG_COLUMNSp4 " AS COLUMN_DEF," MADB_CATALOG_TYPE_SUB\
  "IF(CHARACTER_OCTET_LENGTH IS NOT NULL, @tol, IF(DATA_TYPE='bit' AND NUMERIC_PRECISION =1, NULL, CAST((NUMERIC_PRECISION + 7)/8 AS SIGNED))) AS CHAR_OCTET_LENGTH, "\
  "ORDINAL_POSITION,"\
  "IF(DATA_TYPE='timestamp', 'YES', IF(IS_NULLABLE='YES','YES',IF(EXTRA='auto_increment', 'YES', 'NO'))) AS IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE "

#define ONEQODBC_COLUMNS_OUTSIDESORT\
    "select table_catalog as TABLE_CAT, table_schema as TABLE_SCHEM, table_name as TABLE_NAME, "\
    "column_name as COLUMN_NAME, REAL_TYPE as DATA_TYPE, data_type as TYPE_NAME, COLUMN_SIZE, "\
    "BUFFER_LENGTH, DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE, NULL as REMARKS, "\
    "column_default as COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB, CHAR_OCTET_LENGTH, "\
    "ordinal_position as ORDINAL_POSITION, is_nullable as IS_NULLABLE from (select *, "
#define ONEQODBC_COLUMNS_CHAROCTLEN\
    "case when real_type in (12, 1, -3) then (BUFFER_LENGTH) else null end "\
    "as CHAR_OCTET_LENGTH from (select *, "
#define ONEQODBC_COLUMNS_COLUMNSIZE\
    "case real_type when -7 then 1  when -5 then 19  when 8 then 53  when 3 then 38 "\
    "when -6 then 3  when 5 then 5 when 4 then 10  when 7 then 7 "\
    "when 12 then (if(BracketLen='NULL', 2048, cast(BracketLen as int))) "\
    "when 1 then (if(BracketLen='NULL', 255, cast(BracketLen as int))) "\
    "when -3 then 2048  when 91 then 10 when 92 then 12  when 93 then 23 "\
    "when 107 then 5  when 110 then 18 else -9999 end as COLUMN_SIZE, "
#define ONEQODBC_COLUMNS_BUFFERLEN\
    "case real_type when -7 then 1  when -5 then 20  when 8 then 8  when 3 then 76 "\
    "when -6 then 3  when 5 then 5 when 4 then 10  when 7 then 4 "\
    "when 12 then (if(BracketLen='NULL', 2048, cast(BracketLen as int))) "\
    "when 1 then (if(BracketLen='NULL', 255, cast(BracketLen as int))) "\
    "when -3 then 2048  when 91 then 6 when 92 then 6  when 93 then 16 "\
    "when 107 then 34  when 110 then 34 else -9999 end as BUFFER_LENGTH, "

// GRUOP FOLLOWING COLUMNS TOGETHER AS ONE MACRO MAINBUILD
#define ONEQODBC_COLUMNS_DECDIGITS\
    "case when real_type in (-5, -6, 5, 4) then 0 when real_type=3 then 0 "\
    "when real_type in (92, 93, 110) then 3 else null end as DECIMAL_DIGITS, "
#define ONEQODBC_COLUMNS_NUMPRECRAD\
    "case when real_type in (-5, -6, 5, 4, 3) then 10 when real_type "\
    "in (7, 8) then 2 else null end as NUM_PREC_RADIX, "
#define ONEQODBC_COLUMNS_NULLABLE "case is_nullable when 'YES' then 1 when 'NO' then 0 else 2 end as NULLABLE, "
#define ONEQODBC_COLUMNS_SQLTYPE\
    "case when real_type in (91, 92, 93) then 9 when real_type in (107, 110) "\
    "then 10 else real_type end as SQL_DATA_TYPE, "
#define ONEQODBC_COLUMNS_SQLDTSUB\
    "case real_type when 91 then 1 when 92 then 2 when 93 then 3 when 107 then 7 "\
    "when 110 then 10 else null end as SQL_DATETIME_SUB from (select *, "
#define ONEQODBC_COLUMNS_MAINBUILD ONEQODBC_COLUMNS_DECDIGITS ONEQODBC_COLUMNS_NUMPRECRAD\
    ONEQODBC_COLUMNS_NULLABLE ONEQODBC_COLUMNS_SQLTYPE ONEQODBC_COLUMNS_SQLDTSUB

// GENERATE BASE COLUMNS OF RESULTSET
#define ONEQODBC_COLUMNS_DATATYPE\
    "case column_type when 'boolean' then -7  when 'bigint' then -5  when 'double' then 8 "\
    "when 'decimal' then 3 when 'tinyint' then -6  when 'smallint' then 5 "\
    "when 'integer' then 4  when 'real' then 7 when 'varchar' then 12 "\
    "when 'char' then 1  when 'varbinary' then -3  when 'date' then 91 "\
    "when 'time' then 92  when 'timestamp' then 93  when 'interval year to month' then 12 "\
    "when 'interval day to second' then 12 else -9999 end as REAL_TYPE from (select *, "
#define ONEQODBC_COLUMNS_COLUMNTYPE\
    "if(DATA_TYPE like '%(%)%', "\
    "CONCAT(SUBSTRING(DATA_TYPE,1, POSITION('(' IN DATA_TYPE) - 1 ), SUBSTRING(DATA_TYPE,1+POSITION(')' IN DATA_TYPE)))"\
    ", data_type) as COLUMN_TYPE, "
#define ONEQODBC_COLUMNS_BRACKETLEN\
    "if(DATA_TYPE like '%(%)%', "\
    "SUBSTRING(DATA_TYPE, POSITION('(' IN DATA_TYPE) + 1, POSITION(')' IN DATA_TYPE)-POSITION('(' IN DATA_TYPE)-1), "\
    "'NULL') as BracketLen"

// GENERATE EMPTY RESULTSET, ADD CORRESPONDING DATA_TYPE THIS TIME
#define ONEQODBC_COLUMNS_EMPTY\
    "select * from (select CAST('NULL' AS VARCHAR(2048)) as TABLE_CAT, CAST('NULL' AS VARCHAR(2048)) as TABLE_SCHEM, "\
    "CAST('NOT NULL' AS VARCHAR(2048)) as TABLE_NAME, CAST('NOT NULL' AS VARCHAR(2048)) as COLUMN_NAME, 0 as DATA_TYPE, "\
    "CAST('NOT NULL' AS VARCHAR(2048)) as TYPE_NAME, 0 as COLUMN_SIZE, 0 as BUFFER_LENGTH, 0 as DECIMAL_DIGITS, "\
    "0 as NUM_PREC_RADIX, 0 as NULLABLE, CAST('NOT NULL' AS VARCHAR(2048)) as REMARKS, "\
    "CAST('NULL' AS VARCHAR(2048)) as COLUMN_DEF, 0 as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, 0 as CHAR_OCTET_LENGTH, "\
    "0 as ORDINAL_POSITION, CAST('NULL' AS VARCHAR(2048)) as IS_NULLABLE) where 1 = 0"


/************** End of SQLColumns ************/

/************** SQLProcedureColumns **********/
#define MADB_PROCEDURE_COLUMNSp1 \
  "SELECT SPECIFIC_SCHEMA AS PROCEDURE_CAT, NULL AS PROCEDURE_SCHEM, "\
  "SPECIFIC_NAME PROCEDURE_NAME, IF(PARAMETER_NAME IS NULL, '', PARAMETER_NAME) COLUMN_NAME, "\
  "CASE PARAMETER_MODE "\
  "  WHEN 'IN' THEN " XSTR(SQL_PARAM_INPUT)\
  "  WHEN 'OUT' THEN " XSTR(SQL_PARAM_OUTPUT)\
  "  WHEN 'INOUT' THEN " XSTR(SQL_PARAM_INPUT_OUTPUT)\
  "  ELSE IF(PARAMETER_MODE IS NULL, " XSTR(SQL_RETURN_VALUE) ", " XSTR(SQL_PARAM_TYPE_UNKNOWN) ")"\
  "END COLUMN_TYPE, "
#define MADB_PROCEDURE_COLUMNSp3\
  ", DATA_TYPE TYPE_NAME, "\
  MADB_COLUMN_SIZE " AS COLUMN_SIZE, "\
  MADB_TRANSFER_OCTET_LENGTH(DTD_IDENTIFIER) " AS BUFFER_LENGTH, "\
  "NUMERIC_SCALE DECIMAL_DIGITS, IF(NUMERIC_PRECISION IS NULL, NULL, 10) AS NUM_PREC_RADIX,"\
  XSTR(SQL_NULLABLE_UNKNOWN) " NULLABLE,"\
  "NULL REMARKS, NULL COLUMN_DEF," MADB_CATALOG_TYPE_SUB \
  "IF(CHARACTER_MAXIMUM_LENGTH IS NULL, NULL, @tol) CHAR_OCTET_LENGTH, "\
  "ORDINAL_POSITION, 'YES' IS_NULLABLE FROM INFORMATION_SCHEMA.PARAMETERS "

#define MADB_PROCEDURE_COLUMNS_ODBC3U MADB_PROCEDURE_COLUMNSp1 MADB_SQL_DATATYPE_ODBC3U MADB_PROCEDURE_COLUMNSp3
#define MADB_PROCEDURE_COLUMNS_ODBC2U MADB_PROCEDURE_COLUMNSp1 MADB_SQL_DATATYPE_ODBC2U MADB_PROCEDURE_COLUMNSp3
#define MADB_PROCEDURE_COLUMNS_ODBC3A MADB_PROCEDURE_COLUMNSp1 MADB_SQL_DATATYPE_ODBC3A MADB_PROCEDURE_COLUMNSp3
#define MADB_PROCEDURE_COLUMNS_ODBC2A MADB_PROCEDURE_COLUMNSp1 MADB_SQL_DATATYPE_ODBC2A MADB_PROCEDURE_COLUMNSp3

#define MADB_PROCEDURE_COLUMNS(StmtHndl) (StmtHndl->Connection->Environment->OdbcVersion >= SQL_OV_ODBC3 ?\
                                        (StmtHndl->Connection->IsAnsi ? MADB_PROCEDURE_COLUMNS_ODBC3A : MADB_PROCEDURE_COLUMNS_ODBC3U) : \
                                        (StmtHndl->Connection->IsAnsi ? MADB_PROCEDURE_COLUMNS_ODBC2A : MADB_PROCEDURE_COLUMNS_ODBC2U))

#define ONEQODBC_PROCEDURE_COLUMNS\
    "select * from (select CAST('NULL' AS VARCHAR(2048)) as PROCEDURE_CAT, CAST('NULL' AS VARCHAR(2048)) as PROCEDURE_SCHEM, "\
    "CAST('NOT NULL' AS VARCHAR(2048)) as PROCEDURE_NAME, CAST('NOT NULL' AS VARCHAR(2048)) as COLUMN_NAME, "\
    "CAST(0 AS SMALLINT) as COLUMN_TYPE, CAST(0 AS SMALLINT) as DATA_TYPE, "\
    "CAST('NOT NULL' AS VARCHAR(2048)) as TYPE_NAME, 0 as COLUMN_SIZE, 0 as BUFFER_LENGTH, CAST(0 AS SMALLINT) as DECIMAL_DIGITS, "\
    "CAST(0 AS SMALLINT) as NUM_PREC_RADIX, CAST(0 AS SMALLINT) as NULLABLE, CAST('NULL' AS VARCHAR(2048)) as REMARKS, "\
    "CAST('NULL' AS VARCHAR(2048)) as COLUMN_DEF, CAST(0 AS SMALLINT) as SQL_DATA_TYPE, CAST(0 AS SMALLINT) as SQL_DATETIME_SUB, "\
    "0 as CHAR_OCTET_LENGTH, 0 as ORDINAL_POSITION, CAST('NULL' AS VARCHAR(2048)) as IS_NULLABLE) where 1=0"
    
/************** SQLProcedureColumns **********/

#endif

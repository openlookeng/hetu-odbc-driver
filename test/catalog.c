/*
  Copyright (c) 2001, 2012, Oracle and/or its affiliates. All rights reserved.
                2013, 2016 MariaDB Corporation AB
  Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.

  The MySQL Connector/ODBC is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most
  MySQL Connectors. There are special exceptions to the terms and
  conditions of the GPLv2 as it is applied to this software, see the
  FLOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
  
  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation; version 2 of the License.
  
  This program is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
  or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
  for more details.
  
  You should have received a copy of the GNU General Public License along
  with this program; if not, write to the Free Software Foundation, Inc.,
  51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include "tap.h"

ODBC_TEST(my_columns_null)
{
  SQLLEN rowCount= 0;
  /* initialize data */
  OK_SIMPLE_STMT(Stmt, "drop table if exists my_column_null");

  OK_SIMPLE_STMT(Stmt, "create table my_column_null(id int, name varchar(30))");

  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

  CHECK_STMT_RC(Stmt, SQLColumns(Stmt, NULL, SQL_NTS, NULL, SQL_NTS,
    (SQLCHAR *)"my_column_null", SQL_NTS,
    NULL, SQL_NTS));

  CHECK_STMT_RC(Stmt, SQLRowCount(Stmt, &rowCount));

  is_num(rowCount, 2);

  is_num(2, my_print_non_format_result(Stmt));

  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

  OK_SIMPLE_STMT(Stmt, "DROP TABLE IF EXISTS my_column_null");

  return OK;
}


ODBC_TEST(my_drop_table)
{
  OK_SIMPLE_STMT(Stmt, "drop table if exists my_drop_table");
  OK_SIMPLE_STMT(Stmt, "create table my_drop_table(id int)");

  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

  CHECK_STMT_RC(Stmt, SQLColumns(Stmt, NULL, 0, NULL, 0,
                            (SQLCHAR *)"my_drop_table", SQL_NTS, NULL, 0));

  is_num(1, my_print_non_format_result(Stmt));

  OK_SIMPLE_STMT(Stmt, "drop table my_drop_table");

  return OK;
}


#define TODBC_BIND_CHAR(n,buf) SQLBindCol(Stmt,n,SQL_C_CHAR,&buf,sizeof(buf),NULL);

ODBC_TEST(sqltables_null4cat)
{
    SQLCHAR buf[50];
    OK_SIMPLE_STMT(Stmt, "drop table if exists t_tblnull4cat");
    OK_SIMPLE_STMT(Stmt, "create table t_tblnull4cat (x int)");
    CHECK_STMT_RC(Stmt, SQLTables(Stmt, NULL, 0, NULL, 0,
        (SQLCHAR *)"t_tblnull4cat", SQL_NTS, NULL, 0));
    CHECK_STMT_RC(Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, buf, 3), "t_tblnull4cat", 11);
    FAIL_IF(SQLFetch(Stmt) != SQL_NO_DATA_FOUND, "expected no data");
    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    OK_SIMPLE_STMT(Stmt, "drop table if exists t_tblnull4cat");

    return OK;
}

ODBC_TEST(sqltables_emptyset)
{
  SQLSMALLINT columns;

  /* SQLTables(): no known table types. */
  CHECK_STMT_RC(Stmt, SQLTables(Stmt, NULL, SQL_NTS, NULL, SQL_NTS, NULL, SQL_NTS,
                           (SQLCHAR *)"UNKNOWN", SQL_NTS));
  CHECK_STMT_RC(Stmt, SQLNumResultCols(Stmt, &columns));
  is_num(columns, 5);
  FAIL_IF(SQLFetch(Stmt) != SQL_NO_DATA_FOUND, "expected no data");
  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

  /* SQLTables(): no tables found. */
  CHECK_STMT_RC(Stmt, SQLTables(Stmt, NULL, SQL_NTS, NULL, SQL_NTS,
                           (SQLCHAR *)"no_such_table", SQL_NTS, NULL, SQL_NTS));
  CHECK_STMT_RC(Stmt, SQLNumResultCols(Stmt, &columns));
  is_num(columns, 5);
  FAIL_IF(SQLFetch(Stmt) != SQL_NO_DATA_FOUND, "expected no data");
  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

  /* SQLTables(): empty catalog with existing table */
  OK_SIMPLE_STMT(Stmt, "drop table if exists t_sqltables_empty");
  OK_SIMPLE_STMT(Stmt, "create table t_sqltables_empty (x int)");
  CHECK_STMT_RC(Stmt, SQLTables(Stmt, NULL, SQL_NTS, NULL, 0,
			   (SQLCHAR *) "t_sqltables_empty", SQL_NTS,
			   NULL, SQL_NTS));
  CHECK_STMT_RC(Stmt, SQLNumResultCols(Stmt, &columns));
  is_num(columns, 5);
  FAIL_IF(SQLFetch(Stmt) == SQL_NO_DATA_FOUND, "expected data");
  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
  OK_SIMPLE_STMT(Stmt, "drop table if exists t_sqltables_empty");

  return OK;
}

ODBC_TEST(my_table_schemas)
{
  SQLCHAR    database[100];
  SQLRETURN  rc;
  SQLINTEGER nrows= 0 ;
  SQLLEN lenOrNull, rowCount= 0;

  //MAP DATABASE TO SCHEMA
  OK_SIMPLE_STMT(Stmt, "DROP SCHEMA IF EXISTS my_all_db_test1");
  OK_SIMPLE_STMT(Stmt, "DROP SCHEMA IF EXISTS my_all_db_test2");
  OK_SIMPLE_STMT(Stmt, "DROP SCHEMA IF EXISTS my_all_db_test3");
  OK_SIMPLE_STMT(Stmt, "DROP SCHEMA IF EXISTS my_all_db_test4");

  /* This call caused problems when database names returned as '%' */
  CHECK_STMT_RC(Stmt, SQLTables(Stmt, NULL, 0, (SQLCHAR*)SQL_ALL_SCHEMAS, 1, NULL, 0, NULL, 0));

  while (SQLFetch(Stmt) == SQL_SUCCESS)
  {
    ++nrows;
    CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 2, SQL_C_CHAR, database,
                              sizeof(database), NULL));
    /* the table catalog in the results must not be '%' */
    FAIL_IF(database[0] == '%', "table catalog can't be '%'");
  }
  /* we should have got rows... */
  FAIL_IF(nrows == 0, "nrows should be > 0");

  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

  CHECK_STMT_RC(Stmt, SQLTables(Stmt,  NULL, 0, (SQLCHAR*)SQL_ALL_SCHEMAS, 1, NULL, 0, NULL, 0));

  /* Added calls to SQLRowCount just to have tests of it with SQLTables. */
  CHECK_STMT_RC(Stmt, SQLRowCount(Stmt, &rowCount));
  nrows = my_print_non_format_result(Stmt);

  is_num(rowCount, nrows);
  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

  CHECK_STMT_RC(Stmt, SQLTables(Stmt, "", 0, (SQLCHAR *)"%", 1, "", 0, NULL, 0));

  CHECK_STMT_RC(Stmt, SQLFetch(Stmt));

  memset(database,0,sizeof(database));
  rc = SQLGetData(Stmt,1,SQL_C_CHAR,database, (SQLLEN)sizeof(database), &lenOrNull);
  CHECK_STMT_RC(Stmt,rc);
  diag("catalog: %s", database);
  IS(lenOrNull == SQL_NULL_DATA);

  memset(database,0,sizeof(database));
  rc = SQLGetData(Stmt,2,SQL_C_CHAR,database, (SQLLEN)sizeof(database),&lenOrNull);
  CHECK_STMT_RC(Stmt,rc);
  diag("schema: %s", database);
  

  memset(database,0,sizeof(database));
  rc = SQLGetData(Stmt,3,SQL_C_CHAR,database, (SQLLEN)sizeof(database),&lenOrNull);
  CHECK_STMT_RC(Stmt,rc);
  diag("table: %s", database);

  memset(database,0,sizeof(database));
  rc = SQLGetData(Stmt,4,SQL_C_CHAR,database, (SQLLEN)sizeof(database),&lenOrNull);
  CHECK_STMT_RC(Stmt,rc);
  diag("type: %s", database);

  memset(database,0,sizeof(database));
  rc = SQLGetData(Stmt,5,SQL_C_CHAR, database, (SQLLEN)sizeof(database),&lenOrNull);
  CHECK_STMT_RC(Stmt,rc);
  diag("database remark: %s", database);

  SQLFreeStmt(Stmt,SQL_UNBIND);
  SQLFreeStmt(Stmt,SQL_CLOSE);

  //Check all schemas and record numbers
  CHECK_STMT_RC(Stmt, SQLTables(Stmt, "", 0, (SQLCHAR*)SQL_ALL_SCHEMAS, 1, "", 0,
      NULL, 0));

  CHECK_STMT_RC(Stmt, SQLRowCount(Stmt, &rowCount));
  nrows = my_print_non_format_result(Stmt);
  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

  OK_SIMPLE_STMT(Stmt, "CREATE SCHEMA my_all_db_test1");
  OK_SIMPLE_STMT(Stmt, "CREATE SCHEMA my_all_db_test2");
  OK_SIMPLE_STMT(Stmt, "CREATE SCHEMA my_all_db_test3");
  OK_SIMPLE_STMT(Stmt, "CREATE SCHEMA my_all_db_test4");

  rc = SQLTables(Stmt, "", 0, (SQLCHAR*)SQL_ALL_SCHEMAS, 1, "", 0, "", 0);
  CHECK_STMT_RC(Stmt,rc);

  nrows += 4;
  is_num(nrows, my_print_non_format_result(Stmt));
  rc = SQLFreeStmt(Stmt, SQL_CLOSE);
  CHECK_STMT_RC(Stmt,rc);

  rc = SQLTables(Stmt, NULL, 0,
        (SQLCHAR *)"my_all_db_test", 1, NULL, 0, NULL, 0);
  CHECK_STMT_RC(Stmt,rc);

  CHECK_STMT_RC(Stmt, SQLRowCount(Stmt, &rowCount));
  is_num(rowCount, 0);

  is_num(my_print_non_format_result(Stmt), 0);
  rc = SQLFreeStmt(Stmt, SQL_CLOSE);
  CHECK_STMT_RC(Stmt,rc);

  rc = SQLTables(Stmt, NULL, 0,
      (SQLCHAR *)"my_all_db_test%", SQL_NTS, NULL, 0, NULL, 0);
  CHECK_STMT_RC(Stmt,rc);
  CHECK_STMT_RC(Stmt, SQLRowCount(Stmt, &rowCount));
  is_num(my_print_non_format_result(Stmt), 0); 

  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

  /* unknown table should be empty */
  rc = SQLTables(Stmt, NULL, 0,
      (SQLCHAR *)"my_all_db_test%", 1, (SQLCHAR *)"xyz", SQL_NTS, NULL, 0);
  CHECK_STMT_RC(Stmt,rc);

  is_num(my_print_non_format_result(Stmt), 0);
  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

  OK_SIMPLE_STMT(Stmt, "DROP SCHEMA my_all_db_test1");
  OK_SIMPLE_STMT(Stmt, "DROP SCHEMA my_all_db_test2");
  OK_SIMPLE_STMT(Stmt, "DROP SCHEMA my_all_db_test3");
  OK_SIMPLE_STMT(Stmt, "DROP SCHEMA my_all_db_test4");

  return OK;
}

#define MYSQL_NAME_LEN 192
#define SERVER_VARCHAR_MAXLEN 2147483647

ODBC_TEST(t_sqltables)
{
  SQLLEN      rowCount, LenInd;
  SQLHDBC     hdbc1;
  SQLHSTMT    Stmt1;
  int         AllTablesCount= 0, Rows;
  SQLCHAR     Buffer[64];

  IS(AllocEnvConn(&Env, &hdbc1));
  Stmt1= DoConnect(hdbc1, FALSE, NULL, NULL, NULL, 0, (SQLCHAR *)my_schema, 0, NULL, NULL);
  FAIL_IF(Stmt1 == NULL, "");
  
  OK_SIMPLE_STMT(Stmt1, "CREATE TABLE IF NOT EXISTS t1 (a int)");
  OK_SIMPLE_STMT(Stmt1, "CREATE TABLE IF NOT EXISTS t2 (a int)");
  OK_SIMPLE_STMT(Stmt1, "CREATE TABLE IF NOT EXISTS t3 (a int)");

  CHECK_STMT_RC(Stmt1, SQLTables(Stmt1, NULL, 0, NULL, 0, NULL, 0, NULL, 0));

  AllTablesCount= myrowcount(Stmt1);
  FAIL_IF(AllTablesCount <= 3, "There should be more than 3 tables"); /* 3 tables in current db */

  CHECK_STMT_RC(Stmt1, SQLFreeStmt(Stmt1, SQL_CLOSE));

  CHECK_STMT_RC(Stmt1, SQLTables(Stmt1, NULL, 0, NULL, 0, NULL, 0, NULL, 0));

  Rows= 0;
  while (SQLFetch(Stmt1) != SQL_NO_DATA_FOUND)
  {
    ++Rows;
    CHECK_STMT_RC(Stmt1, SQLGetData(Stmt1, 3, SQL_C_CHAR, Buffer, sizeof(Buffer), &LenInd));
    FAIL_IF(LenInd == SQL_NULL_DATA, "Table Name should not be NULL")
  }
  /* % catalog should give the same result as NULL. May fail if any table added/dropped between calls */
  is_num(Rows, AllTablesCount);

  CHECK_STMT_RC(Stmt1, SQLFreeStmt(Stmt1, SQL_CLOSE));

  CHECK_STMT_RC(Stmt1, SQLTables(Stmt1, NULL, 0, NULL, 0, NULL, 0,
                               (SQLCHAR *)"TABLE", SQL_NTS));
  Rows= my_print_non_format_result_ex(Stmt1, FALSE);
  CHECK_STMT_RC(Stmt1, SQLRowCount(Stmt1, &rowCount));

  is_num(Rows, rowCount);
  CHECK_STMT_RC(Stmt1, SQLFreeStmt(Stmt1, SQL_CLOSE));

  CHECK_STMT_RC(Stmt1, SQLTables(Stmt1, "", 0, (SQLCHAR *)"%", SQL_NTS, "", 0, "", 0));

  diag("all schemas %d", myrowcount(Stmt1));

  CHECK_STMT_RC(Stmt1, SQLFreeStmt(Stmt1, SQL_CLOSE));

  /* List of table types - table, view, system view */
  CHECK_STMT_RC(Stmt1, SQLTables(Stmt1, "", 0, "", 0, "", 0, (SQLCHAR *)"%", SQL_NTS));

  is_num(my_print_non_format_result(Stmt1), 3);
  /* my_print_non_format_result closes cursor by default */

  OK_SIMPLE_STMT(Stmt1, "DROP TABLE IF EXISTS t1");
  OK_SIMPLE_STMT(Stmt1, "DROP TABLE IF EXISTS t2");
  OK_SIMPLE_STMT(Stmt1, "DROP TABLE IF EXISTS t3");

  CHECK_STMT_RC(Stmt1, SQLFreeStmt(Stmt1, SQL_DROP));
  CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
  CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));
  return OK;
}


typedef struct t_table_descol
{
  SQLCHAR     szColName[MAX_NAME_LEN];
  SQLSMALLINT pcbColName;
  SQLSMALLINT pfSqlType;
  SQLUINTEGER pcbColDef;
  SQLSMALLINT pibScale;
  SQLSMALLINT pfNullable;
} t_describe_col;

t_describe_col table_descol_data[10] =
{
  /* For "Unicode" connection */
  {"TABLE_CAT",   9, SQL_WVARCHAR, SERVER_VARCHAR_MAXLEN, 0, SQL_NULLABLE },
  {"TABLE_SCHEM",11, SQL_WVARCHAR, SERVER_VARCHAR_MAXLEN, 0, SQL_NULLABLE},
  {"TABLE_NAME", 10, SQL_WVARCHAR, SERVER_VARCHAR_MAXLEN, 0, SQL_NULLABLE },
  {"TABLE_TYPE", 10, SQL_WVARCHAR, SERVER_VARCHAR_MAXLEN, 0, SQL_NULLABLE },
  {"REMARKS",     7, SQL_WVARCHAR, SERVER_VARCHAR_MAXLEN, 0, SQL_NULLABLE },
  /* For "ANSI" connection */
  { "TABLE_CAT",   9, SQL_VARCHAR, SERVER_VARCHAR_MAXLEN, 0, SQL_NULLABLE },
  { "TABLE_SCHEM",11, SQL_VARCHAR, SERVER_VARCHAR_MAXLEN, 0, SQL_NULLABLE },
  { "TABLE_NAME", 10, SQL_VARCHAR, SERVER_VARCHAR_MAXLEN, 0, SQL_NULLABLE },
  { "TABLE_TYPE", 10, SQL_VARCHAR, SERVER_VARCHAR_MAXLEN, 0, SQL_NULLABLE },
  { "REMARKS",     7, SQL_VARCHAR, SERVER_VARCHAR_MAXLEN, 0, SQL_NULLABLE },

};

ODBC_TEST(sqltables_desccol)
{
  SQLSMALLINT i, ColumnCount, pcbColName, pfSqlType, pibScale, pfNullable;
  SQLULEN     pcbColDef;
  SQLCHAR     szColName[MAX_NAME_LEN];
  const int   RefArrOffset = iOdbc() ? -1 : 4; /* 4 for "ANSI" connection, which is default atm, and -1 for "Unicode" */

  CHECK_STMT_RC(Stmt,  SQLTables(Stmt, NULL, 0, NULL, 0, NULL, 0,
                            (SQLCHAR *)"TABLE", SQL_NTS));

  CHECK_STMT_RC(Stmt, SQLNumResultCols(Stmt, &ColumnCount));
  is_num(ColumnCount, 5);

  for (i= 1; i <= ColumnCount; ++i)
  {
    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, (SQLUSMALLINT)i, szColName,
                                 MAX_NAME_LEN, &pcbColName, &pfSqlType,
                                 &pcbColDef, &pibScale, &pfNullable));

    fprintf(stdout, "# Column '%d':\n", i);
    fprintf(stdout, "#  Column Name   : %s\n", szColName);
    fprintf(stdout, "#  NameLengh     : %d\n", pcbColName);
    fprintf(stdout, "#  DataType      : %d\n", pfSqlType);
    fprintf(stdout, "#  ColumnSize    : %lu\n", pcbColDef);
    fprintf(stdout, "#  DecimalDigits : %d\n", pibScale);
    fprintf(stdout, "#  Nullable      : %d\n", pfNullable);

    IS_STR(table_descol_data[i + RefArrOffset].szColName, szColName, pcbColName);
    is_num(table_descol_data[i + RefArrOffset].pcbColName, pcbColName);
    is_num(table_descol_data[i + RefArrOffset].pfSqlType, pfSqlType);
    /* This depends on NAME_LEN in mysql_com.h */

    is_num(table_descol_data[i + RefArrOffset].pibScale, pibScale);
    is_num(table_descol_data[i + RefArrOffset].pfNullable, pfNullable);
  }

  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

  return OK;
}


ODBC_TEST(sqltables_buffoverrun)
{
  CHECK_STMT_RC(Stmt, SQLTables(Stmt, NULL, 0, NULL, 0, NULL, 0,
                           (SQLCHAR *)
/* Just a really long literal to blow out the buffer. */
"0123456789012345678901234567890123456789012345678901234567890123456789"
"0123456789012345678901234567890123456789012345678901234567890123456789"
"0123456789012345678901234567890123456789012345678901234567890123456789"
"0123456789012345678901234567890123456789012345678901234567890123456789"
"0123456789012345678901234567890123456789012345678901234567890123456789"
"0123456789012345678901234567890123456789012345678901234567890123456789"
"0123456789012345678901234567890123456789012345678901234567890123456789"
"0123456789012345678901234567890123456789012345678901234567890123456789"
"0123456789012345678901234567890123456789012345678901234567890123456789"
"0123456789012345678901234567890123456789012345678901234567890123456789"
"0123456789012345678901234567890123456789012345678901234567890123456789"
"0123456789012345678901234567890123456789012345678901234567890123456789"
"0123456789012345678901234567890123456789012345678901234567890123456789",
                           SQL_NTS));

  return OK;
}

ODBC_TEST(sqltables_view)
{
  SQLCHAR buff[255];

  OK_SIMPLE_STMT(Stmt, "DROP VIEW IF EXISTS t_tables_v");
  OK_SIMPLE_STMT(Stmt, "DROP TABLE IF EXISTS t_tables_t");
  OK_SIMPLE_STMT(Stmt, "CREATE TABLE t_tables_t (a INT)");
  OK_SIMPLE_STMT(Stmt, "CREATE VIEW t_tables_v AS SELECT * FROM t_tables_t");

  /* Get both the table and view. */
  CHECK_STMT_RC(Stmt, SQLTables(Stmt, NULL, SQL_NTS, NULL, SQL_NTS,
                           (SQLCHAR *)"t_tables%", SQL_NTS, NULL, SQL_NTS));

  CHECK_STMT_RC(Stmt, SQLFetch(Stmt));
  IS_STR(my_fetch_str(Stmt, buff, 3), "t_tables_t", 12);
  IS_STR(my_fetch_str(Stmt, buff, 4), "TABLE", 10);

  CHECK_STMT_RC(Stmt, SQLFetch(Stmt));
  IS_STR(my_fetch_str(Stmt, buff, 3), "t_tables_v", 12);
  IS_STR(my_fetch_str(Stmt, buff, 4), "VIEW", 4);

  FAIL_IF(SQLFetch(Stmt) != SQL_NO_DATA_FOUND, "expected no data");

  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

  /* Get just the table. */
  CHECK_STMT_RC(Stmt, SQLTables(Stmt, NULL, SQL_NTS, NULL, SQL_NTS,
                           (SQLCHAR *)"t_tables%", SQL_NTS,
                           (SQLCHAR *)"TABLE", SQL_NTS));

  CHECK_STMT_RC(Stmt, SQLFetch(Stmt));
  IS_STR(my_fetch_str(Stmt, buff, 3), "t_tables_t", 12);
  IS_STR(my_fetch_str(Stmt, buff, 4), "TABLE", 10);

  FAIL_IF(SQLFetch(Stmt) != SQL_NO_DATA_FOUND, "expected no data");

  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

  /* Get just the view. */
  CHECK_STMT_RC(Stmt, SQLTables(Stmt, NULL, SQL_NTS, NULL, SQL_NTS,
                           (SQLCHAR *)"t_tables%", SQL_NTS,
                           (SQLCHAR *)"VIEW", SQL_NTS));

  CHECK_STMT_RC(Stmt, SQLFetch(Stmt));
  IS_STR(my_fetch_str(Stmt, buff, 3), "t_tables_v", 12);
  IS_STR(my_fetch_str(Stmt, buff, 4), "VIEW", 4);

  FAIL_IF(SQLFetch(Stmt) != SQL_NO_DATA_FOUND, "expected no data");

  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

  OK_SIMPLE_STMT(Stmt, "DROP VIEW IF EXISTS t_tables_v");
  OK_SIMPLE_STMT(Stmt, "DROP TABLE IF EXISTS t_tables_t");

  return OK;
}

ODBC_TEST(sqltables_searchpattern_catalog_and_esacape)
{
    SQLHANDLE henv1;
    SQLHANDLE hdbc1;
    SQLHANDLE hstmt1;


    ODBC_Connect(&henv1, &hdbc1, &hstmt1);
    char catbuff[128];
    SQLLEN rowCount = 0;
    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_CURRENT_CATALOG,
        catbuff, sizeof(catbuff), NULL));
    diag("DSNcatalog:%s", catbuff);

    CHECK_STMT_RC(hstmt1, SQLTables(hstmt1, (SQLCHAR *)"sy_tem", SQL_NTS,
                                        (SQLCHAR *)"information\\_schema", SQL_NTS, 
                                        (SQLCHAR *)"table\\_privileges", SQL_NTS, NULL, 0));
    CHECK_STMT_RC(hstmt1, SQLRowCount(hstmt1, &rowCount));
    is_num(my_print_non_format_result(hstmt1), 1);

    CHECK_STMT_RC(hstmt1, SQLTables(hstmt1, (SQLCHAR *)"sy\\_tem", SQL_NTS,
        (SQLCHAR *)"information_schema", SQL_NTS,
        (SQLCHAR *)"table_privileges", SQL_NTS, NULL, 0));
    CHECK_STMT_RC(hstmt1, SQLRowCount(hstmt1, &rowCount));
    is_num(my_print_non_format_result(hstmt1), 0);

    OK_SIMPLE_STMT(hstmt1, "DROP SCHEMA IF EXISTS \"my_te%st\"");
    OK_SIMPLE_STMT(hstmt1, "CREATE SCHEMA \"my_te%st\"");
    OK_SIMPLE_STMT(hstmt1, "USE my_te%st");
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS \"my_test%tbl\"");
    OK_SIMPLE_STMT(hstmt1, "CREATE TABLE \"my_test%tbl\" (\"col%a\" INT)");

    CHECK_STMT_RC(hstmt1, SQLTables(hstmt1, (SQLCHAR *)catbuff, SQL_NTS,
        (SQLCHAR *)"my\\_t_\\%st", SQL_NTS,
        (SQLCHAR *)"my\\_t_st\\%tbl", SQL_NTS, NULL, 0));
    CHECK_STMT_RC(hstmt1, SQLRowCount(hstmt1, &rowCount));
    is_num(my_print_non_format_result(hstmt1), 1);

    OK_SIMPLE_STMT(hstmt1, "DROP TABLE \"my_test%tbl\"");
    OK_SIMPLE_STMT(hstmt1, "DROP SCHEMA \"my_te%st\"");

    ODBC_Disconnect(henv1, hdbc1, hstmt1);

    return OK;
}

ODBC_TEST(t_sqlcol)
{
  SQLCHAR    buff[512];

  OK_SIMPLE_STMT(Stmt, "DROP TABLE IF EXISTS sqlcol_test");
  OK_SIMPLE_STMT(Stmt, "CREATE TABLE sqlcol_test (a INT)");

  CHECK_STMT_RC(Stmt, SQLColumns(Stmt, NULL, SQL_NTS, NULL, SQL_NTS,
                             (SQLCHAR *)"sqlcol_test", SQL_NTS, NULL, 0));

  CHECK_STMT_RC(Stmt, SQLFetch(Stmt));

  IS_STR(my_fetch_str(Stmt, buff, 3), "sqlcol_test", 9);
  IS_STR(my_fetch_str(Stmt, buff, 4), "a", 1);

  FAIL_IF(SQLFetch(Stmt) != SQL_NO_DATA_FOUND, "expected no data");

  OK_SIMPLE_STMT(Stmt, "DROP TABLE sqlcol_test");
  return OK;
}

ODBC_TEST(sqlcol_colnumbers)
{
    SQLRETURN rc;
    SQLCHAR      name[MYSQL_NAME_LEN+1];
    SQLUSMALLINT i;
    SQLSMALLINT  ncols, len;

    SQLCHAR colnames[18][20]= {
        "TABLE_CAT","TABLE_SCHEM","TABLE_NAME","COLUMN_NAME",
        "DATA_TYPE","TYPE_NAME","COLUMN_SIZE","BUFFER_LENGTH",
        "DECIMAL_DIGITS","NUM_PREC_RADIX","NULLABLE","REMARKS",
        "COLUMN_DEF","SQL_DATA_TYPE","SQL_DATETIME_SUB",
        "CHAR_OCTET_LENGTH","ORDINAL_POSITION","IS_NULLABLE"
    };
    SQLSMALLINT collengths[18]= {
        9,11,10,11,9,9,11,13,14,14,8,7,10,13,16,17,16,11
    };

    OK_SIMPLE_STMT(Stmt, "drop table if exists t_catalog");

    OK_SIMPLE_STMT(Stmt,"create table t_catalog(abc tinyint, bcdefghijklmno char(4), uifield int)");

    CHECK_STMT_RC(Stmt, SQLColumns(Stmt, NULL, 0, NULL, 0,
                              (SQLCHAR *)"t_catalog", 9, NULL, 0));

    rc = SQLNumResultCols(Stmt, &ncols);
    CHECK_STMT_RC(Stmt,rc);

    diag("total columns: %d", ncols);
    IS(ncols == 18);
    IS(myrowcount(Stmt) == 3);

    SQLFreeStmt(Stmt, SQL_UNBIND);
    SQLFreeStmt(Stmt, SQL_CLOSE);

    rc = SQLColumns(Stmt, NULL, 0, NULL, 0,
                    (SQLCHAR *)"t_catalog", 9, NULL, 0);
    CHECK_STMT_RC(Stmt,rc);

    rc = SQLNumResultCols(Stmt,&ncols);
    CHECK_STMT_RC(Stmt,rc);

    for (i= 1; i <= (SQLUINTEGER) ncols; i++)
    {
        rc = SQLDescribeCol(Stmt, i, name, MYSQL_NAME_LEN+1, &len, NULL, NULL, NULL, NULL);
        CHECK_STMT_RC(Stmt,rc);

        diag("column %d: %s (%d)", i, name, len);
        is_num(len, collengths[i - 1]);
        IS_STR(name, colnames[i - 1], len);
    }
    SQLFreeStmt(Stmt,SQL_CLOSE);

  OK_SIMPLE_STMT(Stmt, "DROP TABLE IF EXISTS t_catalog");

  return OK;
}

ODBC_TEST(sqlcol_checkresult)
{
    SQLSMALLINT   NumPrecRadix, DataType, Nullable, DecimalDigits;
    SQLLEN        cbColumnSize, cbDecimalDigits, cbNumPrecRadix,
        cbDataType, cbNullable;
    SQLINTEGER    cbDatabaseName;
    SQLUINTEGER   ColumnSize, i;
    SQLUINTEGER   ColumnCount = 5;
    SQLCHAR       ColumnName[MAX_NAME_LEN], DatabaseName[MAX_NAME_LEN];
    SQLINTEGER    Values[5][5][2] =
    {
        { { 5,2 },{ 5,4 },{ 0,2 },{ 10,2 },{ SQL_NULLABLE,2 } },
        { { SQL_CHAR, 2 },{ 5,4 },{ 0,-1 },{ 10,-1 },{ SQL_NULLABLE,2 } },
        { { SQL_VARCHAR,2 },{ 20,4 },{ 0,-1 },{ 10,-1 },{ SQL_NULLABLE,2 } },
        { { -6,2 },{ 3,4 },{ 0,2 },{ 10,2 },{ SQL_NULLABLE,2 } },
        { { 4,2 },{ 10,4 },{ 0,2 },{ 10,2 },{ SQL_NULLABLE,2 } },
    };

    OK_SIMPLE_STMT(Stmt, "DROP TABLE IF EXISTS t_columns");

    OK_SIMPLE_STMT(Stmt,
        "CREATE TABLE t_columns (col0 SMALLINT,"
        "col1 CHAR(5), col2 VARCHAR(20), col3 TINYINT, " //REMOVE DECIMAL
        "col4 INTEGER)");

    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_METADATA_ID,
        (SQLPOINTER)SQL_FALSE, SQL_IS_UINTEGER));

    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_CURSOR_TYPE,
        (SQLPOINTER)SQL_CURSOR_STATIC, 0));

    CHECK_DBC_RC(Connection, SQLGetConnectAttr(Connection, SQL_ATTR_CURRENT_CATALOG,
        DatabaseName, MAX_NAME_LEN,
        &cbDatabaseName)); /* Current Catalog */

    for (i = 0; i < ColumnCount; i++)
    {
        sprintf((char *)ColumnName, "col%d", (int)i);
        diag("checking column `%s`", (char *)ColumnName);

        CHECK_STMT_RC(Stmt, SQLColumns(Stmt,
            DatabaseName, (SQLSMALLINT)cbDatabaseName,
            (SQLCHAR *)"", SQL_NTS,
            (SQLCHAR *)"t_columns", SQL_NTS,
            ColumnName, SQL_NTS));

        /* 5 -- Data type */
        CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 5, SQL_C_SSHORT, &DataType, 0,
            &cbDataType));

        /* 7 -- Column Size */
        CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 7, SQL_C_ULONG, &ColumnSize, 0,
            &cbColumnSize));

        /* 9 -- Decimal Digits */
        CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 9, SQL_C_SSHORT, &DecimalDigits, 0,
            &cbDecimalDigits));

        /* 10 -- Num Prec Radix */
        CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 10, SQL_C_SSHORT, &NumPrecRadix, 0,
            &cbNumPrecRadix));

        /* 11 -- Nullable */
        CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 11, SQL_C_SSHORT, &Nullable, 0,
            &cbNullable));

        CHECK_STMT_RC(Stmt, SQLFetch(Stmt));

        is_num(DataType, GetDefaultCharType(Values[i][0][0], FALSE));
        is_num(cbDataType, Values[i][0][1]);

        is_num(ColumnSize, Values[i][1][0]);
        is_num(cbColumnSize, Values[i][1][1]);

        is_num(DecimalDigits, Values[i][2][0]);
        is_num(cbDecimalDigits, Values[i][2][1]);

        is_num(NumPrecRadix, Values[i][3][0]);
        is_num(cbNumPrecRadix, Values[i][3][1]);

        is_num(Nullable, Values[i][4][0]);
        is_num(cbNullable, Values[i][4][1]);

        FAIL_IF(SQLFetch(Stmt) != SQL_NO_DATA_FOUND, "expected no data");

        CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_UNBIND));
        CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    }

    OK_SIMPLE_STMT(Stmt, "DROP TABLE IF EXISTS t_columns");
    return OK;
}


ODBC_TEST(sqlcol_tablenamewithquote)
{
    SQLCHAR buff[512];
    OK_SIMPLE_STMT(Stmt, "DROP TABLE IF EXISTS tblname_with_quote");
    OK_SIMPLE_STMT(Stmt, "DROP TABLE IF EXISTS \"tblname_with_quote_a'b\"");
    OK_SIMPLE_STMT(Stmt, "CREATE TABLE tblname_with_quote (a INT)");
    OK_SIMPLE_STMT(Stmt, "CREATE TABLE \"tblname_with_quote_a'b\" (b INT)");

    CHECK_STMT_RC(Stmt, SQLColumns(Stmt, NULL, 0, NULL, 0, NULL, 0, NULL, 0));

    /* We should have at least two rows. There may be more. */
    FAIL_IF(myrowcount(Stmt) < 2, "expected min. 2 rows");

    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    if (!using_dm(Connection))
    {
        /* Specifying "" as the table name gets us nothing. */
        CHECK_STMT_RC(Stmt, SQLColumns(Stmt, NULL, 0, NULL, 0, (SQLCHAR *)"", SQL_NTS,
            NULL, 0));

        is_num(myrowcount(Stmt), 0);
    }

    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    /* Get the info from just one table.  */
    CHECK_STMT_RC(Stmt, SQLColumns(Stmt, NULL, 0, NULL, 0,
        (SQLCHAR *)"tblname_with_quote", SQL_NTS,
        NULL, 0));

    CHECK_STMT_RC(Stmt, SQLFetch(Stmt));

    IS_STR(my_fetch_str(Stmt, buff, 3), "tblname_with_quote", 9);
    IS_STR(my_fetch_str(Stmt, buff, 4), "a", 1);

    FAIL_IF(SQLFetch(Stmt) != SQL_NO_DATA_FOUND, "expected no data");

    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    /* Get the info from just one table with a funny name.  */
    CHECK_STMT_RC(Stmt, SQLColumns(Stmt, NULL, 0, NULL, 0,
        (SQLCHAR *)"tblname_with_quote_a''b", SQL_NTS,
        NULL, 0));

    CHECK_STMT_RC(Stmt, SQLFetch(Stmt));

    IS_STR(my_fetch_str(Stmt, buff, 3), "tblname_with_quote_a'b", 13);
    IS_STR(my_fetch_str(Stmt, buff, 4), "b", 1);

    FAIL_IF(SQLFetch(Stmt) != SQL_NO_DATA_FOUND, "expected no data");

    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    OK_SIMPLE_STMT(Stmt, "DROP TABLE IF EXISTS tblname_with_quote");
    OK_SIMPLE_STMT(Stmt, "DROP TABLE IF EXISTS \"tblname_with_quote_a'b\"");
    return OK;
}


ODBC_TEST(sqlcols_quotes)
{
    SQLCHAR name[20];
    SQLLEN len;

    OK_SIMPLE_STMT(Stmt, "drop table if exists t_col_with_quote");
    ERR_SIMPLE_STMT(Stmt, "create table t_col_with_quote (`doesn't work` int)");
    CHECK_SQLSTATE(Stmt, "HY000");

    OK_SIMPLE_STMT(Stmt, "create table t_col_with_quote (\"doesn't work\" int)");

    CHECK_STMT_RC(Stmt, SQLColumns(Stmt, NULL, 0, NULL, 0,
        (SQLCHAR *)"t_col_with_quote", SQL_NTS, NULL, 0));
    CHECK_STMT_RC(Stmt, SQLFetch(Stmt));

    CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 4, SQL_C_CHAR, name, 20, &len));
    is_num(len, 12);
    IS_STR(name, "doesn't work", 13);
    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "drop table if exists t_col_with_quote");
    return OK;
}

ODBC_TEST(sqlcol_searchpattern_catalog_and_esacape)
{
    SQLHANDLE henv1;
    SQLHANDLE hdbc1;
    SQLHANDLE hstmt1;


    ODBC_Connect(&henv1, &hdbc1, &hstmt1);
    char catbuff[128];
    SQLLEN rowCount = 0;
    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_CURRENT_CATALOG,
        catbuff, sizeof(catbuff), NULL));
    diag("DSNcatalog:%s", catbuff);


    OK_SIMPLE_STMT(hstmt1, "DROP SCHEMA IF EXISTS \"my_te%st\"");
    OK_SIMPLE_STMT(hstmt1, "CREATE SCHEMA \"my_te%st\"");
    OK_SIMPLE_STMT(hstmt1, "USE my_te%st");
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS \"my_test%tbl\"");
    OK_SIMPLE_STMT(hstmt1, "CREATE TABLE \"my_test%tbl\" (\"col%a\" INT)");

    CHECK_STMT_RC(hstmt1, SQLColumns(hstmt1, (SQLCHAR *)"system", SQL_NTS, 
                                   (SQLCHAR *)"information\\_schema", SQL_NTS,
                                   (SQLCHAR *)"table\\_privileges", SQL_NTS,
                                   (SQLCHAR *)"table\\_name", SQL_NTS));
    CHECK_STMT_RC(hstmt1, SQLRowCount(hstmt1, &rowCount));
    is_num(my_print_non_format_result(hstmt1), 1);

    CHECK_STMT_RC(hstmt1, SQLColumns(hstmt1, (SQLCHAR *)catbuff, SQL_NTS,
                                   (SQLCHAR *)"my\\_t_\\%st", SQL_NTS,
                                   (SQLCHAR *)"my\\_t_st\\%tbl", SQL_NTS,
                                   (SQLCHAR *)"c_l\\%a", SQL_NTS));
    CHECK_STMT_RC(hstmt1, SQLRowCount(hstmt1, &rowCount));
    is_num(my_print_non_format_result(hstmt1), 1);

    OK_SIMPLE_STMT(hstmt1, "DROP TABLE \"my_test%tbl\"");
    OK_SIMPLE_STMT(hstmt1, "DROP SCHEMA \"my_te%st\"");

    ODBC_Disconnect(henv1, hdbc1, hstmt1);

    return OK;
}


ODBC_TEST(sqlcol_catalognotexist)
{
    SQLHANDLE henv1;
    SQLHANDLE hdbc1;
    SQLHANDLE hstmt1;


    ODBC_Connect(&henv1, &hdbc1, &hstmt1);
    char catbuff[128];
    SQLLEN rowCount = 0;
    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_CURRENT_CATALOG,
        catbuff, sizeof(catbuff), NULL));
    diag("DSNcatalog:%s", catbuff);

    CHECK_STMT_RC(hstmt1, SQLColumns(hstmt1, (SQLCHAR *)"syste", SQL_NTS,
        NULL, 0, NULL, 0, NULL, 0));
    CHECK_STMT_RC(hstmt1, SQLRowCount(hstmt1, &rowCount));
    is_num(my_print_non_format_result(hstmt1), 0);

    CHECK_STMT_RC(hstmt1, SQLColumns(hstmt1, catbuff, SQL_NTS,
        NULL, 0, NULL, 0, NULL, 0));
    CHECK_STMT_RC(hstmt1, SQLRowCount(hstmt1, &rowCount));
    FAIL_IF(rowCount < 1, "expected have results!");

    ODBC_Disconnect(henv1, hdbc1, hstmt1);

    return OK;
}


ODBC_TEST(gettypeinfo_colnumber)
{
    SQLSMALLINT pccol;

    CHECK_STMT_RC(Stmt, SQLGetTypeInfo(Stmt, SQL_ALL_TYPES));

    CHECK_STMT_RC(Stmt, SQLNumResultCols(Stmt, &pccol));
    is_num(pccol, 19);

    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    return OK;
}

ODBC_TEST(gettypeinfo_time)
{
    CHECK_STMT_RC(Stmt, SQLGetTypeInfo(Stmt, SQL_TYPE_TIMESTAMP));
    is_num(myrowcount(Stmt), 1);
    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    CHECK_STMT_RC(Stmt, SQLGetTypeInfo(Stmt, SQL_TYPE_TIME));
    is_num(myrowcount(Stmt), 1);
    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    CHECK_STMT_RC(Stmt, SQLGetTypeInfo(Stmt, SQL_TYPE_DATE));
    is_num(myrowcount(Stmt), 1);
    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    return OK;
}

// Mapping Interval to VARCHAR cause this case invalid
//ODBC_TEST(gettypeinfo_wchar)
//{
//    SQLCHAR params[64];
//    SQLLEN ind, rc;
//    CHECK_STMT_RC(Stmt, SQLGetTypeInfo(Stmt, SQL_CHAR));
//    CHECK_STMT_RC(Stmt, SQLFetch(Stmt));
//    IS_STR(my_fetch_str(Stmt, params, 6), "LENGTH", sizeof("LENGTH"));
//    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
//
//    CHECK_STMT_RC(Stmt, SQLGetTypeInfo(Stmt, SQL_VARCHAR));
//    CHECK_STMT_RC(Stmt, SQLFetch(Stmt));
//    IS_STR(my_fetch_str(Stmt, params, 6), "max length", sizeof("max length"));
//    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
//
//    CHECK_STMT_RC(Stmt, SQLGetTypeInfo(Stmt, SQL_WLONGVARCHAR));
//    rc = SQLFetch(Stmt);
//    is_num(rc, SQL_NO_DATA);
//    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
//
//    CHECK_STMT_RC(Stmt, SQLGetTypeInfo(Stmt, SQL_INTEGER));
//    CHECK_STMT_RC(Stmt, SQLFetch(Stmt));
//    CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 6, SQL_C_CHAR, params, sizeof(params), &ind));
//    is_num(ind, SQL_NULL_DATA);
//    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
//
//    CHECK_STMT_RC(Stmt, SQLGetTypeInfo(Stmt, SQL_WCHAR));
//    rc = SQLFetch(Stmt);
//    is_num(rc, SQL_NO_DATA);
//    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
//
//    CHECK_STMT_RC(Stmt, SQLGetTypeInfo(Stmt, SQL_WVARCHAR));
//    rc = SQLFetch(Stmt);
//    is_num(rc, SQL_NO_DATA);
//    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
//
//    CHECK_STMT_RC(Stmt, SQLGetTypeInfo(Stmt, SQL_LONGVARCHAR));
//    rc = SQLFetch(Stmt);
//    is_num(rc, SQL_NO_DATA);
//    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
//    return OK;
//}

ODBC_TEST(t_sqlstatistics)
{
    SQLRETURN rc;
    SQLCHAR      name[MYSQL_NAME_LEN + 1];
    SQLUSMALLINT i;
    SQLSMALLINT  ncols, len;

    SQLCHAR colnames[13][20] = {
        "TABLE_CAT","TABLE_SCHEM","TABLE_NAME","NON_UNIQUE",
        "INDEX_QUALIFIER","INDEX_NAME","TYPE","ORDINAL_POSITION",
        "COLUMN_NAME","ASC_OR_DESC","CARDINALITY","PAGES","FILTER_CONDITION"
    };
    SQLSMALLINT collengths[13] = {
        9,11,10,10,15,10,4,16,11,11,11,5,16
    };

    rc = SQLStatistics(Stmt, NULL, 0, NULL, 0, NULL, 0,
        SQL_INDEX_UNIQUE, SQL_QUICK);
    is_num(rc, SQL_ERROR);
    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    CHECK_STMT_RC(Stmt, SQLStatistics(Stmt, NULL, 0, NULL, 0, (SQLCHAR *)"test", SQL_NTS,
        SQL_INDEX_UNIQUE, SQL_QUICK));

    rc = SQLNumResultCols(Stmt, &ncols);
    CHECK_STMT_RC(Stmt, rc);

    diag("total columns: %d", ncols);
    IS(ncols == 13);

    for (i = 1; i <= (SQLUINTEGER)ncols; i++)
    {
        rc = SQLDescribeCol(Stmt, i, name, MYSQL_NAME_LEN + 1, &len, NULL, NULL, NULL, NULL);
        CHECK_STMT_RC(Stmt, rc);

        diag("column %d: %s (%d)", i, name, len);
        is_num(len, collengths[i - 1]);
        IS_STR(name, colnames[i - 1], len);
    }
    SQLFreeStmt(Stmt, SQL_CLOSE);
    return OK;
}

ODBC_TEST(t_sqltablepriv)
{
    SQLRETURN rc;
    SQLCHAR      name[MYSQL_NAME_LEN + 1];
    SQLUSMALLINT i;
    SQLSMALLINT  ncols, len;

    SQLCHAR colnames[7][20] = {
        "TABLE_CAT","TABLE_SCHEM","TABLE_NAME","GRANTOR",
        "GRANTEE","PRIVILEGE","IS_GRANTABLE"
    };
    SQLSMALLINT collengths[7] = {
        9,11,10,7,7,9,12
    };

    CHECK_STMT_RC(Stmt, SQLTablePrivileges(Stmt, NULL, 0, NULL, 0, NULL, 0));

    rc = SQLNumResultCols(Stmt, &ncols);
    CHECK_STMT_RC(Stmt, rc);

    diag("total columns: %d", ncols);
    IS(ncols == 7);

    for (i = 1; i <= (SQLUINTEGER)ncols; i++)
    {
        rc = SQLDescribeCol(Stmt, i, name, MYSQL_NAME_LEN + 1, &len, NULL, NULL, NULL, NULL);
        CHECK_STMT_RC(Stmt, rc);

        diag("column %d: %s (%d)", i, name, len);
        is_num(len, collengths[i - 1]);
        IS_STR(name, colnames[i - 1], len);
    }
    SQLFreeStmt(Stmt, SQL_CLOSE);
    return OK;
}

ODBC_TEST(t_sqlprocedures)
{
    SQLRETURN rc;
    SQLCHAR      name[MYSQL_NAME_LEN + 1];
    SQLUSMALLINT i;
    SQLSMALLINT  ncols, len;

    SQLCHAR colnames[8][20] = {
        "PROCEDURE_CAT","PROCEDURE_SCHEM","PROCEDURE_NAME","NUM_INPUT_PARAMS",
        "NUM_OUTPUT_PARAMS","NUM_RESULT_SETS","REMARKS","PROCEDURE_TYPE"
    };
    SQLSMALLINT collengths[8] = {
        13,15,14,16,17,15,7,14
    };

    CHECK_STMT_RC(Stmt, SQLProcedures(Stmt, NULL, 0, NULL, 0,  NULL, 0));

    rc = SQLNumResultCols(Stmt, &ncols);
    CHECK_STMT_RC(Stmt, rc);

    diag("total columns: %d", ncols);
    IS(ncols == 8);

    for (i = 1; i <= (SQLUINTEGER)ncols; i++)
    {
        rc = SQLDescribeCol(Stmt, i, name, MYSQL_NAME_LEN + 1, &len, NULL, NULL, NULL, NULL);
        CHECK_STMT_RC(Stmt, rc);

        diag("column %d: %s (%d)", i, name, len);
        is_num(len, collengths[i - 1]);
        IS_STR(name, colnames[i - 1], len);
    }
    SQLFreeStmt(Stmt, SQL_CLOSE);
    return OK;
}

ODBC_TEST(sqlprocedures_crash)
{
  CHECK_STMT_RC(Stmt, SQLProcedures(Stmt, NULL, SQL_NTS, NULL, SQL_NTS, NULL, SQL_NTS));

  return OK;
}

ODBC_TEST(t_sqlprocedcols)
{
    SQLRETURN rc;
    SQLCHAR      name[MYSQL_NAME_LEN + 1];
    SQLUSMALLINT i;
    SQLSMALLINT  ncols, len;

    SQLCHAR colnames[19][30] = {
        "PROCEDURE_CAT","PROCEDURE_SCHEM","PROCEDURE_NAME","COLUMN_NAME",
        "COLUMN_TYPE","DATA_TYPE","TYPE_NAME","COLUMN_SIZE",
        "BUFFER_LENGTH","DECIMAL_DIGITS","NUM_PREC_RADIX","NULLABLE",
        "REMARKS","COLUMN_DEF","SQL_DATA_TYPE","SQL_DATETIME_SUB",
        "CHAR_OCTET_LENGTH","ORDINAL_POSITION","IS_NULLABLE"
    };
    SQLSMALLINT collengths[19] = {
        13,15,14,11,11,9,9,11,13,14,14,8,7,10,13,16,17,16,11
    };

    CHECK_STMT_RC(Stmt, SQLProcedureColumns(Stmt, NULL, 0, NULL, 0,
        NULL, 0, NULL, 0));

    rc = SQLNumResultCols(Stmt, &ncols);
    CHECK_STMT_RC(Stmt, rc);

    diag("total columns: %d", ncols);
    IS(ncols == 19);
    IS(myrowcount(Stmt) == 0);

    SQLFreeStmt(Stmt, SQL_UNBIND);
    SQLFreeStmt(Stmt, SQL_CLOSE);

    rc = SQLProcedureColumns(Stmt, NULL, 0, NULL, 0,
        NULL, 0, NULL, 0);
    CHECK_STMT_RC(Stmt, rc);

    rc = SQLNumResultCols(Stmt, &ncols);
    CHECK_STMT_RC(Stmt, rc);

    for (i = 1; i <= (SQLUINTEGER)ncols; i++)
    {
        rc = SQLDescribeCol(Stmt, i, name, MYSQL_NAME_LEN + 1, &len, NULL, NULL, NULL, NULL);
        CHECK_STMT_RC(Stmt, rc);

        diag("column %d: %s (%d)", i, name, len);
        is_num(len, collengths[i - 1]);
        IS_STR(name, colnames[i - 1], len);
    }
    SQLFreeStmt(Stmt, SQL_CLOSE);
    return OK;
}

ODBC_TEST(t_colpriv)
{
    SQLRETURN rc;
    SQLCHAR      name[MYSQL_NAME_LEN + 1];
    SQLUSMALLINT i;
    SQLSMALLINT  ncols, len;

    SQLCHAR colnames[8][20] = {
        "TABLE_CAT","TABLE_SCHEM","TABLE_NAME","COLUMN_NAME",
        "GRANTOR","GRANTEE","PRIVILEGE","IS_GRANTABLE"
    };
    SQLSMALLINT collengths[8] = {
        9,11,10,11,7,7,9,12
    };

    rc = SQLColumnPrivileges(Stmt, NULL, 0, NULL, 0, NULL, 0, NULL, 0);
    is_num(rc, SQL_ERROR);
    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    CHECK_STMT_RC(Stmt, SQLColumnPrivileges(Stmt, NULL, 0, NULL, 0, (SQLCHAR *)"test", SQL_NTS, NULL, 0));
    rc = SQLNumResultCols(Stmt, &ncols);
    CHECK_STMT_RC(Stmt, rc);
    
    diag("total columns: %d", ncols);
    IS(ncols == 8);

    for (i = 1; i <= (SQLUINTEGER)ncols; i++)
    {
        rc = SQLDescribeCol(Stmt, i, name, MYSQL_NAME_LEN + 1, &len, NULL, NULL, NULL, NULL);
        CHECK_STMT_RC(Stmt, rc);

        diag("column %d: %s (%d)", i, name, len);
        is_num(len, collengths[i - 1]);
        IS_STR(name, colnames[i - 1], len);
    }
    SQLFreeStmt(Stmt, SQL_CLOSE);
    return OK;
}

ODBC_TEST(t_specialcols)
{
  SQLRETURN rc;
  SQLCHAR      name[MYSQL_NAME_LEN + 1];
  SQLUSMALLINT i;
  SQLSMALLINT  ncols, len;

  SQLCHAR colnames[8][20] = {
      "SCOPE","COLUMN_NAME","DATA_TYPE","TYPE_NAME",
      "COLUMN_SIZE","BUFFER_LENGTH","DECIMAL_DIGITS","PSEUDO_COLUMN"
  };
  SQLSMALLINT collengths[8] = {
      5,11,9,9,11,13,14,13
  };

  rc = SQLSpecialColumns(Stmt, SQL_BEST_ROWID, NULL, 0, NULL, 0, NULL, 0,
      SQL_SCOPE_CURROW, SQL_NO_NULLS);
  is_num(rc, SQL_ERROR);
  CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

  CHECK_STMT_RC(Stmt, SQLSpecialColumns(Stmt, SQL_BEST_ROWID, NULL, 0, NULL, 0,
      (SQLCHAR *)"test", SQL_NTS,
      SQL_SCOPE_CURROW, SQL_NO_NULLS));

  rc = SQLNumResultCols(Stmt, &ncols);
  CHECK_STMT_RC(Stmt, rc);

  diag("total columns: %d", ncols);
  IS(ncols == 8);

  for (i = 1; i <= (SQLUINTEGER)ncols; i++)
  {
      rc = SQLDescribeCol(Stmt, i, name, MYSQL_NAME_LEN + 1, &len, NULL, NULL, NULL, NULL);
      CHECK_STMT_RC(Stmt, rc);

      diag("column %d: %s (%d)", i, name, len);
      is_num(len, collengths[i - 1]);
      IS_STR(name, colnames[i - 1], len);
  }
  SQLFreeStmt(Stmt, SQL_CLOSE);
  return OK;
}

ODBC_TEST(t_sqlprimarykeys)
{
    SQLRETURN rc;
    SQLCHAR      name[MYSQL_NAME_LEN + 1];
    SQLUSMALLINT i;
    SQLSMALLINT  ncols, len;

    SQLCHAR colnames[6][20] = {
        "TABLE_CAT","TABLE_SCHEM","TABLE_NAME","COLUMN_NAME",
        "KEY_SEQ","PK_NAME"
    };
    SQLSMALLINT collengths[6] = {
        9,11,10,11,7,7
    };

    rc = SQLPrimaryKeys(Stmt, NULL, 0, NULL, 0, NULL, 0);
    is_num(rc, SQL_ERROR);
    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    CHECK_STMT_RC(Stmt, SQLPrimaryKeys(Stmt, NULL, 0, NULL, 0, (SQLCHAR *)"test", SQL_NTS));
    rc = SQLNumResultCols(Stmt, &ncols);
    CHECK_STMT_RC(Stmt, rc);

    diag("total columns: %d", ncols);
    IS(ncols == 6);

    for (i = 1; i <= (SQLUINTEGER)ncols; i++)
    {
        rc = SQLDescribeCol(Stmt, i, name, MYSQL_NAME_LEN + 1, &len, NULL, NULL, NULL, NULL);
        CHECK_STMT_RC(Stmt, rc);

        diag("column %d: %s (%d)", i, name, len);
        is_num(len, collengths[i - 1]);
        IS_STR(name, colnames[i - 1], len);
    }
    SQLFreeStmt(Stmt, SQL_CLOSE);
    return OK;
}

ODBC_TEST(t_sqlforeignkeys)
{
    SQLRETURN rc;
    SQLCHAR      name[MYSQL_NAME_LEN + 1];
    SQLUSMALLINT i;
    SQLSMALLINT  ncols, len;

    SQLCHAR colnames[14][20] = {
        "PKTABLE_CAT","PKTABLE_SCHEM","PKTABLE_NAME","PKCOLUMN_NAME",
        "FKTABLE_CAT","FKTABLE_SCHEM","FKTABLE_NAME","FKCOLUMN_NAME",
        "KEY_SEQ","UPDATE_RULE","DELETE_RULE","FK_NAME","PK_NAME","DEFERRABILITY"
    };
    SQLSMALLINT collengths[14] = {
        11,13,12,13,11,13,12,13,7,11,11,7,7,13
    };

    rc = SQLForeignKeys(Stmt, NULL, 0, NULL, 0, NULL, 0, NULL, 0,
        NULL, 0, NULL, 0);
    is_num(rc, SQL_ERROR);
    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    CHECK_STMT_RC(Stmt, SQLForeignKeys(Stmt, NULL, 0, NULL, 0, (SQLCHAR *)"test1", SQL_NTS, NULL, 0,
        NULL, 0, (SQLCHAR *)"test2", SQL_NTS));
    rc = SQLNumResultCols(Stmt, &ncols);
    CHECK_STMT_RC(Stmt, rc);

    diag("total columns: %d", ncols);
    IS(ncols == 14);

    for (i = 1; i <= (SQLUINTEGER)ncols; i++)
    {
        rc = SQLDescribeCol(Stmt, i, name, MYSQL_NAME_LEN + 1, &len, NULL, NULL, NULL, NULL);
        CHECK_STMT_RC(Stmt, rc);

        diag("column %d: %s (%d)", i, name, len);
        is_num(len, collengths[i - 1]);
        IS_STR(name, colnames[i - 1], len);
    }
    SQLFreeStmt(Stmt, SQL_CLOSE);
    return OK;
}

MA_ODBC_TESTS my_tests[]=
{
  { my_columns_null, "my_columns_null" },
  { my_drop_table, "my_drop_table" },
  { my_table_schemas, "my_table_schemas" },
  { sqltables_null4cat,"sqltables_null4cat" },
  { sqltables_emptyset, "sqltables_emptyset" },
  { t_sqltables, "t_sqltables"},
  { sqltables_buffoverrun, "sqltables_buffoverrun"},
  { sqltables_view, "sqltables_view" },
  { sqltables_searchpattern_catalog_and_esacape, "sqltables_searchpattern_catalog_and_esacape" },
  { sqltables_desccol, "sqltables_desccol" },
  { t_sqlcol, "t_sqlcol" },
  { sqlcol_colnumbers, "sqlcol_colnumbers" },
  { sqlcol_checkresult, "sqlcol_checkresult" },
  { sqlcol_tablenamewithquote, "sqlcol_tablenamewithquote" },
  { sqlcols_quotes, "sqlcols_quotes" },
  { sqlcol_searchpattern_catalog_and_esacape, "sqlcol_searchpattern_catalog_and_esacape" },
  { sqlcol_catalognotexist , "sqlcol_catalognotexist" },
  { gettypeinfo_colnumber, "gettypeinfo_colnumber" },
  { gettypeinfo_time, "gettypeinfo_time" },
  //{ gettypeinfo_wchar, "gettypeinfo_wchar" },
  { t_sqlstatistics, "t_sqlstatistics" },
  { t_sqltablepriv, "t_sqltablepriv" },
  { t_sqlprocedures, "t_sqlprocedures" },
  { sqlprocedures_crash, "sqlprocedures_crash" },
  { t_sqlprocedcols, "t_sqlprocedcols" },
  { t_colpriv, "t_colpriv" },
  { t_specialcols, "t_specialcols" },
  { t_sqlprimarykeys, "t_sqlprimarykeys" },
  { t_sqlforeignkeys, "t_sqlforeignkeys" },
  {NULL, NULL}
};

int main(int argc, char **argv)
{
  int tests= sizeof(my_tests)/sizeof(MA_ODBC_TESTS) - 1;
  get_options(argc, argv);
  plan(tests);
  mark_all_tests_normal(my_tests);
  return run_tests(my_tests);
}

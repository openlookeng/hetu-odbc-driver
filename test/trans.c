/*
  Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
  
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

ODBC_TEST(test_trans_basic)
{
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;  
    SQLUINTEGER mode = 0;
    SQLSMALLINT transCapable = SQL_TC_NONE;

    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    /* default auto commit is open */
    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    FAIL_IF(mode == SQL_AUTOCOMMIT_OFF, "auto commit value should be open on defalt");

    /* support transaction */
    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_TXN_CAPABLE, (SQLPOINTER)&transCapable, 0, NULL));
    FAIL_IF(mode == SQL_TC_NONE, "expect support transaction");

    /* switch to manual commit mode test */
    CHECK_DBC_RC(hdbc1, SQLSetConnectAttr(hdbc1, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0));
    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_OFF);

    /* switch to auto commit mode test */
    CHECK_DBC_RC(hdbc1, SQLSetConnectAttr(hdbc1, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0));
    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_ON);
    
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

    return OK;
}

/* manual test, trans2 will not see data of trans1 untill commit */
ODBC_TEST(test_manual_trans_basic)
{
#define ATTR_STATUS_RECORD_ARRAY_SIZE  3
    SQLHANDLE   henv0;
    SQLHANDLE   hdbc0;
    SQLHANDLE   hstmt0;
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLULEN      rowsFetchedPtr = 0;

    typedef struct {
        SQLINTEGER id;
        SQLINTEGER value;
        SQLLEN     idLen;
        SQLLEN     valueLen;
    }Record;
    SQLULEN      rowcnt = ATTR_STATUS_RECORD_ARRAY_SIZE;
    SQLINTEGER   row_size = sizeof(Record);
    Record       record[ATTR_STATUS_RECORD_ARRAY_SIZE];
    Record       record1[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLUINTEGER  mode = 0;

    ODBC_Connect(&henv0, &hdbc0, &hstmt0);
    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    for (int i = 0; i < ATTR_STATUS_RECORD_ARRAY_SIZE; i++)
    {
        record[i].id      = i;
        record[i].value   = i * 10;
    }

    OK_SIMPLE_STMT(hstmt0, "DROP TABLE IF EXISTS test_manual_trans_basic");
    OK_SIMPLE_STMT(hstmt0, "CREATE TABLE test_manual_trans_basic (id INTEGER, myValue INTEGER)");

    /* switch to manual commit mode */
    CHECK_DBC_RC(hdbc0, SQLSetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0));
    CHECK_DBC_RC(hdbc0, SQLGetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_OFF);
    
    FAIL_IF(SQLExecDirect(hstmt0, "INSERT INTO test_manual_trans_basic (id, myValue) VALUES (0, 0),(1, 10),(2, 20)", SQL_NTS) != SQL_SUCCESS, "success expected");

    CHECK_STMT_RC(hstmt0, SQLFreeStmt(hstmt0, SQL_RESET_PARAMS));

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetchedPtr, 0));
    
    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_manual_trans_basic ORDER BY id");

    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &record1[0].id, sizeof(record1[0].id), &record1[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_LONG, &record1[0].value, sizeof(record1[0].value), &record1[0].valueLen));

    /* trans not commit yet, can not see any data in hdbc1 */
    EXPECT_STMT(hstmt1, SQLFetch(hstmt1), SQL_NO_DATA);
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    CHECK_DBC_RC(hdbc0, SQLEndTran(SQL_HANDLE_DBC, hdbc0, SQL_COMMIT));

    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_manual_trans_basic ORDER BY id");
    CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));

    is_num(rowcnt, rowsFetchedPtr);
    for (int i = 0; i < rowcnt; i++)
    {
        is_num(record1[i].id, record[i].id);
        is_num(record1[i].value, record[i].value);
    }

    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_manual_trans_basic");

    ODBC_Disconnect(henv0, hdbc0, hstmt0);
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

#undef ATTR_STATUS_RECORD_ARRAY_SIZE

    return OK;
}

/* test commit/rollback empty trans */
ODBC_TEST(test_empty_tran)
{
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;    
    SQLUINTEGER mode = 0;

    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    CHECK_DBC_RC(hdbc1, SQLSetConnectAttr(hdbc1, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0));
    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_OFF);

    /* empty commit test */
    CHECK_DBC_RC(hdbc1, SQLEndTran(SQL_HANDLE_DBC, hdbc1, SQL_COMMIT));
    
    CHECK_DBC_RC(hdbc1, SQLSetConnectAttr(hdbc1, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0));
    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_ON);

    CHECK_DBC_RC(hdbc1, SQLSetConnectAttr(hdbc1, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0));
    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_OFF);

    /* empty rollback test */
    CHECK_DBC_RC(hdbc1, SQLEndTran(SQL_HANDLE_DBC, hdbc1, SQL_ROLLBACK));
    
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

    return OK;
}

/* end trans with env will commit all trans */
ODBC_TEST(test_end_env_trans)
{
#define ATTR_STATUS_RECORD_ARRAY_SIZE  3
    SQLHANDLE   henv0;
    SQLHANDLE   hdbc0;
    SQLHANDLE   hstmt0;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLHANDLE   henv2;
    SQLHANDLE   hdbc2;
    SQLHANDLE   hstmt2;
    SQLULEN     rowsFetchedPtr = 0;

    typedef struct {
        SQLINTEGER id;
        SQLINTEGER value;
        SQLLEN     idLen;
        SQLLEN     valueLen;
    }Record;
    
    SQLULEN      rowcnt = ATTR_STATUS_RECORD_ARRAY_SIZE;
    SQLINTEGER   row_size = sizeof(Record);
    Record       record[ATTR_STATUS_RECORD_ARRAY_SIZE];
    Record       record1[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLUINTEGER  mode = 0;

    ODBC_Connect(&henv0, &hdbc0, &hstmt0);
    IS(AllocEnvConn(&henv0, &hdbc1));
    hstmt1 = DoConnect(hdbc1, FALSE, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL);
    FAIL_IF(hstmt1 == NULL, "connect to dsn error.");
    ODBC_Connect(&henv2, &hdbc2, &hstmt2);

    OK_SIMPLE_STMT(hstmt0, "DROP TABLE IF EXISTS test_end_env_trans");
    OK_SIMPLE_STMT(hstmt0, "CREATE TABLE test_end_env_trans (id INTEGER, myValue INTEGER)");

    OK_SIMPLE_STMT(hstmt0, "DROP TABLE IF EXISTS test_end_env_trans1");
    OK_SIMPLE_STMT(hstmt0, "CREATE TABLE test_end_env_trans1 (id INTEGER, myValue INTEGER)");

    /* hdbc0/hdbc1 all switch to manual commit mode */
    CHECK_DBC_RC(hdbc0, SQLSetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0));
    CHECK_DBC_RC(hdbc0, SQLGetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_OFF);

    CHECK_DBC_RC(hdbc1, SQLSetConnectAttr(hdbc1, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0));
    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_OFF);

    FAIL_IF(SQLExecDirect(hstmt0, "INSERT INTO test_end_env_trans (id, myValue) VALUES (0, 0),(1, 10),(2, 20)", SQL_NTS) != SQL_SUCCESS, "success expected");
    FAIL_IF(SQLExecDirect(hstmt1, "INSERT INTO test_end_env_trans1 (id, myValue) VALUES (10, 100),(20, 200),(30, 300)", SQL_NTS) != SQL_SUCCESS, "success expected");

    CHECK_STMT_RC(hstmt0, SQLFreeStmt(hstmt0, SQL_RESET_PARAMS));
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_RESET_PARAMS));

    CHECK_STMT_RC(hstmt2, SQLSetStmtAttr(hstmt2, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(hstmt2, SQLSetStmtAttr(hstmt2, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(hstmt2, SQLSetStmtAttr(hstmt2, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetchedPtr, 0));
    
    OK_SIMPLE_STMT(hstmt2, "SELECT * FROM test_end_env_trans ORDER BY id");

    CHECK_STMT_RC(hstmt2, SQLBindCol(hstmt2, 1, SQL_C_LONG, &record1[0].id, sizeof(record1[0].id), &record1[0].idLen));
    CHECK_STMT_RC(hstmt2, SQLBindCol(hstmt2, 2, SQL_C_LONG, &record1[0].value, sizeof(record1[0].value), &record1[0].valueLen));

    /* trans not commit yet, can not see any data in hdbc1 */
    EXPECT_STMT(hstmt2, SQLFetch(hstmt2), SQL_NO_DATA);
    CHECK_STMT_RC(hstmt2, SQLFreeStmt(hstmt2, SQL_CLOSE));

    /* end all trans in henv0 */
    CHECK_ENV_RC(henv0, SQLEndTran(SQL_HANDLE_ENV, henv0, SQL_COMMIT));

    /* check data in test_end_env_trans */
    OK_SIMPLE_STMT(hstmt2, "SELECT * FROM test_end_env_trans ORDER BY id");
    CHECK_STMT_RC(hstmt2, SQLFetch(hstmt2));
    is_num(rowcnt, rowsFetchedPtr);

    /* this data is base on the insert statement */
    for (int i = 0; i < ATTR_STATUS_RECORD_ARRAY_SIZE; i++)
    {
        record[i].id      = i;
        record[i].value   = i * 10;
    }

    /* do the real check work */
    for (int i = 0; i < rowcnt; i++)
    {
        is_num(record1[i].id, record[i].id);
        is_num(record1[i].value, record[i].value);
    }

    CHECK_STMT_RC(hstmt2, SQLFreeStmt(hstmt2, SQL_CLOSE));
    rowsFetchedPtr = 0;

    /* check data in test_end_env_trans1 */
    OK_SIMPLE_STMT(hstmt2, "SELECT * FROM test_end_env_trans1 ORDER BY id");
    CHECK_STMT_RC(hstmt2, SQLFetch(hstmt2));
    is_num(rowcnt, rowsFetchedPtr);

    /* this data is base on the insert statement */
    for (int i = 0; i < ATTR_STATUS_RECORD_ARRAY_SIZE; i++)
    {
        record[i].id      = (i + 1) * 10;
        record[i].value   = (i + 1) * 100;
    }

    /* do the real check work */
    for (int i = 0; i < rowcnt; i++)
    {
        is_num(record1[i].id, record[i].id);
        is_num(record1[i].value, record[i].value);
    }
    
    CHECK_STMT_RC(hstmt2, SQLFreeStmt(hstmt1, SQL_CLOSE));
    OK_SIMPLE_STMT(hstmt0, "DROP TABLE IF EXISTS test_end_env_trans");
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_end_env_trans1");

    ODBC_Disconnect(NULL, hdbc1, hstmt1);
    ODBC_Disconnect(henv0, hdbc0, hstmt0);
    ODBC_Disconnect(henv2, hdbc2, hstmt2);

#undef ATTR_STATUS_RECORD_ARRAY_SIZE

    return OK;
}

/* execute DDL in trans will commit all trans in manual mode */
ODBC_TEST(test_ddl_force_commit)
{
#define ATTR_STATUS_RECORD_ARRAY_SIZE  3
    SQLHANDLE   henv0;
    SQLHANDLE   hdbc0;
    SQLHANDLE   hstmt0;
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLULEN      rowsFetchedPtr = 0;

    typedef struct {
        SQLINTEGER id;
        SQLINTEGER value;
        SQLLEN     idLen;
        SQLLEN     valueLen;
    }Record;

    SQLULEN      rowcnt = ATTR_STATUS_RECORD_ARRAY_SIZE;
    SQLINTEGER   row_size = sizeof(Record);
    Record       record[ATTR_STATUS_RECORD_ARRAY_SIZE];
    Record       record1[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLUINTEGER  mode = 0;

    ODBC_Connect(&henv0, &hdbc0, &hstmt0);
    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    for (int i = 0; i < ATTR_STATUS_RECORD_ARRAY_SIZE; i++)
    {
        record[i].id      = i;
        record[i].value   = i * 10;
    }

    OK_SIMPLE_STMT(hstmt0, "DROP TABLE IF EXISTS test_ddl_force_commit");
    OK_SIMPLE_STMT(hstmt0, "CREATE TABLE test_ddl_force_commit (id INTEGER, myValue INTEGER)");

    /* switch to manual commit mode */
    CHECK_DBC_RC(hdbc0, SQLSetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0));
    CHECK_DBC_RC(hdbc0, SQLGetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_OFF);
    
    FAIL_IF(SQLExecDirect(hstmt0, "INSERT INTO test_ddl_force_commit (id, myValue) VALUES (0, 0),(1, 10),(2, 20)", SQL_NTS) != SQL_SUCCESS, "success expected");

    CHECK_STMT_RC(hstmt0, SQLFreeStmt(hstmt0, SQL_RESET_PARAMS));

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetchedPtr, 0));
    
    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_ddl_force_commit ORDER BY id");

    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &record1[0].id, sizeof(record1[0].id), &record1[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_LONG, &record1[0].value, sizeof(record1[0].value), &record1[0].valueLen));

    /* trans not commit yet, can not see any data in hdbc1 */
    EXPECT_STMT(hstmt1, SQLFetch(hstmt1), SQL_NO_DATA);
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    CHECK_DBC_RC(hdbc0, SQLEndTran(SQL_HANDLE_DBC, hdbc0, SQL_COMMIT));
    OK_SIMPLE_STMT(hstmt0, "DROP TABLE IF EXISTS test_ddl_force_commit1");
    OK_SIMPLE_STMT(hstmt0, "CREATE TABLE test_ddl_force_commit1 (id INTEGER, myValue INTEGER)");

    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_ddl_force_commit ORDER BY id");
    CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));

    is_num(rowcnt, rowsFetchedPtr);
    for (int i = 0; i < rowcnt; i++)
    {
        is_num(record1[i].id, record[i].id);
        is_num(record1[i].value, record[i].value);
    }

    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_ddl_force_commit");
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_ddl_force_commit1");

    ODBC_Disconnect(henv0, hdbc0, hstmt0);
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

#undef ATTR_STATUS_RECORD_ARRAY_SIZE

    return OK;
}

/* test rollback in manual mode */
ODBC_TEST(test_rollback_trans)
{
#define ATTR_STATUS_RECORD_ARRAY_SIZE  3
    SQLHANDLE   henv0;
    SQLHANDLE   hdbc0;
    SQLHANDLE   hstmt0;
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;

    typedef struct {
        SQLINTEGER id;
        SQLINTEGER value;
        SQLLEN     idLen;
        SQLLEN     valueLen;
    }Record;

    SQLULEN      rowcnt = ATTR_STATUS_RECORD_ARRAY_SIZE;
    SQLINTEGER   row_size = sizeof(Record);
    SQLUINTEGER  mode = 0;

    ODBC_Connect(&henv0, &hdbc0, &hstmt0);
    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    OK_SIMPLE_STMT(hstmt0, "DROP TABLE IF EXISTS test_rollback_trans");
    OK_SIMPLE_STMT(hstmt0, "CREATE TABLE test_rollback_trans (id INTEGER, myValue INTEGER)");

    /* switch to manual commit mode */
    CHECK_DBC_RC(hdbc0, SQLSetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0));
    CHECK_DBC_RC(hdbc0, SQLGetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_OFF);
    
    FAIL_IF(SQLExecDirect(hstmt0, "INSERT INTO test_rollback_trans (id, myValue) VALUES (0, 0),(1, 10),(2, 20)", SQL_NTS) != SQL_SUCCESS, "success expected");

    CHECK_STMT_RC(hstmt0, SQLFreeStmt(hstmt0, SQL_RESET_PARAMS));

    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_rollback_trans ORDER BY id");

    /* trans not commit yet, can not see any data in hdbc1 */
    EXPECT_STMT(hstmt1, SQLFetch(hstmt1), SQL_NO_DATA);
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    CHECK_DBC_RC(hdbc0, SQLEndTran(SQL_HANDLE_DBC, hdbc0, SQL_ROLLBACK));

    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_rollback_trans ORDER BY id");
    
    /* trans not commit yet, can not see any data in hdbc1 */
    EXPECT_STMT(hstmt1, SQLFetch(hstmt1), SQL_NO_DATA);

    SQLEndTran(SQL_HANDLE_DBC, hdbc0, SQL_COMMIT);

    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_rollback_trans ORDER BY id");

    /* trans has been rollback, no data in table */
    EXPECT_STMT(hstmt1, SQLFetch(hstmt1), SQL_NO_DATA);
    
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_rollback_trans");

    ODBC_Disconnect(henv0, hdbc0, hstmt0);
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

#undef ATTR_STATUS_RECORD_ARRAY_SIZE

    return OK;
}

/* test roolback in auto commit mode */
ODBC_TEST(test_rollback_trans1)
{
#define ATTR_STATUS_RECORD_ARRAY_SIZE  3
    SQLHANDLE   henv0;
    SQLHANDLE   hdbc0;
    SQLHANDLE   hstmt0;
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLULEN     rowsFetchedPtr = 0;

    typedef struct {
        SQLINTEGER id;
        SQLINTEGER value;
        SQLLEN     idLen;
        SQLLEN     valueLen;
    }Record;
    SQLULEN      rowcnt = ATTR_STATUS_RECORD_ARRAY_SIZE;
    SQLINTEGER   row_size = sizeof(Record);
    Record       record[ATTR_STATUS_RECORD_ARRAY_SIZE];
    Record       record1[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLUINTEGER  mode = 0;

    ODBC_Connect(&henv0, &hdbc0, &hstmt0);
    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    for (int i = 0; i < ATTR_STATUS_RECORD_ARRAY_SIZE; i++)
    {
        record[i].id      = i;
        record[i].value   = i * 10;
    }

    OK_SIMPLE_STMT(hstmt0, "DROP TABLE IF EXISTS test_rollback_trans1");
    OK_SIMPLE_STMT(hstmt0, "CREATE TABLE test_rollback_trans1 (id INTEGER, myValue INTEGER)");

    /* default is in auto commit mode */
    CHECK_DBC_RC(hdbc0, SQLGetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    FAIL_IF(mode == SQL_AUTOCOMMIT_OFF, "defalt auto commit value should be open");
    
    FAIL_IF(SQLExecDirect(hstmt0, "INSERT INTO test_rollback_trans1 (id, myValue) VALUES (0, 0),(1, 10),(2, 20)", SQL_NTS) != SQL_SUCCESS, "success expected");

    CHECK_STMT_RC(hstmt0, SQLFreeStmt(hstmt0, SQL_RESET_PARAMS));

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetchedPtr, 0));

    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &record1[0].id, sizeof(record1[0].id), &record1[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_LONG, &record1[0].value, sizeof(record1[0].value), &record1[0].valueLen));

    CHECK_DBC_RC(hdbc0, SQLEndTran(SQL_HANDLE_DBC, hdbc0, SQL_ROLLBACK));

    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_rollback_trans1 ORDER BY id");

    /* rollback will not take affect in auto commit mode, data will exist in table */
    CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));

    is_num(rowcnt, rowsFetchedPtr);
    for (int i = 0; i < rowcnt; i++)
    {
        is_num(record1[i].id, record[i].id);
        is_num(record1[i].value, record[i].value);
    }

    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_rollback_trans1");

    ODBC_Disconnect(henv0, hdbc0, hstmt0);
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

#undef ATTR_STATUS_RECORD_ARRAY_SIZE

    return OK;
}

/* test Effect of trans actions(commit/rollback) on Cursors */
ODBC_TEST(test_trans_affect_cursor)
{
#define ATTR_STATUS_RECORD_ARRAY_SIZE  8
    SQLHANDLE   henv0;
    SQLHANDLE   hdbc0;
    SQLHANDLE   hstmt0;
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLULEN     rowsFetchedPtr = 0;
    int         index;

    typedef struct {
        SQLINTEGER id;
        SQLINTEGER value;
        SQLLEN     idLen;
        SQLLEN     valueLen;
    }Record;

    SQLULEN      curType;
    SQLULEN      rowcnt = ATTR_STATUS_RECORD_ARRAY_SIZE;
    SQLULEN      rowcn1 = ATTR_STATUS_RECORD_ARRAY_SIZE - 5; /* read part of rows at the first time */
    SQLULEN      rowcn2 = ATTR_STATUS_RECORD_ARRAY_SIZE - 3; /* read part of rows at the first time */
    SQLINTEGER   row_size = sizeof(Record);
    Record       record[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLUINTEGER  mode = 0;

    ODBC_Connect(&henv0, &hdbc0, &hstmt0);
    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    OK_SIMPLE_STMT(hstmt0, "DROP TABLE IF EXISTS test_trans_affect_cursor");
    OK_SIMPLE_STMT(hstmt0, "CREATE TABLE test_trans_affect_cursor (id INTEGER, myValue INTEGER)");

    /* switch to manual commit mode */
    CHECK_DBC_RC(hdbc0, SQLSetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0));
    CHECK_DBC_RC(hdbc0, SQLGetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_OFF);

    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)&curType, 0, NULL));
    is_num(curType, SQL_CURSOR_STATIC);

    FAIL_IF(SQLExecDirect(hstmt0, "INSERT INTO test_trans_affect_cursor (id, myValue) VALUES (10, 100),(20, 200),"
            "(30, 300),(40, 400),(50, 500),(60, 600),(70, 700),(80, 800)", SQL_NTS) != SQL_SUCCESS, "success expected");
    CHECK_DBC_RC(hdbc0, SQLEndTran(SQL_HANDLE_DBC, hdbc0, SQL_COMMIT));

    CHECK_STMT_RC(hstmt0, SQLFreeStmt(hstmt0, SQL_RESET_PARAMS));

    for (int i = 0; i < ATTR_STATUS_RECORD_ARRAY_SIZE; i++)
    {
        record[i].id      = (i + 1) * 10;
        record[i].value   = (i + 1) * 100;
    }

    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_trans_affect_cursor ORDER BY id");

    //CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_FIRST, 0L));
    for (index = 0; index < rowcn1; index++)
    {
        is_num(my_fetch_int(hstmt1, 1), record[index].id);
        is_num(my_fetch_int(hstmt1, 2), record[index].value);
        CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_NEXT, 0L));
    }

    FAIL_IF(SQLExecDirect(hstmt0, "INSERT INTO test_trans_affect_cursor (id, myValue) VALUES (35, 350),(45, 450),(55, 550)", SQL_NTS) != SQL_SUCCESS, "success expected");
    CHECK_DBC_RC(hdbc0, SQLEndTran(SQL_HANDLE_DBC, hdbc0, SQL_COMMIT));

    /* trans commit has no affect on current cursor */
    for (; index < rowcn2; index++)
    {
        is_num(my_fetch_int(hstmt1, 1), record[index].id);
        is_num(my_fetch_int(hstmt1, 2), record[index].value);
        CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_NEXT, 0L));
    }

    FAIL_IF(SQLExecDirect(hstmt0, "INSERT INTO test_trans_affect_cursor (id, myValue) VALUES (65, 650),(75, 750),(85, 850)", SQL_NTS) != SQL_SUCCESS, "success expected");
    CHECK_DBC_RC(hdbc0, SQLEndTran(SQL_HANDLE_DBC, hdbc0, SQL_ROLLBACK));

    /* trans rollback has no affect on current cursor */
    for (; index < ATTR_STATUS_RECORD_ARRAY_SIZE; index++)
    {
        is_num(my_fetch_int(hstmt1, 1), record[index].id);
        is_num(my_fetch_int(hstmt1, 2), record[index].value);
        if ((index != ATTR_STATUS_RECORD_ARRAY_SIZE - 1)) {
            CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_NEXT, 0L));
        }
    }
    
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_trans_affect_cursor");

    ODBC_Disconnect(henv0, hdbc0, hstmt0);
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

#undef ATTR_STATUS_RECORD_ARRAY_SIZE

    return OK;
}

/* test Effect of Transactions on Prepared Statements */
ODBC_TEST(test_trans_affect_statement)
{
#define ATTR_STATUS_RECORD_ARRAY_SIZE  3
#define TRANS_AFFECT_RECORD_NAME_LEN   20
    SQLHANDLE   henv0;
    SQLHANDLE   hdbc0;
    SQLHANDLE   hstmt0;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLULEN     rowsFetchedPtr = 0;
    SQLRETURN   rc;

    typedef struct {
        SQLINTEGER id;
        SQLCHAR name[TRANS_AFFECT_RECORD_NAME_LEN + 1];
        SQLLEN idLen;
        SQLLEN nameLen;
    }Record;

    SQLULEN      rowcnt = ATTR_STATUS_RECORD_ARRAY_SIZE;
    SQLINTEGER   row_size = sizeof(Record);
    Record       record[ATTR_STATUS_RECORD_ARRAY_SIZE];
    Record       record1[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLUINTEGER  mode = 0;

    ODBC_Connect(&henv0, &hdbc0, &hstmt0);
    IS(AllocEnvConn(&henv0, &hdbc1));
    hstmt1 = DoConnect(hdbc1, FALSE, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL);
    FAIL_IF(hstmt1 == NULL, "connect to dsn error.");

    OK_SIMPLE_STMT(hstmt0, "DROP TABLE IF EXISTS test_trans_affect_statement");
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_trans_affect_statement1");
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_trans_affect_statement2");
    OK_SIMPLE_STMT(hstmt0, "CREATE TABLE test_trans_affect_statement (id INTEGER, myValue INTEGER)");
    OK_SIMPLE_STMT(hstmt1, "CREATE TABLE test_trans_affect_statement1 (id INTEGER, name CHAR(20))");
    OK_SIMPLE_STMT(hstmt1, "CREATE TABLE test_trans_affect_statement2 (id INTEGER, name CHAR(20))");

    /* switch to manual commit mode */
    CHECK_DBC_RC(hdbc0, SQLSetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0));
    CHECK_DBC_RC(hdbc0, SQLGetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_OFF);

    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    FAIL_IF(mode == SQL_AUTOCOMMIT_OFF, "default auto commit should be on.");

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAM_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)rowcnt, 0));
    for (int i = 0; i < ATTR_STATUS_RECORD_ARRAY_SIZE; i++)
    {
        record[i].id      = (i + 1) * 10;
        record[i].idLen   = sizeof(record[i].id);
        rc = _snprintf_s(record[i].name, sizeof(record[i].name), sizeof(record[i].name) - 1, "data%d", i);
        record[i].nameLen = strlen(record[i].name);
    }

    /* now check commit affect on prepared statement */
    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER,
                  0, 0, &record[0].id, sizeof(record[0].id), &record[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR,
                  TRANS_AFFECT_RECORD_NAME_LEN, 0, record[0].name, sizeof(record[0].name), &record[0].nameLen));

    /* first prepare statment on hdbc1 */
    CHECK_STMT_RC(hstmt1, SQLPrepare(hstmt1, "INSERT INTO test_trans_affect_statement1 (id, name) VALUES (?,?)", SQL_NTS));

    /* then commit trans on hdbc0 */
    FAIL_IF(SQLExecDirect(hstmt0, "INSERT INTO test_trans_affect_statement (id, myValue) VALUES "
            "(10, 100),(20, 200),(30, 300)", SQL_NTS) != SQL_SUCCESS, "success expected");
    CHECK_DBC_RC(hdbc0, SQLEndTran(SQL_HANDLE_DBC, hdbc0, SQL_COMMIT));
    CHECK_STMT_RC(hstmt0, SQLFreeStmt(hstmt0, SQL_RESET_PARAMS));
    
    /* now commit statment on hdbc1 */
    CHECK_STMT_RC(hstmt1, SQLExecute(hstmt1));
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_RESET_PARAMS));

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetchedPtr, 0));

    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_trans_affect_statement1 ORDER BY id");

    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &record1[0].id, sizeof(record1[0].id), &record1[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_CHAR, record1[0].name, sizeof(record1[0].name), &record1[0].nameLen));

    CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));
    is_num(rowcnt, rowsFetchedPtr);

    for (int i = 0; i < rowcnt; i++)
    {
        is_num(record1[i].id, record[i].id);
        IS_STR(record1[i].name, record[i].name, strlen(record[i].name));
    }

    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    /* now check rollback affect on prepared statement */
    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER,
                  0, 0, &record[0].id, sizeof(record[0].id), &record[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR,
                  TRANS_AFFECT_RECORD_NAME_LEN, 0, record[0].name, sizeof(record[0].name), &record[0].nameLen));
    
    /* first prepare statment on hdbc1 */
    CHECK_STMT_RC(hstmt1, SQLPrepare(hstmt1, "INSERT INTO test_trans_affect_statement2 (id, name) VALUES (?,?)", SQL_NTS));

    /* then rollback trans on hdbc0 */
    FAIL_IF(SQLExecDirect(hstmt0, "INSERT INTO test_trans_affect_statement (id, myValue) VALUES "
            "(15, 100),(25, 200),(35, 300)", SQL_NTS) != SQL_SUCCESS, "success expected");
    CHECK_DBC_RC(hdbc0, SQLEndTran(SQL_HANDLE_DBC, hdbc0, SQL_ROLLBACK));
    CHECK_STMT_RC(hstmt0, SQLFreeStmt(hstmt0, SQL_RESET_PARAMS));
    
    /* now commit statment on hdbc1 */
    CHECK_STMT_RC(hstmt1, SQLExecute(hstmt1));
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_RESET_PARAMS));

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetchedPtr, 0));

    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_trans_affect_statement2 ORDER BY id");

    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &record1[0].id, sizeof(record1[0].id), &record1[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_CHAR, record1[0].name, sizeof(record1[0].name), &record1[0].nameLen));

    CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));
    is_num(rowcnt, rowsFetchedPtr);

    for (int i = 0; i < rowcnt; i++)
    {
        is_num(record1[i].id, record[i].id);
        IS_STR(record1[i].name, record[i].name, strlen(record[i].name));
    }
    
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_trans_affect_statement");
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_trans_affect_statement1");
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_trans_affect_statement2");

    ODBC_Disconnect(NULL, hdbc1, hstmt1);
    ODBC_Disconnect(henv0, hdbc0, hstmt0);

#undef ATTR_STATUS_RECORD_ARRAY_SIZE

    return OK;
}

/* switch from manual to auto will commit all trans */
ODBC_TEST(test_switch_force_commit)
{
#define ATTR_STATUS_RECORD_ARRAY_SIZE  3
    SQLHANDLE   henv0;
    SQLHANDLE   hdbc0;
    SQLHANDLE   hstmt0;
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLULEN      rowsFetchedPtr = 0;

    typedef struct {
        SQLINTEGER id;
        SQLINTEGER value;
        SQLLEN     idLen;
        SQLLEN     valueLen;
    }Record;

    SQLULEN      rowcnt = ATTR_STATUS_RECORD_ARRAY_SIZE;
    SQLINTEGER   row_size = sizeof(Record);
    Record       record[ATTR_STATUS_RECORD_ARRAY_SIZE];
    Record       record1[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLUINTEGER  mode = 0;

    ODBC_Connect(&henv0, &hdbc0, &hstmt0);
    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    for (int i = 0; i < ATTR_STATUS_RECORD_ARRAY_SIZE; i++)
    {
        record[i].id      = i;
        record[i].value   = i * 10;
    }

    OK_SIMPLE_STMT(hstmt0, "DROP TABLE IF EXISTS test_switch_force_commit");
    OK_SIMPLE_STMT(hstmt0, "CREATE TABLE test_switch_force_commit (id INTEGER, myValue INTEGER)");

    /* switch to manual commit mode */
    CHECK_DBC_RC(hdbc0, SQLSetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0));
    CHECK_DBC_RC(hdbc0, SQLGetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_OFF);
    
    FAIL_IF(SQLExecDirect(hstmt0, "INSERT INTO test_switch_force_commit (id, myValue) VALUES (0, 0),(1, 10),(2, 20)", SQL_NTS) != SQL_SUCCESS, "success expected");

    CHECK_STMT_RC(hstmt0, SQLFreeStmt(hstmt0, SQL_RESET_PARAMS));

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetchedPtr, 0));
    
    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_switch_force_commit ORDER BY id");

    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &record1[0].id, sizeof(record1[0].id), &record1[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_LONG, &record1[0].value, sizeof(record1[0].value), &record1[0].valueLen));

    /* trans not commit yet, can not see any data in hdbc1 */
    EXPECT_STMT(hstmt1, SQLFetch(hstmt1), SQL_NO_DATA);
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    /* hdbc0 switch to auto commit mode, hope to trigger commit action */
    CHECK_DBC_RC(hdbc0, SQLSetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0));
    CHECK_DBC_RC(hdbc0, SQLGetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_ON);

    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_switch_force_commit ORDER BY id");
    CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));

    is_num(rowcnt, rowsFetchedPtr);
    for (int i = 0; i < rowcnt; i++)
    {
        is_num(record1[i].id, record[i].id);
        is_num(record1[i].value, record[i].value);
    }

    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_switch_force_commit");

    ODBC_Disconnect(henv0, hdbc0, hstmt0);
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

#undef ATTR_STATUS_RECORD_ARRAY_SIZE

    return OK;
}

/* commit/rollback in auto commit mode */
ODBC_TEST(test_rollback_with_auto_commit)
{
#define ATTR_STATUS_RECORD_ARRAY_SIZE  3
    SQLHANDLE   henv0;
    SQLHANDLE   hdbc0;
    SQLHANDLE   hstmt0;
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLULEN      rowsFetchedPtr = 0;

    typedef struct {
        SQLINTEGER id;
        SQLINTEGER value;
        SQLLEN     idLen;
        SQLLEN     valueLen;
    }Record;

    SQLULEN      rowcnt = ATTR_STATUS_RECORD_ARRAY_SIZE;
    SQLINTEGER   row_size = sizeof(Record);
    Record       record[ATTR_STATUS_RECORD_ARRAY_SIZE];
    Record       record1[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLUINTEGER  mode = 0;

    ODBC_Connect(&henv0, &hdbc0, &hstmt0);
    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    for (int i = 0; i < ATTR_STATUS_RECORD_ARRAY_SIZE; i++)
    {
        record[i].id      = i;
        record[i].value   = i * 10;
    }

    OK_SIMPLE_STMT(hstmt0, "DROP TABLE IF EXISTS test_rollback_with_auto_commit");
    OK_SIMPLE_STMT(hstmt0, "CREATE TABLE test_rollback_with_auto_commit (id INTEGER, myValue INTEGER)");

    /* switch to manual commit mode */
    CHECK_DBC_RC(hdbc0, SQLSetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0));
    CHECK_DBC_RC(hdbc0, SQLGetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_ON);
    
    FAIL_IF(SQLExecDirect(hstmt0, "INSERT INTO test_rollback_with_auto_commit (id, myValue) VALUES (0, 0),(1, 10),(2, 20)", SQL_NTS) != SQL_SUCCESS, "success expected");

    CHECK_STMT_RC(hstmt0, SQLFreeStmt(hstmt0, SQL_RESET_PARAMS));

    CHECK_DBC_RC(hdbc0, SQLEndTran(SQL_HANDLE_DBC, hdbc0, SQL_ROLLBACK));

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetchedPtr, 0));

    /* check result: expect no impact in auto commit mode, data is integrated */
    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_rollback_with_auto_commit ORDER BY id");

    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &record1[0].id, sizeof(record1[0].id), &record1[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_LONG, &record1[0].value, sizeof(record1[0].value), &record1[0].valueLen));

    CHECK_DBC_RC(hdbc0, SQLGetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_ON);

    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_rollback_with_auto_commit ORDER BY id");
    CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));

    is_num(rowcnt, rowsFetchedPtr);
    for (int i = 0; i < rowcnt; i++)
    {
        is_num(record1[i].id, record[i].id);
        is_num(record1[i].value, record[i].value);
    }

    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_rollback_with_auto_commit");

    ODBC_Disconnect(henv0, hdbc0, hstmt0);
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

#undef ATTR_STATUS_RECORD_ARRAY_SIZE

    return OK;
}

/* commit/rollback in auto commit mode */
ODBC_TEST(test_commit_with_auto_commit)
{
#define ATTR_STATUS_RECORD_ARRAY_SIZE  3
    SQLHANDLE   henv0;
    SQLHANDLE   hdbc0;
    SQLHANDLE   hstmt0;
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLULEN      rowsFetchedPtr = 0;

    typedef struct {
        SQLINTEGER id;
        SQLINTEGER value;
        SQLLEN     idLen;
        SQLLEN     valueLen;
    }Record;

    SQLULEN      rowcnt = ATTR_STATUS_RECORD_ARRAY_SIZE;
    SQLINTEGER   row_size = sizeof(Record);
    Record       record[ATTR_STATUS_RECORD_ARRAY_SIZE];
    Record       record1[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLUINTEGER  mode = 0;

    ODBC_Connect(&henv0, &hdbc0, &hstmt0);
    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    for (int i = 0; i < ATTR_STATUS_RECORD_ARRAY_SIZE; i++)
    {
        record[i].id      = i;
        record[i].value   = i * 10;
    }

    OK_SIMPLE_STMT(hstmt0, "DROP TABLE IF EXISTS test_commit_with_auto_commit");
    OK_SIMPLE_STMT(hstmt0, "CREATE TABLE test_commit_with_auto_commit (id INTEGER, myValue INTEGER)");

    /* switch to manual commit mode */
    CHECK_DBC_RC(hdbc0, SQLSetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0));
    CHECK_DBC_RC(hdbc0, SQLGetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_ON);
    
    FAIL_IF(SQLExecDirect(hstmt0, "INSERT INTO test_commit_with_auto_commit (id, myValue) VALUES (0, 0),(1, 10),(2, 20)", SQL_NTS) != SQL_SUCCESS, "success expected");

    CHECK_STMT_RC(hstmt0, SQLFreeStmt(hstmt0, SQL_RESET_PARAMS));

    CHECK_DBC_RC(hdbc0, SQLEndTran(SQL_HANDLE_DBC, hdbc0, SQL_COMMIT));

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetchedPtr, 0));
    
    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_commit_with_auto_commit ORDER BY id");

    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &record1[0].id, sizeof(record1[0].id), &record1[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_LONG, &record1[0].value, sizeof(record1[0].value), &record1[0].valueLen));

    CHECK_DBC_RC(hdbc0, SQLGetConnectAttr(hdbc0, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_AUTOCOMMIT_ON);

    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_commit_with_auto_commit ORDER BY id");
    CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));

    is_num(rowcnt, rowsFetchedPtr);
    for (int i = 0; i < rowcnt; i++)
    {
        is_num(record1[i].id, record[i].id);
        is_num(record1[i].value, record[i].value);
    }

    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_commit_with_auto_commit");

    ODBC_Disconnect(henv0, hdbc0, hstmt0);
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

#undef ATTR_STATUS_RECORD_ARRAY_SIZE

    return OK;
}

MA_ODBC_TESTS my_tests[]=
{
    {test_trans_basic,               "test_trans_basic"},
    {test_empty_tran,                "test_commit_rollback_empty_tran"},
#if 1
    {test_manual_trans_basic,        "test_basic_manual_commit"},
    {test_end_env_trans,             "test_end_env_trans"},
    {test_ddl_force_commit,          "execute_ddl_will_force_commit_trans"},
    {test_rollback_trans,            "test_rollback_trans"},
    {test_rollback_trans1,           "test_rollback_trans_in_auto_commit_mode"},
    {test_trans_affect_cursor,       "test_trans_affect_on_cursor"},
    {test_trans_affect_statement,    "test_trans_affect_on_prepared_statement"},
    {test_rollback_with_auto_commit, "test_rollback_with_auto_commit_mode"},
    {test_commit_with_auto_commit,   "test_commit_with_auto_commit_mode"},
    {test_switch_force_commit,       "test_switch_to_auto_commit_will_force_commit"},
#endif
    {NULL, NULL}
};

int main(int argc, char **argv)
{
    int ret;
    int count = sizeof(my_tests) / sizeof(MA_ODBC_TESTS) - 1;
    
    get_options(argc, argv);
    plan(count);
    mark_all_tests_normal(my_tests);
    
    ret = run_tests(my_tests);

    return ret;
}

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

ODBC_TEST(test_attr_basic)
{
    SQLULEN     type = 0;
    SQLULEN     metaId;
    SQLINTEGER  odbc_version;
    SQLUINTEGER mode = 0;
    SQLRETURN   ret;

    CHECK_ENV_RC(Env, SQLGetEnvAttr(Env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)&odbc_version, 0, NULL));
    is_num(odbc_version, SQL_OV_ODBC3);
    
    CHECK_DBC_RC(Connection, SQLGetConnectAttr(Connection, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)&mode, 0, NULL));
    is_num(mode, SQL_MODE_READ_WRITE);

    ret = SQLSetConnectAttr(Connection, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_ONLY, 0);
    FAIL_IF(ret == SQL_SUCCESS, "expect fail, attribute access mode can not be readonly.")
    CHECK_DBC_RC(Connection, SQLSetConnectAttr(Connection, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_WRITE, 0));
    
    CHECK_STMT_RC(Stmt, SQLGetStmtAttr(Stmt, SQL_ATTR_NOSCAN, (SQLPOINTER)&type, 0, NULL));
    is_num(type, SQL_NOSCAN_ON);

    CHECK_STMT_RC(Stmt, SQLGetStmtAttr(Stmt, SQL_ATTR_METADATA_ID, (SQLPOINTER)&metaId, 0, NULL));
    is_num(metaId, SQL_FALSE);

    return OK;
}

ODBC_TEST(test_desc_attr_apd)
{
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLHANDLE   apd;
    SQLHANDLE   org;
    SQLHANDLE   apd1 = NULL;
    SQLINTEGER  len;
    
    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_APP_PARAM_DESC, (SQLPOINTER)&org, 0, &len));

    CHECK_DBC_RC(hdbc1, SQLAllocHandle(SQL_HANDLE_DESC, hdbc1, &apd));

    // set apd to new alloc ptr
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_APP_PARAM_DESC, apd, 0));
    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_APP_PARAM_DESC, (SQLPOINTER)&apd1, 0, &len));
    FAIL_IF(apd1 != apd, "expect origin pointer");

    // reset apd
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_APP_PARAM_DESC, NULL, 0));
    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_APP_PARAM_DESC, (SQLPOINTER)&apd1, 0, &len));
    FAIL_IF(apd1 != org, "expect origin pointer");

    CHECK_DBC_RC(hdbc1, SQLFreeHandle(SQL_HANDLE_DESC, apd));
        
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

    return OK;
}

ODBC_TEST(test_desc_attr_ard)
{
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLHANDLE   ard;
    SQLHANDLE   org;
    SQLHANDLE   ard1 = NULL;
    SQLINTEGER  len;
    
    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_APP_ROW_DESC, (SQLPOINTER)&org, 0, &len));

    CHECK_DBC_RC(hdbc1, SQLAllocHandle(SQL_HANDLE_DESC, hdbc1, &ard));

    // set ard to new alloc ptr
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_APP_ROW_DESC, ard, 0));
    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_APP_ROW_DESC, (SQLPOINTER)&ard1, 0, &len));
    FAIL_IF(ard1 != ard, "expect origin pointer");

    // reset ard
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_APP_ROW_DESC, NULL, 0));
    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_APP_ROW_DESC, (SQLPOINTER)&ard1, 0, &len));
    FAIL_IF(ard1 != org, "expect origin pointer");

    CHECK_DBC_RC(hdbc1, SQLFreeHandle(SQL_HANDLE_DESC, ard));
        
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

    return OK;
}

ODBC_TEST(test_desc_attr_basic)
{
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLHANDLE   ipd;
    SQLHANDLE   ird;
    
    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    CHECK_DBC_RC(hdbc1, SQLAllocHandle(SQL_HANDLE_DESC, hdbc1, &ipd));
    CHECK_DBC_RC(hdbc1, SQLAllocHandle(SQL_HANDLE_DESC, hdbc1, &ird));

    // not allow to set this fields
    FAIL_IF(SQLSetStmtAttr(hstmt1, SQL_ATTR_IMP_PARAM_DESC, ipd, 0) != SQL_ERROR, "Error expected");
    FAIL_IF(SQLSetStmtAttr(hstmt1, SQL_ATTR_IMP_ROW_DESC, ird, 0) != SQL_ERROR, "Error expected");

    CHECK_DBC_RC(hdbc1, SQLFreeHandle(SQL_HANDLE_DESC, ipd));
    CHECK_DBC_RC(hdbc1, SQLFreeHandle(SQL_HANDLE_DESC, ird));
    
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

    return OK;
}

/*
    test SQL_ATTR_APP_PARAM_DESC/SQL_ATTR_IMP_PARAM_DESC indirect bind by SQLBindParameter
    note: APD --- application parameter descriptor, a type of application buffer
          IPD --- implementation parameter descriptor, a type of implementation buffer
*/
ODBC_TEST(test_stmt_desc_attr)
{
#define ATTR_STATUS_RECORD_NAME_LEN    20
#define ATTR_STATUS_RECORD_ARRAY_SIZE  5
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLHANDLE   apd;
    SQLHANDLE   ipd;
    SQLULEN      rowsFetchedPtr = 0;
    SQLRETURN    rc;

    typedef struct {
        SQLINTEGER id;
        SQLCHAR name[ATTR_STATUS_RECORD_NAME_LEN + 1];
        SQLLEN idLen;
        SQLLEN nameLen;
    }Record;
    SQLULEN      rowcnt = ATTR_STATUS_RECORD_ARRAY_SIZE;
    SQLINTEGER   row_size = sizeof(Record);
    Record       record0[ATTR_STATUS_RECORD_ARRAY_SIZE];
    Record       record[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLUSMALLINT paramStatusArray[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLULEN      paramsProcessed;

    ODBC_Connect(&henv1, &hdbc1, &hstmt1);
    
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_stmt_desc_attr");
    OK_SIMPLE_STMT(hstmt1, "CREATE TABLE test_stmt_desc_attr (id INTEGER, name CHAR(20))");

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAM_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAM_STATUS_PTR, paramStatusArray, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMS_PROCESSED_PTR, &paramsProcessed, 0));
    for (int i = 0; i < ATTR_STATUS_RECORD_ARRAY_SIZE; i++)
    {
        record0[i].id      = (i + 1) * 10;
        record0[i].idLen   = sizeof(record0[i].id);
        rc = _snprintf_s(record0[i].name, sizeof(record0[i].name), sizeof(record0[i].name) - 1, "data%d", i);
        record0[i].nameLen = strlen(record0[i].name);
    }
    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER,
                  0, 0, &record0[0].id, sizeof(record0[0].id), &record0[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR,
                  ATTR_STATUS_RECORD_NAME_LEN, 0, record0[0].name, sizeof(record0[0].name), &record0[0].nameLen));

    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_APP_PARAM_DESC, &apd, SQL_IS_POINTER, NULL));
    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_IMP_PARAM_DESC, &ipd, SQL_IS_POINTER, NULL));

    // check the APD and IPD
    {
        SQLPOINTER  ValuePtr;
        SQLPOINTER  indicatorPtr;
        SQLUSMALLINT paramType;
        SQLLEN       colSize;
        
        CHECK_STMT_RC(hstmt1, SQLGetDescField(apd, 1, SQL_DESC_DATA_PTR, (SQLPOINTER)&ValuePtr, 0, NULL));
        FAIL_IF(ValuePtr != &record0[0].id, "expect origin data pointer");
        CHECK_STMT_RC(hstmt1, SQLGetDescField(apd, 2, SQL_DESC_DATA_PTR, (SQLPOINTER)&ValuePtr, 0, NULL));
        FAIL_IF(ValuePtr != &record0[0].name, "expect origin data pointer");

        CHECK_DESC_RC(apd, SQLGetDescField(apd, 1, SQL_DESC_OCTET_LENGTH, &colSize, SQL_IS_INTEGER, NULL));
        is_num(colSize, sizeof(SQLINTEGER));

        CHECK_STMT_RC(hstmt1, SQLGetDescField(apd, 1, SQL_DESC_INDICATOR_PTR, (SQLPOINTER)&indicatorPtr, 0, NULL));
        FAIL_IF(indicatorPtr != &record0[0].idLen, "expect origin data pointer");
        CHECK_STMT_RC(hstmt1, SQLGetDescField(apd, 2, SQL_DESC_INDICATOR_PTR, (SQLPOINTER)&indicatorPtr, 0, NULL));
        FAIL_IF(indicatorPtr != &record0[0].nameLen, "expect origin data pointer");

        CHECK_STMT_RC(hstmt1, SQLGetDescField(apd, 1, SQL_DESC_OCTET_LENGTH_PTR, (SQLPOINTER)&indicatorPtr, 0, NULL));
        FAIL_IF(indicatorPtr != &record0[0].idLen, "expect origin data pointer");
        CHECK_STMT_RC(hstmt1, SQLGetDescField(apd, 2, SQL_DESC_OCTET_LENGTH_PTR, (SQLPOINTER)&indicatorPtr, 0, NULL));
        FAIL_IF(indicatorPtr != &record0[0].nameLen, "expect origin data pointer");

        CHECK_STMT_RC(hstmt1, SQLGetDescField(ipd, 1, SQL_DESC_PARAMETER_TYPE, (SQLPOINTER)&paramType, 0, NULL));
        FAIL_IF(paramType != SQL_PARAM_INPUT, "expect type equals input");
        CHECK_STMT_RC(hstmt1, SQLGetDescField(ipd, 2, SQL_DESC_PARAMETER_TYPE, (SQLPOINTER)&paramType, 0, NULL));
        FAIL_IF(paramType != SQL_PARAM_INPUT, "expect type equals input");

        CHECK_STMT_RC(hstmt1, SQLGetDescField(ipd, 2, SQL_DESC_LENGTH, (SQLPOINTER)&colSize, 0, NULL));
        FAIL_IF(colSize != ATTR_STATUS_RECORD_NAME_LEN, "expect columnsize equals input");
    }

    FAIL_IF(SQLExecDirect(hstmt1, "INSERT INTO test_stmt_desc_attr (id, name) VALUES (?,?)", SQL_NTS) != SQL_SUCCESS, "success expected");
    is_num(paramsProcessed, rowcnt);
    for (int i = 0; i < rowcnt; ++i) {
        if (paramStatusArray[i] != SQL_PARAM_SUCCESS
            && paramStatusArray[i] != SQL_PARAM_SUCCESS_WITH_INFO)
        {
            diag("Parameter #%u status isn't successful(0x%X)", i, paramStatusArray[i]);
            return FAIL;
        }
    }

    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_RESET_PARAMS));

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetchedPtr, 0));
    
    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_stmt_desc_attr ORDER BY id");

    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &record[0].id, sizeof(record[0].id), &record[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_CHAR, record[0].name, sizeof(record[0].name), &record[0].nameLen));
    
    CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));

    is_num(rowcnt, rowsFetchedPtr);
    // check if SQL_ATTR_ROW_BIND_TYPE makes sense, by checking whether results filled in right pos 
    for (int i = 0; i < rowcnt; i++)
    {
        is_num(record[i].id, record0[i].id);
        IS_STR(record[i].name, record0[i].name, strlen(record0[i].name));
    }

    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_stmt_desc_attr");
    
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

#undef ATTR_STATUS_RECORD_NAME_LEN
#undef ATTR_STATUS_RECORD_ARRAY_SIZE

    return OK;
}

/*
    test SQL_ATTR_APP_ROW_DESC/SQL_ATTR_IMP_ROW_DESC indirect bind by SQLBindCol
    note: ARD --- application row descriptor, a type of application buffer
          IRD --- implementation row descriptor, a type of implementation buffer, which 
                  contains the row from the database
*/
ODBC_TEST(test_stmt_desc_attr1)
{
#define ATTR_STATUS_RECORD_NAME_LEN    20
#define ATTR_STATUS_RECORD_ARRAY_SIZE  5
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLHANDLE   ard;
    SQLHANDLE   ird;
    SQLUSMALLINT rowStatusPtr[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLULEN      rowsFetchedPtr = 0;

    typedef struct {
        SQLINTEGER id;
        SQLCHAR name[ATTR_STATUS_RECORD_NAME_LEN + 1];
        SQLLEN idLen;
        SQLLEN nameLen;
    }Record;
    SQLULEN      rowcnt = ATTR_STATUS_RECORD_ARRAY_SIZE;
    SQLINTEGER   row_size = sizeof(Record);
    Record       record[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLCHAR      recordName[ATTR_STATUS_RECORD_NAME_LEN + 1];

    ODBC_Connect(&henv1, &hdbc1, &hstmt1);
    
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_stmt_desc_attr1");
    OK_SIMPLE_STMT(hstmt1, "CREATE TABLE test_stmt_desc_attr1 (id INTEGER, name CHAR(20))");

    FAIL_IF(SQLExecDirect(hstmt1, "INSERT INTO test_stmt_desc_attr1 (id, name) VALUES (0,'data0'),(1,'data1'),"
            "(2,'data2'),(3,'data3'),(4,'data4')", SQL_NTS) != SQL_SUCCESS, "success expected");

    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetchedPtr, 0));
    
    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_stmt_desc_attr1 ORDER BY id");

    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_APP_ROW_DESC, &ard, SQL_IS_POINTER, NULL));
    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_IMP_ROW_DESC, &ird, SQL_IS_POINTER, NULL));

    CHECK_DESC_RC(ird, SQLSetDescField(ird, 0, SQL_DESC_ARRAY_STATUS_PTR, (SQLPOINTER)rowStatusPtr, SQL_IS_POINTER));
  
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &record[0].id, sizeof(record[0].id), &record[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_CHAR, record[0].name, sizeof(record[0].name), &record[0].nameLen));

    // check the ARD impact by SQLBindCol
    {
        SQLPOINTER  ValuePtr;
        SQLINTEGER  targetType;
        SQLPOINTER  indPtr;

        CHECK_STMT_RC(hstmt1, SQLGetDescField(ard, 1, SQL_DESC_DATA_PTR, (SQLPOINTER)&ValuePtr, 0, NULL));
        FAIL_IF(ValuePtr != &record[0].id, "expect origin data pointer");
        CHECK_STMT_RC(hstmt1, SQLGetDescField(ard, 2, SQL_DESC_DATA_PTR, (SQLPOINTER)&ValuePtr, 0, NULL));
        FAIL_IF(ValuePtr != &record[0].name[0], "expect origin data pointer");

        CHECK_STMT_RC(hstmt1, SQLGetDescField(ard, 1, SQL_DESC_TYPE, (SQLPOINTER)&targetType, 0, NULL));
        FAIL_IF(targetType != SQL_C_LONG, "expect SQL_C_LONG");
        CHECK_STMT_RC(hstmt1, SQLGetDescField(ard, 2, SQL_DESC_TYPE, (SQLPOINTER)&targetType, 0, NULL));
        FAIL_IF(targetType != SQL_C_CHAR, "expect SQL_C_CHAR");

        CHECK_STMT_RC(hstmt1, SQLGetDescField(ard, 1, SQL_DESC_OCTET_LENGTH_PTR, (SQLPOINTER)&indPtr, 0, NULL));
        FAIL_IF(indPtr != &record[0].idLen, "expect origin data pointer");
        CHECK_STMT_RC(hstmt1, SQLGetDescField(ard, 2, SQL_DESC_OCTET_LENGTH_PTR, (SQLPOINTER)&indPtr, 0, NULL));
        FAIL_IF(indPtr != &record[0].nameLen, "expect origin data pointer");

        CHECK_STMT_RC(hstmt1, SQLGetDescField(ard, 1, SQL_DESC_INDICATOR_PTR, (SQLPOINTER)&indPtr, 0, NULL));
        FAIL_IF(indPtr != &record[0].idLen, "expect origin data pointer");
        CHECK_STMT_RC(hstmt1, SQLGetDescField(ard, 2, SQL_DESC_INDICATOR_PTR, (SQLPOINTER)&indPtr, 0, NULL));
        FAIL_IF(indPtr != &record[0].nameLen, "expect origin data pointer");
    }
    
    CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));

    {
        SQLINTEGER  targetType;

        // check IRD
        CHECK_STMT_RC(hstmt1, SQLGetDescField(ird, 1, SQL_DESC_TYPE, (SQLPOINTER)&targetType, 0, NULL));
        is_num(targetType, SQL_INTEGER);

        CHECK_STMT_RC(hstmt1, SQLGetDescField(ird, 2, SQL_DESC_TYPE, (SQLPOINTER)&targetType, 0, NULL));
        is_num(targetType, SQL_CHAR);

        is_num(rowcnt, rowsFetchedPtr);
        // check if SQL_ATTR_ROW_BIND_TYPE makes sense, by checking whether results filled in right pos 
        for (int i = 0; i < rowcnt; i++)
        {
            is_num(record[i].id, i);
            (void)_snprintf_s(recordName, sizeof(recordName), sizeof(recordName) - 1, "data%d", i);
            IS_STR(record[i].name, recordName, strlen(recordName));
            is_num(rowStatusPtr[i], SQL_SUCCESS);
        }
    }

    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_stmt_desc_attr1");
    
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

#undef ATTR_STATUS_RECORD_NAME_LEN
#undef ATTR_STATUS_RECORD_ARRAY_SIZE

    return OK;
}

/*
 * Tests for paramset:
 * SQL_ATTR_PARAMSET_SIZE (apd->array_size)
 * SQL_ATTR_PARAM_STATUS_PTR (ipd->array_status_ptr)
 * SQL_ATTR_PARAM_OPERATION_PTR (apd->array_status_ptr)
 * SQL_ATTR_PARAMS_PROCESSED_PTR (apd->rows_processed_ptr)
 */
ODBC_TEST(test_desc_status_array)
{
#define DESCRIPTOR_PARAM_ARRAY_SIZE    4
    SQLULEN       parsetsize= DESCRIPTOR_PARAM_ARRAY_SIZE;
    SQLUSMALLINT  parstatus[DESCRIPTOR_PARAM_ARRAY_SIZE];
    SQLUSMALLINT  parop[DESCRIPTOR_PARAM_ARRAY_SIZE]; /* operation */
    SQLULEN       pardone; /* processed */
    SQLHANDLE     ipd;
    SQLHANDLE     apd;
    SQLINTEGER    params1[DESCRIPTOR_PARAM_ARRAY_SIZE];
    SQLINTEGER    params2[DESCRIPTOR_PARAM_ARRAY_SIZE];

    parop[0]= SQL_PARAM_PROCEED;
    parop[1]= SQL_PARAM_IGNORE;
    parop[2]= SQL_PARAM_IGNORE;
    parop[3]= SQL_PARAM_PROCEED;
    params1[0]= 0;
    params1[1]= 1;
    params1[2]= 2;
    params1[3]= 3;
    params2[0]= 100;
    params2[1]= 101;
    params2[2]= 102;
    params2[3]= 103;

    /* get the descriptors */
    CHECK_STMT_RC(Stmt, SQLGetStmtAttr(Stmt, SQL_ATTR_APP_PARAM_DESC, &apd, SQL_IS_POINTER, NULL));
    CHECK_STMT_RC(Stmt, SQLGetStmtAttr(Stmt, SQL_ATTR_IMP_PARAM_DESC, &ipd, SQL_IS_POINTER, NULL));

    /* set the fields */
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)parsetsize, 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAM_STATUS_PTR, parstatus, 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAM_OPERATION_PTR, parop, 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &pardone, 0));

    /* verify the fields */
    {
        SQLPOINTER  x_parstatus, x_parop, x_pardone;
        SQLULEN     x_parsetsize;

        CHECK_DESC_RC(apd, SQLGetDescField(apd, 0, SQL_DESC_ARRAY_SIZE, &x_parsetsize, SQL_IS_UINTEGER, NULL));
        CHECK_DESC_RC(ipd, SQLGetDescField(ipd, 0, SQL_DESC_ARRAY_STATUS_PTR, &x_parstatus, SQL_IS_POINTER, NULL));

        CHECK_DESC_RC(apd, SQLGetDescField(apd, 0, SQL_DESC_ARRAY_STATUS_PTR, &x_parop, SQL_IS_POINTER, NULL));

        CHECK_DESC_RC(ipd, SQLGetDescField(ipd, 0, SQL_DESC_ROWS_PROCESSED_PTR, &x_pardone, SQL_IS_POINTER, NULL));

        is_num(x_parsetsize, parsetsize);
        FAIL_IF(x_parstatus != parstatus, "x_parstatus != parstatus");
        FAIL_IF(x_parop != parop, "x_parop != parop");
        FAIL_IF(x_pardone != &pardone, "x_pardone != pardone");
    }

    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

#undef DESCRIPTOR_PARAM_ARRAY_SIZE

    return OK;
}

ODBC_TEST(test_cursor_type)
{
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLULEN     curType;
    
    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    CHECK_DBC_RC(Connection, SQLGetConnectAttr(Connection, SQL_ATTR_ODBC_CURSORS, (SQLPOINTER)&curType, 0, NULL));
    is_num(curType, SQL_CUR_USE_DRIVER);
    
    // default cursor type is SQL_CURSOR_STATIC
    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)&curType, 0, NULL));
    is_num(curType, SQL_CURSOR_STATIC);

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_FORWARD_ONLY, 0));

    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)&curType, 0, NULL));
    is_num(curType, SQL_CURSOR_FORWARD_ONLY); // SQL_CURSOR_FORWARD_ONLY is the basic cursor type in ODBC core level
    
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_DYNAMIC, 0));

    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)&curType, 0, NULL));
    is_num(curType, SQL_CURSOR_STATIC); // SQL_CURSOR_DYNAMIC not make sense in ODBC core level, will reset to SQL_CURSOR_STATIC
    
    ODBC_Disconnect(henv1, hdbc1, hstmt1);

    return OK;
}

ODBC_TEST(test_cursor_attribute)
{
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLULEN     type;
    SQLULEN     attributes;
    SQLUINTEGER properties;

    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    CHECK_DBC_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_CURSOR_SENSITIVITY, (SQLPOINTER)&attributes, sizeof(attributes), NULL));
    is_num(attributes, SQL_UNSPECIFIED);

    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_SCROLL_OPTIONS, (SQLPOINTER)&properties, sizeof(properties), NULL));
    is_num(properties, SQL_SO_FORWARD_ONLY|SQL_SO_STATIC);
    
    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_STATIC_CURSOR_ATTRIBUTES1, (SQLPOINTER)&properties, sizeof(properties), NULL));
    is_num(properties , (SQL_CA1_ABSOLUTE|SQL_CA1_BOOKMARK|SQL_CA1_NEXT|
                         SQL_CA1_RELATIVE|SQL_CA1_LOCK_NO_CHANGE|SQL_CA1_POS_POSITION));

    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_STATIC_CURSOR_ATTRIBUTES2, (SQLPOINTER)&properties, sizeof(properties), NULL));
    is_num(properties , (SQL_CA2_CRC_EXACT|SQL_CA2_MAX_ROWS_DELETE|SQL_CA2_MAX_ROWS_INSERT|
                         SQL_CA2_MAX_ROWS_SELECT|SQL_CA2_MAX_ROWS_UPDATE));

    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1, (SQLPOINTER)&properties, sizeof(properties), NULL));
    is_num(properties , (SQL_CA1_ABSOLUTE|SQL_CA1_NEXT|SQL_CA1_RELATIVE|
                         SQL_CA1_LOCK_NO_CHANGE|SQL_CA1_POS_POSITION));

    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2, (SQLPOINTER)&properties, sizeof(properties), NULL));
    is_num(properties , (SQL_CA2_CRC_EXACT|SQL_CA2_MAX_ROWS_DELETE|SQL_CA2_MAX_ROWS_INSERT|
                         SQL_CA2_MAX_ROWS_SELECT|SQL_CA2_MAX_ROWS_UPDATE));

    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_DYNAMIC_CURSOR_ATTRIBUTES1, (SQLPOINTER)&properties, sizeof(properties), NULL));
    is_num(properties , 0);
    
    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_DYNAMIC_CURSOR_ATTRIBUTES2, (SQLPOINTER)&properties, sizeof(properties), NULL));
    is_num(properties , 0);
    
    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_KEYSET_CURSOR_ATTRIBUTES1, (SQLPOINTER)&properties, sizeof(properties), NULL));
    is_num(properties , 0);
    
    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_KEYSET_CURSOR_ATTRIBUTES2, (SQLPOINTER)&properties, sizeof(properties), NULL));
    is_num(properties , 0);

    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_STATIC_SENSITIVITY, (SQLPOINTER)&properties, sizeof(properties), NULL));
    is_num(properties, 0);

    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_POS_OPERATIONS, (SQLPOINTER)&properties, sizeof(properties), NULL));
    is_num(properties, SQL_POS_POSITION);
    
    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_CURSOR_SCROLLABLE, (SQLPOINTER)&type, sizeof(type), NULL));
    is_num(type, SQL_CURSOR_STATIC);

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_CURSOR_SCROLLABLE, (SQLPOINTER)SQL_NONSCROLLABLE, 0));

    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_cursor_attribute");
    OK_SIMPLE_STMT(hstmt1, "CREATE TABLE test_cursor_attribute (id INTEGER, name VARCHAR(20))");
    OK_SIMPLE_STMT(hstmt1, "INSERT INTO test_cursor_attribute VALUES (0,'MyData0'),(1,'MyData1'),"
                   "(2,'MyData2'),(3,'MyData3'),(4,'MyData4')");

    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_CURSOR_SCROLLABLE, (SQLPOINTER)&type, sizeof(type), NULL));
    is_num(type, SQL_CURSOR_FORWARD_ONLY);

    OK_SIMPLE_STMT(hstmt1,"SELECT * FROM test_cursor_attribute ORDER BY id");

    // test scroll disable with SQL_NONSCROLLABLE
    FAIL_IF(SQLFetchScroll(hstmt1, SQL_FETCH_FIRST, 0L) == SQL_SUCCESS, "expect no scrollable");

    /* free the statement cursor */
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_cursor_attribute");

    ODBC_Disconnect(henv1, hdbc1, hstmt1);

    return OK; 
}

ODBC_TEST(test_scroll_cursor)
{
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;

    SQLCHAR     data[MAX_NAME_LEN];

    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_scroll_cursor");
    OK_SIMPLE_STMT(hstmt1, "CREATE TABLE test_scroll_cursor (id INTEGER, name VARCHAR(20))");
    OK_SIMPLE_STMT(hstmt1, "INSERT INTO test_scroll_cursor VALUES (0,'MyData0'),(1,'MyData1'),"
                   "(2,'MyData2'),(3,'MyData3'),(4,'MyData4')");

    /* Open the resultset of table 'my_demo_cursor' */
    OK_SIMPLE_STMT(hstmt1,"SELECT * FROM test_scroll_cursor ORDER BY id");
    
    /* goto the last row */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_LAST, 0L));
    is_num(my_fetch_int(hstmt1, 1), 4);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData4", strlen("MyData4"));

    /* goto the pre of last row */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_PRIOR, 0L));
    is_num(my_fetch_int(hstmt1, 1), 3);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData3", strlen("MyData3"));

    /* goto the first row */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_FIRST, 0L));
    is_num(my_fetch_int(hstmt1, 1), 0);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData0", strlen("MyData0"));
    
    /* goto row 1 */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_ABSOLUTE, 1L));
    is_num(my_fetch_int(hstmt1, 1), 0);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData0", strlen("MyData0"));

    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_NEXT, 0L));
    is_num(my_fetch_int(hstmt1, 1), 1);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData1", strlen("MyData1"));

    /* move 2 rows foward */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_RELATIVE, 2L));
    is_num(my_fetch_int(hstmt1, 1), 3);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData3", strlen("MyData3"));

    /* move 3 rows backward */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_RELATIVE, -3L));
    is_num(my_fetch_int(hstmt1, 1), 0);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData0", strlen("MyData0"));
    
    /* goto row 4 */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_ABSOLUTE, 4L));
    is_num(my_fetch_int(hstmt1, 1), 3);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData3", strlen("MyData3"));
    
    /* free the statement cursor */
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_scroll_cursor");

    ODBC_Disconnect(henv1, hdbc1, hstmt1);

    return OK;
}

// test set cursor position by SQLSetPos
ODBC_TEST(test_cursor_set_pos)
{
#define CURSOR_SET_POS_RECORD_NAME_LEN    20
#define CURSOR_SET_POS_RECORD_ARRAY_SIZE  5

    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;

    typedef struct {
        SQLINTEGER id;
        SQLCHAR name[CURSOR_SET_POS_RECORD_NAME_LEN + 1];
        SQLLEN idLen;
        SQLLEN nameLen;
    }Record;
    Record record[CURSOR_SET_POS_RECORD_ARRAY_SIZE];
    SQLULEN type;

    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_cursor_set_pos");
    OK_SIMPLE_STMT(hstmt1, "CREATE TABLE test_cursor_set_pos (id INTEGER, name VARCHAR(20))");
    OK_SIMPLE_STMT(hstmt1, "INSERT INTO test_cursor_set_pos VALUES (1,'MyData1'),"
                   "(2,'MyData2'),(3,'MyData3'),(4,'MyData4'),(5,'MyData5')");

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));

    /* Open the resultset of table */
    OK_SIMPLE_STMT(hstmt1,"SELECT * FROM test_cursor_set_pos ORDER BY id");

    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &record[0].id, sizeof(record[0].id), &record[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_CHAR, record[0].name, sizeof(record[0].name), &record[0].nameLen));

    /* test set position by SQLSetPos */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_FIRST, 0L)); //fetch any row so as to we can call SQLSetPos
    CHECK_STMT_RC(hstmt1, SQLSetPos(hstmt1, 3, SQL_POSITION, SQL_LOCK_NO_CHANGE));
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_RELATIVE, 0L));
    is_num(record[0].id, 3);
    IS_STR(record[0].name, "MyData3", strlen("MyData3"));

    CHECK_STMT_RC(hstmt1, SQLSetPos(hstmt1, 1, SQL_POSITION, SQL_LOCK_NO_CHANGE));
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_RELATIVE, 0L));
    is_num(record[0].id, 1);
    IS_STR(record[0].name, "MyData1", strlen("MyData1"));

    CHECK_STMT_RC(hstmt1, SQLSetPos(hstmt1, 5, SQL_POSITION, SQL_LOCK_NO_CHANGE));
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_RELATIVE, 0L));
    is_num(record[0].id, 5);
    IS_STR(record[0].name, "MyData5", strlen("MyData5"));

    /* free the statement cursor */
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    /* now change the cursor type to forward only */
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_CURSOR_SCROLLABLE, (SQLPOINTER)SQL_NONSCROLLABLE, 0));
    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_CURSOR_SCROLLABLE, (SQLPOINTER)&type, sizeof(type), NULL));
    is_num(type, SQL_CURSOR_FORWARD_ONLY);

    OK_SIMPLE_STMT(hstmt1,"SELECT * FROM test_cursor_set_pos ORDER BY id");

    FAIL_IF(SQLFetchScroll(hstmt1, SQL_FETCH_RELATIVE, 0L) == SQL_SUCCESS, "expect no scrollable");

    /* free the statement cursor */
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    /* now change to forward only cusrsors */
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_FORWARD_ONLY, 0));

    /* Open the resultset of table */
    OK_SIMPLE_STMT(hstmt1,"SELECT * FROM test_cursor_set_pos ORDER BY id");

    /* test set position by SQLSetPos */
    CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1)); //fetch any row so as to we can call SQLSetPos
    CHECK_STMT_RC(hstmt1, SQLSetPos(hstmt1, 3, SQL_POSITION, SQL_LOCK_NO_CHANGE));
    CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));
    is_num(record[0].id, 3);
    IS_STR(record[0].name, "MyData3", strlen("MyData3"));

    /* free the statement cursor */
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_cursor_set_pos");

    ODBC_Disconnect(henv1, hdbc1, hstmt1);

#undef      CURSOR_SET_POS_RECORD_NAME_LEN
#undef      CURSOR_SET_POS_RECORD_ARRAY_SIZE

    return OK;
}

// test bookmark cursor operations
ODBC_TEST(test_bookmark_cursor)
{
#define BOOKMARK_CURSOR_RECORD_NAME_LEN    20
#define BOOKMARK_CURSOR_RECORD_ARRAY_SIZE  5

    typedef struct {
        SQLINTEGER id;
        SQLCHAR name[BOOKMARK_CURSOR_RECORD_NAME_LEN + 1];
        SQLLEN idLen;
        SQLLEN nameLen;
    }Record;

    SQLCHAR     data[MAX_NAME_LEN];
    Record      record[BOOKMARK_CURSOR_RECORD_ARRAY_SIZE];
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLINTEGER  bookmarkType;
    SQLINTEGER  bookmarkType1;
    SQLINTEGER  bookMark = 1;

    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    bookmarkType = SQL_UB_FIXED;
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_USE_BOOKMARKS, (SQLPOINTER)SQL_UB_FIXED, sizeof(bookmarkType)));
    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_USE_BOOKMARKS, &bookmarkType1, 0, NULL));
    is_num(bookmarkType1, bookmarkType);
    
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_bookmark_cursor");
    OK_SIMPLE_STMT(hstmt1, "CREATE TABLE test_bookmark_cursor (id INTEGER, name VARCHAR(20))");
    OK_SIMPLE_STMT(hstmt1, "INSERT INTO test_bookmark_cursor VALUES (0,'MyData0'),(1,'MyData1'),(2,'MyData2'),(3,'MyData3'),(4,'MyData4')");

    /* Open the resultset of table 'my_demo_cursor' */
    OK_SIMPLE_STMT(hstmt1,"SELECT * FROM test_bookmark_cursor ORDER BY id");

    /* now bind cursor as bookmark (column 0 is for bookmark binding) */
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 0, SQL_C_BOOKMARK, &bookMark, sizeof(bookMark), NULL));

    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &record[0].id, sizeof(record[0].id), &record[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_CHAR, record[0].name, sizeof(record[0].name), &record[0].nameLen));

    /* set bookmark value/position and check bookmark row */
    bookMark = 1;
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_BOOKMARK, 0L));
    is_num(my_fetch_int(hstmt1, 1), 1);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData1", strlen("MyData1"));

    /* get pre of current position by bookmark */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_BOOKMARK, -1L));
    is_num(my_fetch_int(hstmt1, 1), 0);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData0", strlen("MyData0"));
    is_num(bookMark, 0); /* now bookmark move to a backward position */

    bookMark = 1; /* reset bookmark position to initial value */

    /* get next of current position by bookmark */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_BOOKMARK, 1L));
    is_num(my_fetch_int(hstmt1, 1), 2);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData2", strlen("MyData2"));
    is_num(bookMark, 2); /* now bookmark move to a foward position */

    /* test get current bookmark position */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_BOOKMARK, 0L));
    is_num(my_fetch_int(hstmt1, 1), 2);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData2", strlen("MyData2"));
    is_num(bookMark, 2); /* now bookmark remain same */

    /* free the statement cursor */
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_bookmark_cursor");

    ODBC_Disconnect(henv1, hdbc1, hstmt1);

#undef BOOKMARK_CURSOR_RECORD_NAME_LEN
#undef BOOKMARK_CURSOR_RECORD_ARRAY_SIZE

    return OK;
}

// test bookmark cursor mixed with other scroll cursor
ODBC_TEST(test_bookmark_cursor1)
{
#define BOOKMARK_CURSOR_RECORD_NAME_LEN    20
#define BOOKMARK_CURSOR_RECORD_ARRAY_SIZE  5

    typedef struct {
        SQLINTEGER id;
        SQLCHAR name[BOOKMARK_CURSOR_RECORD_NAME_LEN + 1];
        SQLLEN idLen;
        SQLLEN nameLen;
    }Record;

    SQLCHAR     data[MAX_NAME_LEN];
    Record      record[BOOKMARK_CURSOR_RECORD_ARRAY_SIZE];
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;
    SQLINTEGER  bookmarkType;
    SQLINTEGER  bookmarkType1;
    SQLINTEGER  bookMark = 1;

    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    bookmarkType = SQL_UB_VARIABLE;
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_USE_BOOKMARKS, (SQLPOINTER)SQL_UB_VARIABLE, sizeof(bookmarkType)));
    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_USE_BOOKMARKS, &bookmarkType1, 0, NULL));
    is_num(bookmarkType1, bookmarkType);
    
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_bookmark_cursor");
    OK_SIMPLE_STMT(hstmt1, "CREATE TABLE test_bookmark_cursor (id INTEGER, name VARCHAR(20))");
    OK_SIMPLE_STMT(hstmt1, "INSERT INTO test_bookmark_cursor VALUES (0,'MyData0'),(1,'MyData1'),(2,'MyData2'),(3,'MyData3'),(4,'MyData4')");

    /* Open the resultset of table 'my_demo_cursor' */
    OK_SIMPLE_STMT(hstmt1,"SELECT * FROM test_bookmark_cursor ORDER BY id");

    /* now bind cursor as bookmark (column 0 is for bookmark binding) */
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 0, SQL_C_VARBOOKMARK, &bookMark, sizeof(bookMark), NULL));

    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &record[0].id, sizeof(record[0].id), &record[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_CHAR, record[0].name, sizeof(record[0].name), &record[0].nameLen));

    /* set bookmark value/position and check bookmark row */
    bookMark = 1;
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_BOOKMARK, 0L));
    is_num(my_fetch_int(hstmt1, 1), 1);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData1", strlen("MyData1"));

    /* get pre of current position by bookmark */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_BOOKMARK, -1L));
    is_num(my_fetch_int(hstmt1, 1), 0);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData0", strlen("MyData0"));
    is_num(bookMark, 0); /* now bookmark move to a backward position */

    bookMark = 1; /* reset bookmark position to initial value */

    /* get next of current position by bookmark */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_BOOKMARK, 1L));
    is_num(my_fetch_int(hstmt1, 1), 2);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData2", strlen("MyData2"));
    is_num(bookMark, 2); /* now bookmark move to a foward position */

    /* test get current bookmark position */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_BOOKMARK, 0L));
    is_num(my_fetch_int(hstmt1, 1), 2);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData2", strlen("MyData2"));
    is_num(bookMark, 2); /* now bookmark remain same */

    /* goto the first row */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_FIRST, 0L));
    is_num(my_fetch_int(hstmt1, 1), 0);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData0", strlen("MyData0"));
    is_num(bookMark, 0); /* now bookmark comes to first */

    /* goto the last row */
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_LAST, 0L));
    is_num(my_fetch_int(hstmt1, 1), 4);
    IS_STR(my_fetch_str(hstmt1, data, 2), "MyData4", strlen("MyData4"));
    is_num(bookMark, 4); /* now bookmark comes to last */

    /* free the statement cursor */
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_bookmark_cursor");

    ODBC_Disconnect(henv1, hdbc1, hstmt1);

#undef BOOKMARK_CURSOR_RECORD_NAME_LEN
#undef BOOKMARK_CURSOR_RECORD_ARRAY_SIZE

    return OK;
}

/*
  Test basic handling of SQL_ATTR_PARAM_BIND_OFFSET_PTR
*/
ODBC_TEST(test_bind_offset)
{
#define MAX_BIND_TEST_ROW_NUM    25
    const SQLINTEGER rowcnt = 5;
    SQLINTEGER i;
    typedef struct {
        SQLINTEGER id;
        SQLINTEGER x;
    }DataInfo;
    DataInfo rows[MAX_BIND_TEST_ROW_NUM];
    size_t row_size = sizeof(DataInfo);
    SQLINTEGER out_id;
    SQLINTEGER out_x;
    SQLULEN row_offset = 20;
    SQLULEN bind_offset = row_offset * row_size;

    OK_SIMPLE_STMT(Stmt, "DROP TABLE IF EXISTS test_param_bind_offset");
    OK_SIMPLE_STMT(Stmt, "CREATE TABLE test_param_bind_offset (id integer, x integer)");

    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR, &bind_offset, 0));

    CHECK_STMT_RC(Stmt, SQLBindParameter(Stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, &rows[0].id, 0, NULL));
    CHECK_STMT_RC(Stmt, SQLBindParameter(Stmt, 2, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, &rows[0].x, 0, NULL));

    for (i = 0; i < rowcnt; ++i)
    {
        rows[row_offset + i].id = i * 10;
        rows[row_offset + i].x  = (i * 1000) % 97;
        OK_SIMPLE_STMT(Stmt, "insert into test_param_bind_offset values (?,?)");
        bind_offset += row_size;
    }

    /* verify the data */

    OK_SIMPLE_STMT(Stmt, "select id, x from test_param_bind_offset order by 1");

    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 1, SQL_C_LONG, &out_id, 0, NULL));
    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 2, SQL_C_LONG, &out_x, 0, NULL));

    for (i = 0; i < rowcnt; ++i)
    {
        CHECK_STMT_RC(Stmt, SQLFetch(Stmt));
        is_num(out_id, rows[row_offset + i].id);
        is_num(out_id, i * 10);
        is_num(out_x, rows[row_offset + i].x);
        is_num(out_x, (i * 1000) % 97);
    }

#undef MAX_BIND_TEST_ROW_NUM

    return OK;
}

ODBC_TEST(test_row_bind_offset)
{
#define ATTR_ROW_BIND_OFFSET_ROWS 5
    int i;
    SQLLEN offset;
    struct {
        SQLINTEGER xval;
        SQLINTEGER dummy;
        SQLLEN ylen;
    } results[ATTR_ROW_BIND_OFFSET_ROWS];
    SQLHANDLE     hstmt1;

    CHECK_DBC_RC(Connection, SQLAllocHandle(SQL_HANDLE_STMT, Connection, &hstmt1));
    
    OK_SIMPLE_STMT(hstmt1, "drop table if exists test_row_bind_offset");
    OK_SIMPLE_STMT(hstmt1, "create table test_row_bind_offset (x integer, y integer)");

    OK_SIMPLE_STMT(hstmt1, "insert into test_row_bind_offset values (0,0),(1,NULL),(2,2),(3,NULL),(4,4)");
    OK_SIMPLE_STMT(hstmt1, "select x,y from test_row_bind_offset order by x");

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_BIND_OFFSET_PTR, &offset, SQL_IS_POINTER));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &results[0].xval, 0, NULL));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_LONG, &results[0].dummy, 0, &results[0].ylen));

    /* fetch all the data */
    for(i = 0; i < ATTR_ROW_BIND_OFFSET_ROWS; ++i)
    {
        offset = i * sizeof(results[0]);
        CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));
    }
    FAIL_IF(SQLFetch(hstmt1) != SQL_NO_DATA_FOUND, "eof expected");

    /* verify it */
    for(i = 0; i < ATTR_ROW_BIND_OFFSET_ROWS; ++i)
    {
        printf("xval[%d] = %d\n", i, results[i].xval);
        printf("ylen[%d] = %ld\n", i, results[i].ylen);
        is_num(results[i].xval, i);
        
        if(i % 2)
        {
            is_num(results[i].ylen, SQL_NULL_DATA);
        }
        else
        {
            is_num(results[i].ylen, sizeof(SQLINTEGER));
        }
    }

    OK_SIMPLE_STMT(hstmt1, "drop table if exists test_row_bind_offset");
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_DROP));

#undef ATTR_ROW_BIND_OFFSET_ROWS

    return OK;
}

/*
parameters array support.
Binding by row test
*/
ODBC_TEST(test_param_bind_by_row)
{
#define ROWS_TO_INSERT 3
#define STR_FIELD_LENGTH 255
    typedef struct DataBinding
    {
        SQLINTEGER  intField;
        SQLCHAR     strField[STR_FIELD_LENGTH];
        SQLLEN      indInt;
        SQLLEN      indStr;
    } DATA_BINDING;

    const SQLCHAR *str[]= {"nothing for 1st", "longest string for row 2", "shortest"  };

    SQLCHAR       buff[STR_FIELD_LENGTH];
    DATA_BINDING  dataBinding[ROWS_TO_INSERT];
    SQLUSMALLINT  paramStatusArray[ROWS_TO_INSERT];
    SQLULEN       paramsProcessed, i;
    SQLLEN        rowsCount;
    int           ret;
    SQLHANDLE     hstmt1;
    SQLULEN       bindType;

    CHECK_DBC_RC(Connection, SQLAllocHandle(SQL_HANDLE_STMT, Connection, &hstmt1));
    
    CHECK_STMT_RC(hstmt1, SQLExecDirect(hstmt1, "DROP TABLE IF EXISTS test_param_bind_by_row", SQL_NTS));
    CHECK_STMT_RC(hstmt1, SQLExecDirect(hstmt1, "CREATE TABLE test_param_bind_by_row (intField integer, strField varchar(255))", SQL_NTS));

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAM_BIND_TYPE, (SQLPOINTER)sizeof(DATA_BINDING), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)ROWS_TO_INSERT, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAM_STATUS_PTR, paramStatusArray, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMS_PROCESSED_PTR, &paramsProcessed, 0));

    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_PARAM_BIND_TYPE, &bindType, 0, NULL));
    is_num(bindType, sizeof(DATA_BINDING)); // in row bind mode, attr value is the the actual bind val
    
    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER,
                  0, 0, &dataBinding[0].intField, 0, &dataBinding[0].indInt));
    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR,
                  0, 0, dataBinding[0].strField, 0, &dataBinding[0].indStr));

    dataBinding[0].intField = 0;
    dataBinding[1].intField = 1;
    dataBinding[2].intField = 223322;

    for (i= 0; i < ROWS_TO_INSERT; ++i)
    {
        ret = strcpy_s(dataBinding[i].strField, STR_FIELD_LENGTH, str[i]);
        FAIL_IF(ret != 0, "buffer over flow");
        dataBinding[i].indInt = 0;
        dataBinding[i].indStr = SQL_NTS;
    }

    /* We don't expect errors in paramsets processing, thus we should get SQL_SUCCESS only*/
    FAIL_IF(SQLExecDirect(hstmt1, "INSERT INTO test_param_bind_by_row (intField, strField) VALUES (?,?)", SQL_NTS) != SQL_SUCCESS, "success expected");

    is_num(paramsProcessed, ROWS_TO_INSERT);

    CHECK_STMT_RC(hstmt1, SQLRowCount(hstmt1, &rowsCount));
    is_num(rowsCount, ROWS_TO_INSERT);

    for (i = 0; i < paramsProcessed; ++i) {
        if (paramStatusArray[i] != SQL_PARAM_SUCCESS
            && paramStatusArray[i] != SQL_PARAM_SUCCESS_WITH_INFO)
        {
            diag("Parameter #%u status isn't successful(0x%X)", i + 1, paramStatusArray[i]);
            return FAIL;
        }
    }

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)1, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMS_PROCESSED_PTR, NULL, 0));

    CHECK_STMT_RC(hstmt1, SQLExecDirect(hstmt1, "SELECT intField, strField FROM test_param_bind_by_row ORDER BY intField", SQL_NTS));

    /* Just to make sure RowCount isn't broken */
    CHECK_STMT_RC(hstmt1, SQLRowCount(hstmt1, &rowsCount));
    FAIL_IF(rowsCount != 0 && rowsCount != ROWS_TO_INSERT, "Wrong row count");

    for (i= 0; i < paramsProcessed; ++i)
    {
        CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));

        is_num(my_fetch_int(hstmt1, 1), dataBinding[i].intField);
        IS_STR(my_fetch_str(hstmt1, buff, 2), dataBinding[i].strField, strlen(str[i]));
    }

    FAIL_IF(SQLFetch(hstmt1) != SQL_NO_DATA_FOUND, "eof expected");
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    /* Clean-up */
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));
    CHECK_STMT_RC(hstmt1, SQLExecDirect(hstmt1, "DROP TABLE IF EXISTS test_param_bind_by_row", SQL_NTS));
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_DROP));

#undef ROWS_TO_INSERT
#undef STR_FIELD_LENGTH

    return OK;
}

/*
parameters array support.
Binding by column test
*/
ODBC_TEST(test_param_bind_by_col)
{
#define ROWS_TO_INSERT 3
#define STR_FIELD_LENGTH 5
#define STR_FIELD_BUFF_LENGTH 50
    SQLCHAR       buff[STR_FIELD_BUFF_LENGTH];

    const SQLCHAR strField[ROWS_TO_INSERT][STR_FIELD_LENGTH] = {{'\0'}, {'x','\0'}, {'x','x','x','\0'} };
    SQLLEN        strInd[ROWS_TO_INSERT] = {SQL_NTS, SQL_NTS, SQL_NTS};

    SQLINTEGER    intField[ROWS_TO_INSERT] = {0, 1, 123321};
    SQLLEN        intInd[ROWS_TO_INSERT] = {5, 4, 3};

    SQLUSMALLINT  paramStatusArray[ROWS_TO_INSERT];
    SQLULEN       paramsProcessed;
    SQLULEN       i;
    SQLHANDLE     hstmt1;
    SQLULEN       bindType;

    CHECK_DBC_RC(Connection, SQLAllocHandle(SQL_HANDLE_STMT, Connection, &hstmt1));
    
    CHECK_STMT_RC(hstmt1, SQLExecDirect(hstmt1, "DROP TABLE IF EXISTS test_param_bind_by_col", SQL_NTS));
    CHECK_STMT_RC(hstmt1, SQLExecDirect(hstmt1, "CREATE TABLE test_param_bind_by_col (intField integer, strField varchar(255))", SQL_NTS));

    CHECK_STMT_RC(hstmt1, SQLGetStmtAttr(hstmt1, SQL_ATTR_PARAM_BIND_TYPE, &bindType, 0, NULL));
    is_num(bindType, SQL_PARAM_BIND_BY_COLUMN); // default value should be SQL_PARAM_BIND_BY_COLUMN
    
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)ROWS_TO_INSERT, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAM_STATUS_PTR, paramStatusArray, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMS_PROCESSED_PTR, &paramsProcessed, 0));

    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER,
                  0, 0, intField, 0, NULL));
    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR,
                  0, 0, (SQLPOINTER)strField, STR_FIELD_LENGTH, strInd));
    is_num(iOdbcSetParamBufferSize(hstmt1, 2, STR_FIELD_LENGTH), OK);

    /* We don't expect errors in paramsets processing, thus we should get SQL_SUCCESS only*/
    FAIL_IF(SQLExecDirect(hstmt1, "INSERT INTO test_param_bind_by_col (intField, strField) VALUES (?,?)", SQL_NTS) != SQL_SUCCESS, "success expected");

    is_num(paramsProcessed, ROWS_TO_INSERT);

    for (i = 0; i < paramsProcessed; ++i) {
        if (paramStatusArray[i] != SQL_PARAM_SUCCESS
            && paramStatusArray[i] != SQL_PARAM_SUCCESS_WITH_INFO)
        {
            diag("Parameter #%u status isn't successful(0x%X)", i + 1, paramStatusArray[i]);
            return FAIL;
        }
    }

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)1, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMS_PROCESSED_PTR, NULL, 0));

    CHECK_STMT_RC(hstmt1, SQLExecDirect(hstmt1, "SELECT intField, strField FROM test_param_bind_by_col ORDER BY intField", SQL_NTS));

    for (i= 0; i < paramsProcessed; ++i)
    {
        CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));

        is_num(my_fetch_int(hstmt1, 1), intField[i]);
        IS_STR(my_fetch_str(hstmt1, buff, 2), strField[i], strlen(strField[i]));
    }

    /* Clean-up */
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));
    CHECK_STMT_RC(hstmt1, SQLExecDirect(hstmt1, "DROP TABLE IF EXISTS test_param_bind_by_col", SQL_NTS));
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_DROP));

#undef ROWS_TO_INSERT
#undef STR_FIELD_LENGTH
#undef STR_FIELD_BUFF_LENGTH

    return OK;
}

/*
parameters array support.
Ignore paramset test
*/
ODBC_TEST(test_param_with_ignore_set)
{
#define ROWS_TO_INSERT 4
#define STR_FIELD_LENGTH 5
#define STR_FIELD_BUFF_LENGTH 50
    SQLCHAR       buff[STR_FIELD_BUFF_LENGTH];

    const SQLCHAR strField[ROWS_TO_INSERT][STR_FIELD_LENGTH]= {{'\0'}, {'x','\0'}, {'x','x','x','\0'} };
    SQLLEN        strInd[ROWS_TO_INSERT]= {SQL_NTS, SQL_NTS, SQL_NTS};

    SQLINTEGER    intField[ROWS_TO_INSERT] = {0, 1, 123321};
    SQLLEN        intInd[ROWS_TO_INSERT]= {5,4,3};

    SQLUSMALLINT  paramOperationArr[ROWS_TO_INSERT]={0, SQL_PARAM_IGNORE, 0, SQL_PARAM_IGNORE};
    SQLUSMALLINT  paramStatusArr[ROWS_TO_INSERT];
    SQLULEN       paramsProcessed, i, rowsInserted= 0;

    CHECK_STMT_RC(Stmt, SQLExecDirect(Stmt, "DROP TABLE IF EXISTS test_param_with_ignore_set", SQL_NTS));
    CHECK_STMT_RC(Stmt, SQLExecDirect(Stmt, "CREATE TABLE test_param_with_ignore_set ("\
                  "intField integer, strField varchar(255))", SQL_NTS));

    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)ROWS_TO_INSERT, 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAM_STATUS_PTR, paramStatusArr, 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAM_OPERATION_PTR, paramOperationArr, 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &paramsProcessed, 0));

    CHECK_STMT_RC(Stmt, SQLBindParameter(Stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER,
                  0, 0, intField, 0, intInd));
    CHECK_STMT_RC(Stmt, SQLBindParameter(Stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR,
                  0, 0, (SQLPOINTER)strField, STR_FIELD_LENGTH, strInd ));
    is_num(iOdbcSetParamBufferSize(Stmt, 2, STR_FIELD_LENGTH), OK);

    /* We don't expect errors in paramsets processing, thus we should get SQL_SUCCESS only*/
    FAIL_IF(SQLExecDirect(Stmt, "INSERT INTO test_param_with_ignore_set (intField, strField) " \
                          "VALUES (?,?)", SQL_NTS) != SQL_SUCCESS, "success expected");

    is_num(paramsProcessed, ROWS_TO_INSERT);

    for (i = 0; i < paramsProcessed; ++i)
    {
        if (paramOperationArr[i] == SQL_PARAM_IGNORE)
        {
            is_num(paramStatusArr[i], SQL_PARAM_UNUSED);
        }
        else if (paramStatusArr[i] != SQL_PARAM_SUCCESS
                  && paramStatusArr[i] != SQL_PARAM_SUCCESS_WITH_INFO)
        {
            diag("Parameter #%u status isn't successful(0x%X)", i + 1, paramStatusArr[i]);
            return FAIL;
        }
    }

    /* Resetting statements attributes */
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)1, 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, NULL, 0));

    CHECK_STMT_RC(Stmt, SQLExecDirect(Stmt, "SELECT intField, strField FROM test_param_with_ignore_set\
                                      ORDER BY intField", SQL_NTS));

    i = 0;
    while(i < paramsProcessed)
    {
        if (paramStatusArr[i] == SQL_PARAM_UNUSED)
        {
            ++i;
            continue;
        }

        CHECK_STMT_RC(Stmt, SQLFetch(Stmt));
        is_num(my_fetch_int(Stmt, 1), intField[i]);
        IS_STR(my_fetch_str(Stmt, buff, 2), strField[i], strlen(strField[i]));

        ++rowsInserted;
        ++i;
    }

    /* Making sure that there is nothing else to fetch ... */
    FAIL_IF(SQLFetch(Stmt) != SQL_NO_DATA_FOUND, "eof expected");

    /* ... and that inserted was less than SQL_ATTR_PARAMSET_SIZE rows */
    IS(rowsInserted < ROWS_TO_INSERT);

    /* Clean-up */
    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    CHECK_STMT_RC(Stmt, SQLExecDirect(Stmt, "DROP TABLE IF EXISTS test_param_with_ignore_set", SQL_NTS));

#undef ROWS_TO_INSERT
#undef STR_FIELD_LENGTH
#undef STR_FIELD_BUFF_LENGTH

    return OK;
}

ODBC_TEST(test_param_status_array)
{
#define ATTR_STATUS_RECORD_NAME_LEN    20
#define ATTR_STATUS_RECORD_ARRAY_SIZE  5
    SQLUSMALLINT rowStatusPtr[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLULEN      rowsFetchedPtr = 0;
    SQLRETURN    rc;

    typedef struct {
        SQLINTEGER id;
        SQLCHAR name[ATTR_STATUS_RECORD_NAME_LEN + 1];
        SQLLEN idLen;
        SQLLEN nameLen;
    }Record;
    SQLULEN      rowcnt = ATTR_STATUS_RECORD_ARRAY_SIZE;
    SQLINTEGER   row_size = sizeof(Record);
    Record       record0[ATTR_STATUS_RECORD_ARRAY_SIZE];
    Record       record[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLUSMALLINT paramStatusArray[ATTR_STATUS_RECORD_ARRAY_SIZE];
    SQLULEN      paramsProcessed;

    OK_SIMPLE_STMT(Stmt, "DROP TABLE IF EXISTS test_param_status_array");
    OK_SIMPLE_STMT(Stmt, "CREATE TABLE test_param_status_array (id INTEGER, name CHAR(20))");

    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAM_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAM_STATUS_PTR, paramStatusArray, 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &paramsProcessed, 0));
    for (int i = 0; i < ATTR_STATUS_RECORD_ARRAY_SIZE; i++)
    {
        record0[i].id      = (i + 1) * 10;
        record0[i].idLen   = sizeof(record0[i].id);
        rc = _snprintf_s(record0[i].name, sizeof(record0[i].name), sizeof(record0[i].name) - 1, "data%d", i);
        record0[i].nameLen = strlen(record0[i].name);
    }
    CHECK_STMT_RC(Stmt, SQLBindParameter(Stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER,
                  0, 0, &record0[0].id, 0, &record0[0].idLen));
    CHECK_STMT_RC(Stmt, SQLBindParameter(Stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR,
                  0, 0, record0[0].name, 0, &record0[0].nameLen));

    FAIL_IF(SQLExecDirect(Stmt, "INSERT INTO test_param_status_array (id, name) VALUES (?,?)", SQL_NTS) != SQL_SUCCESS, "success expected");
    is_num(paramsProcessed, rowcnt);
    for (int i = 0; i < rowcnt; ++i) {
        if (paramStatusArray[i] != SQL_PARAM_SUCCESS
            && paramStatusArray[i] != SQL_PARAM_SUCCESS_WITH_INFO)
        {
            diag("Parameter #%u status isn't successful(0x%X)", i, paramStatusArray[i]);
            return FAIL;
        }
    }

    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_CURSOR_SCROLLABLE, (SQLPOINTER)SQL_NONSCROLLABLE, 0));

    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetchedPtr, 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_ROW_STATUS_PTR, rowStatusPtr, 0));
    
    OK_SIMPLE_STMT(Stmt, "SELECT * FROM test_param_status_array ORDER BY id");

    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 1, SQL_C_LONG, &record[0].id, sizeof(record[0].id), &record[0].idLen));
    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 2, SQL_C_CHAR, record[0].name, sizeof(record[0].name), &record[0].nameLen));
    
    CHECK_STMT_RC(Stmt, SQLFetch(Stmt));

    is_num(rowcnt, rowsFetchedPtr); // check if SQL_ATTR_ROW_STATUS_PTR makes sense
    // check if SQL_ATTR_ROW_BIND_TYPE makes sense, by checking whether results filled in right pos 
    for (int i = 0; i < rowcnt; i++)
    {
        is_num(record[i].id, record0[i].id);
        IS_STR(record[i].name, record0[i].name, strlen(record0[i].name));
        is_num(rowStatusPtr[i], SQL_PARAM_PROCEED);
    }

    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)1, 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)SQL_PARAM_BIND_BY_COLUMN, 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_CURSOR_SCROLLABLE, (SQLPOINTER)SQL_SCROLLABLE, 0));

    OK_SIMPLE_STMT(Stmt, "SELECT * FROM test_param_status_array");

    CHECK_STMT_RC(Stmt, SQLFetchScroll(Stmt, SQL_FETCH_ABSOLUTE, 2));

    is_num(rowsFetchedPtr, 1);
    is_num(rowStatusPtr[0], SQL_PARAM_PROCEED);

    CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_ROWS_FETCHED_PTR, NULL, 0));
    CHECK_STMT_RC(Stmt, SQLSetStmtAttr(Stmt, SQL_ATTR_ROW_STATUS_PTR, NULL, 0));

    OK_SIMPLE_STMT(Stmt, "DROP TABLE IF EXISTS test_param_status_array");

#undef ATTR_STATUS_RECORD_NAME_LEN
#undef ATTR_STATUS_RECORD_ARRAY_SIZE

    return OK;
}

/* test COM_QUIT with SQLDisconnect */
ODBC_TEST(test_com_close)
{
    SQLHANDLE   henv0;
    SQLHANDLE   hdbc0;
    SQLHANDLE   hstmt0;
    SQLINTEGER  state;

    ODBC_Connect(&henv0, &hdbc0, &hstmt0);

    CHECK_DBC_RC(hdbc0, SQLGetConnectAttr(hdbc0, SQL_ATTR_CONNECTION_DEAD, (SQLPOINTER)&state, 0, NULL));
    is_num(state, SQL_CD_FALSE); // connect is active
    
    FAIL_IF(SQLDisconnect(hdbc0) != SQL_SUCCESS, "close db connection error.");

    FAIL_IF(SQLGetConnectAttr(hdbc0, SQL_ATTR_CONNECTION_DEAD, (SQLPOINTER)&state, 0, NULL) == SQL_SUCCESS, "connection is closed, can not get attribute.");
    
    FAIL_IF(SQLFreeHandle(SQL_HANDLE_DBC, hdbc0) != SQL_SUCCESS, "free db connection error.");
    
    FAIL_IF(SQLFreeHandle(SQL_HANDLE_ENV, henv0) != SQL_SUCCESS, "free env error.");

    return OK;
}

/* test basic statement operation: alloc/reset/unbind/close/drop */
ODBC_TEST(test_statement_operate)
{
#define STATEMENT_OPERATE_RECORD_NAME_LEN    20
#define STATEMENT_OPERATE_RECORD_ARRAY_SIZE  5
    SQLHANDLE   hstmt1;
    SQLULEN     rowsFetchedPtr = 0;
    SQLRETURN   rc;

    typedef struct {
        SQLINTEGER id;
        SQLCHAR name[STATEMENT_OPERATE_RECORD_NAME_LEN + 1];
        SQLLEN idLen;
        SQLLEN nameLen;
    }Record;
    SQLULEN      rowcnt = STATEMENT_OPERATE_RECORD_ARRAY_SIZE;
    SQLINTEGER   row_size = sizeof(Record);
    Record       record0[STATEMENT_OPERATE_RECORD_ARRAY_SIZE];
    Record       record[STATEMENT_OPERATE_RECORD_ARRAY_SIZE];
    SQLULEN      paramsProcessed;

    FAIL_IF(SQLAllocHandle(SQL_HANDLE_STMT, Connection, &hstmt1), "Alloc new statement error.");
    
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_statement_operate");
    OK_SIMPLE_STMT(hstmt1, "CREATE TABLE test_statement_operate (id INTEGER, name CHAR(20))");

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAM_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMS_PROCESSED_PTR, &paramsProcessed, 0));
    for (int i = 0; i < STATEMENT_OPERATE_RECORD_ARRAY_SIZE; i++)
    {
        record0[i].id      = (i + 1) * 10;
        record0[i].idLen   = sizeof(record0[i].id);
        rc = _snprintf_s(record0[i].name, sizeof(record0[i].name), sizeof(record0[i].name) - 1, "value%d", i);
        record0[i].nameLen = strlen(record0[i].name);
    }
    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER,
                  0, 0, &record0[0].id, sizeof(record0[0].id), &record0[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR,
                  STATEMENT_OPERATE_RECORD_NAME_LEN, 0, record0[0].name, sizeof(record0[0].name), &record0[0].nameLen));

    /* reset params, execute will fail */
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_RESET_PARAMS));
    FAIL_IF(SQLExecDirect(hstmt1, "INSERT INTO test_statement_operate (id, name) VALUES (?,?)", SQL_NTS) == SQL_SUCCESS, 
            "expect fail, due to params has been reset by SQL_RESET_PARAMS");

    /* rebind params */
    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER,
                  0, 0, &record0[0].id, sizeof(record0[0].id), &record0[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR,
                  STATEMENT_OPERATE_RECORD_NAME_LEN, 0, record0[0].name, sizeof(record0[0].name), &record0[0].nameLen));

    /* expect execute successfully */
    CHECK_STMT_RC(hstmt1, SQLExecDirect(hstmt1, "INSERT INTO test_statement_operate (id, name) VALUES (?,?)", SQL_NTS));
    is_num(paramsProcessed, rowcnt);

    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_RESET_PARAMS));

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)rowcnt, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(Record), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetchedPtr, 0));
    
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &record[0].id, sizeof(record[0].id), &record[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_CHAR, record[0].name, sizeof(record[0].name), &record[0].nameLen));

    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_statement_operate ORDER BY id");
    CHECK_STMT_RC(hstmt1, SQLFetchScroll(hstmt1, SQL_FETCH_FIRST, 0L));

    /* test statement resultset unbind */
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_UNBIND));
    FAIL_IF(SQLFetchScroll(hstmt1, SQL_FETCH_NEXT, 0L) == SQL_SUCCESS, "result set has been reset, expect fetch none");

    /* bind resultset again */
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &record[0].id, sizeof(record[0].id), &record[0].idLen));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_CHAR, record[0].name, sizeof(record[0].name), &record[0].nameLen));
    
    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_statement_operate ORDER BY id");
    
    CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));

    /* check the result */
    is_num(rowcnt, rowsFetchedPtr);
    for (int i = 0; i < rowcnt; i++)
    {
        is_num(record[i].id, record0[i].id);
        IS_STR(record[i].name, record0[i].name, strlen(record0[i].name));
    }

    /* normal close statment, test COM_STMT_RESET work normally */
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_statement_operate");

    /* normal drop statment, test COM_STMT_CLOSE work normally */
    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_DROP));

#undef STATEMENT_OPERATE_RECORD_NAME_LEN
#undef STATEMENT_OPERATE_RECORD_ARRAY_SIZE

    return OK;
}

/* test basic function of COM_STMT_SEND_LONG_DATA */
ODBC_TEST(test_send_long_data)
{
#define TEST_SEND_LONG_DATA_BUFFER_LEN    50
    SQLHANDLE   henv1;
    SQLHANDLE   hdbc1;
    SQLHANDLE   hstmt1;

    SQLLEN     valueLen;
    SQLINTEGER id = 0;
    int        rc;
    SQLCHAR    buffer[TEST_SEND_LONG_DATA_BUFFER_LEN];
    SQLCHAR    value[TEST_SEND_LONG_DATA_BUFFER_LEN];
    SQLPOINTER parameter;
    SQLPOINTER invalidPtr = (SQLPOINTER)5;

    ODBC_Connect(&henv1, &hdbc1, &hstmt1);
    
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_send_long_data");
    OK_SIMPLE_STMT(hstmt1, "CREATE TABLE test_send_long_data (id INTEGER, value VARCHAR(300))");

    CHECK_STMT_RC(hstmt1, SQLPrepare(hstmt1, (SQLCHAR *)"INSERT INTO test_send_long_data VALUES(?, ?)", SQL_NTS));

    CHECK_STMT_RC(hstmt1,  SQLBindParameter(hstmt1, 1, SQL_PARAM_INPUT, SQL_C_LONG,
                                            SQL_INTEGER, 0, 0, (SQLPOINTER)&id, 0, NULL));

    /* bind an invalid pointer for ParameterValuePtr of SQLBindParameter */
    CHECK_STMT_RC(hstmt1,  SQLBindParameter(hstmt1, 2, SQL_PARAM_INPUT, SQL_C_CHAR,
                                            SQL_VARCHAR, 0, 0, invalidPtr, 0, &valueLen));

    /* mark param as special length/indicator values */
    valueLen = SQL_DATA_AT_EXEC;

    id = 0;
    EXPECT_STMT(hstmt1, SQLExecute(hstmt1), SQL_NEED_DATA);
    EXPECT_STMT(hstmt1, SQLParamData(hstmt1, &parameter), SQL_NEED_DATA);
    is_num(parameter, invalidPtr);

    rc = _snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, "hello world%d", id);
    CHECK_STMT_RC(hstmt1, SQLPutData(hstmt1, buffer, strlen(buffer))); //send long data to server
    CHECK_STMT_RC(hstmt1, SQLParamData(hstmt1, &parameter)); //try execute, all params ready, expect SQL_SUCCESS

    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));

    OK_SIMPLE_STMT(hstmt1, "SELECT * FROM test_send_long_data");

    CHECK_STMT_RC(hstmt1, SQLFetch(hstmt1));
    CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, 2, SQL_CHAR, value, sizeof(value), &valueLen));
    is_num(valueLen, strlen(buffer));
    IS_STR(value, buffer, strlen(buffer));

    CHECK_STMT_RC(hstmt1, SQLFreeStmt(hstmt1, SQL_CLOSE));
    OK_SIMPLE_STMT(hstmt1, "DROP TABLE IF EXISTS test_send_long_data");

    ODBC_Disconnect(henv1, hdbc1, hstmt1);

#undef TEST_SEND_LONG_DATA_BUFFER_LEN
    return OK;
}

/*
    test cases for attributes of environment,connection and statements
     environment: odbc protocal version
     connection: attribute access mode, and cursor type of connection
     statements: descriptors, statement cursor op, metadata attribute id
                 param bind type/offset/operation pointer/status pointer/processed pointer/size
                 row array size/row bind offset/row status pointer/fetch pointer info
*/
MA_ODBC_TESTS my_tests[]=
{
    {test_attr_basic,               "test_attr_basic"},
    {test_desc_attr_apd,            "test_desc_attr_apd"},
    {test_desc_attr_ard,            "test_desc_attr_ard"},
    {test_desc_attr_basic,          "test_desc_attr_basic_operate"},
    {test_stmt_desc_attr,           "test_attribute_apd_ipd"},
    {test_stmt_desc_attr1,          "test_attribute_ard_ird"},
    {test_desc_status_array,        "test_descriptor_status_array"},
    {test_cursor_type,              "test_cursor_type"},
    {test_cursor_attribute,         "test_cursor_attribute"},
    {test_scroll_cursor,            "test_scroll_cursor"},
    {test_cursor_set_pos,           "test_cursor_set_pos"},
    {test_bookmark_cursor,          "test_bookmark_cursor"},
    {test_bookmark_cursor1,         "test_bookmark_cursor_mix"},
    {test_param_bind_by_row,        "test_param_bind_by_row"},
    {test_param_bind_by_col,        "test_param_bind_by_column"},
    {test_param_with_ignore_set,    "test_param_with_ignore_set"},
    {test_param_status_array,       "test_param_status_array"},
    {test_bind_offset,              "test_param_bind_offset"},
    {test_row_bind_offset,          "test_row_bind_offset_ptr"},
    {test_com_close,                "test_com_close"},
    {test_statement_operate,        "test_statement_operate"},
    {test_send_long_data,           "test_send_long_data"},
    {NULL, NULL}
};

int main(int argc, char **argv)
{
    int ret;
    int tests = sizeof(my_tests) / sizeof(MA_ODBC_TESTS) - 1;
    
    get_options(argc, argv);
    plan(tests);
    mark_all_tests_normal(my_tests);
    ret = run_tests(my_tests);

    return ret;
}

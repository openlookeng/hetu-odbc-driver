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

void preparedata()
{

    OK_SIMPLE_STMT(Stmt, "drop table if exists test_tbl_blockcursor");

    OK_SIMPLE_STMT(Stmt, "create table if not exists test_tbl_blockcursor (id integer, val varchar)");
}

void cleanup(SQLHANDLE hstmt, char* tblname)
{
    char stmtbuffer[255] = {0};

    sprintf_s(stmtbuffer, sizeof(stmtbuffer) - 1, "drop table if exists %s", tblname);

    OK_SIMPLE_STMT(hstmt, stmtbuffer);
}

#define PARAM_ARRAY_SIZE 10

#define ROW_ARRAY_SIZE 5

SQLINTEGER ArrIds[PARAM_ARRAY_SIZE] = { 1,2,3,4,5,6,7,8,9,10 };
SQLCHAR ArrVals[PARAM_ARRAY_SIZE][2] = { "a","b","c","d","e","f","g","h","i","j" };

ODBC_TEST(test_colwise)
{
    SQLHANDLE henv1;
    SQLHANDLE hdbc1;
    SQLHANDLE hstmt1;

    preparedata();

    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    /* Insert Using Parameter Array -- Col-Wise Binding */

    SQLUINTEGER infoval = 0;
    SQLLEN procparams = 0;
    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_PARAM_ARRAY_ROW_COUNTS, (SQLPOINTER)&infoval, 0, NULL));
    is_num(infoval, SQL_PARC_NO_BATCH);

    SQLUSMALLINT ArrParamStatus[PARAM_ARRAY_SIZE] = { SQL_PARAM_ERROR };
  
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAM_BIND_TYPE,
                                        SQL_PARAM_BIND_BY_COLUMN, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMSET_SIZE,
                                        (SQLPOINTER) PARAM_ARRAY_SIZE, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAM_STATUS_PTR,
                                        ArrParamStatus, PARAM_ARRAY_SIZE));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMS_PROCESSED_PTR,
                                        (SQLPOINTER)&procparams, 0));

    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 1, SQL_PARAM_INPUT, SQL_C_LONG,
                                        SQL_INTEGER, 0, 0, ArrIds, 0, NULL));
    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 2, SQL_PARAM_INPUT, SQL_C_CHAR,
                                        SQL_VARCHAR, sizeof(ArrVals[0]), 0, ArrVals, sizeof(ArrVals[0]), NULL));

    OK_SIMPLE_STMT(hstmt1, "insert into test_tbl_blockcursor (id, val) values (?,?)");
    is_num(procparams, PARAM_ARRAY_SIZE);

    for (int i = 0; i < procparams; i++) {
        is_num(ArrParamStatus[i], SQL_PARAM_SUCCESS);
    }

    /* Fetching result with col-wise binding */
    SQLLEN rowsfetched = 0;
    SQLINTEGER RowIds[ROW_ARRAY_SIZE] = { 0 };
    SQLCHAR RowVals[ROW_ARRAY_SIZE][2] = { " " };

    SQLLEN ArrIndId[ROW_ARRAY_SIZE];
    SQLLEN ArrIndVal[ROW_ARRAY_SIZE];
    SQLUSMALLINT ArrRowStatus[ROW_ARRAY_SIZE] = { SQL_ROW_ERROR };

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_BIND_TYPE,
        SQL_BIND_BY_COLUMN, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_ARRAY_SIZE,
        (SQLPOINTER)ROW_ARRAY_SIZE, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_STATUS_PTR,
        ArrRowStatus, ROW_ARRAY_SIZE));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROWS_FETCHED_PTR,
        &rowsfetched, 0));

    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, RowIds, 0, ArrIndId));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_CHAR, RowVals, sizeof(ArrVals[0]), ArrIndVal));

    OK_SIMPLE_STMT(hstmt1, "select id, val from test_tbl_blockcursor order by id");

    int rowcount = 0;

    //while (SQLFetchScroll(hstmt1, SQL_FETCH_NEXT, 0) != SQL_NO_DATA) {
    while (SQL_SUCCEEDED(SQLFetch(hstmt1))) { //SQLFetchScroll and SQLFetch are both working here
        for (int i = 0; i < rowsfetched; i++) {
            if (ArrRowStatus[i] == SQL_ROW_SUCCESS || ArrRowStatus[i] == SQL_ROW_SUCCESS_WITH_INFO) {
                if (ArrIndId[i] == SQL_NULL_DATA) {
                    printf(" NULL   ");
                }
                else {
                    printf(" %d\t", RowIds[i]);
                    is_num(RowIds[i], rowcount + 1);
                }

                if (ArrIndVal[i] == SQL_NULL_DATA) {
                    printf(" NULL   ");
                }
                else {
                    printf(" %s\t\n", RowVals[i]);
                    IS_STR(RowVals[i], ArrVals[rowcount++], sizeof(RowVals[i]));
                }
            }
        }
    }
    is_num(rowcount, PARAM_ARRAY_SIZE);
    //}

    cleanup(hstmt1, "test_tbl_blockcursor");

    ODBC_Disconnect(henv1, hdbc1, hstmt1);

    return OK;
}


typedef struct {
    SQLINTEGER Id;
    SQLLEN IndId;
    SQLCHAR Val[2];
    SQLLEN IndVal;
} RECORD;

ODBC_TEST(test_rowwise)
{
    SQLHANDLE henv1;
    SQLHANDLE hdbc1;
    SQLHANDLE hstmt1;

    preparedata();

    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    /* Insert Using Parameter Array -- Row-Wise Binding */
    SQLUINTEGER infoval = 0;
    SQLLEN procparams = 0;
    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_PARAM_ARRAY_ROW_COUNTS, (SQLPOINTER)&infoval, 0, NULL));
    is_num(infoval, SQL_PARC_NO_BATCH);

    RECORD inputs[PARAM_ARRAY_SIZE] = { { 0,0,"\0",0 } };
    SQLUSMALLINT ArrParamStatus[PARAM_ARRAY_SIZE] = { SQL_PARAM_ERROR };

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAM_BIND_TYPE,
                 (SQLPOINTER)sizeof(RECORD), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMSET_SIZE,
                 (SQLPOINTER)PARAM_ARRAY_SIZE, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAM_STATUS_PTR,
                 ArrParamStatus, PARAM_ARRAY_SIZE));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_PARAMS_PROCESSED_PTR,
                 (SQLPOINTER)&procparams, 0));

    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 1, SQL_PARAM_INPUT, SQL_C_LONG,
        SQL_INTEGER, 0, 0, &inputs[0].Id, 0, &inputs[0].IndId));
    CHECK_STMT_RC(hstmt1, SQLBindParameter(hstmt1, 2, SQL_PARAM_INPUT, SQL_C_CHAR,
        SQL_VARCHAR, sizeof(inputs[0].Val), 0, &inputs[0].Val, sizeof(inputs[0].Val), NULL));
    
    for (int i = 0; i < PARAM_ARRAY_SIZE; i++) {
        inputs[i].Id = ArrIds[i];
        for (int j = 0; j < sizeof(inputs[i].Val); j++) {
            inputs[i].Val[j] = ArrVals[i][j];
        }
    }

    OK_SIMPLE_STMT(hstmt1, "insert into test_tbl_blockcursor (id, val) values (?,?)");

    for (int i = 0; i < procparams; i++) {
        is_num(ArrParamStatus[i], SQL_PARAM_SUCCESS);
    }

    is_num(procparams, PARAM_ARRAY_SIZE);

    /* Fetching result with col-wise binding */
    SQLLEN rowsfetched = 0;

    RECORD records[ROW_ARRAY_SIZE] = { { 0,0,"\0",0 } };
    SQLUSMALLINT ArrRowStatus[ROW_ARRAY_SIZE] = { SQL_ROW_ERROR };

    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_BIND_TYPE,
        (SQLPOINTER)sizeof(RECORD), 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_ARRAY_SIZE,
        (SQLPOINTER)ROW_ARRAY_SIZE, 0));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROW_STATUS_PTR,
        ArrRowStatus, ROW_ARRAY_SIZE));
    CHECK_STMT_RC(hstmt1, SQLSetStmtAttr(hstmt1, SQL_ATTR_ROWS_FETCHED_PTR,
        &rowsfetched, 0));

    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 1, SQL_C_LONG, &records[0].Id, 0, &records[0].IndId));
    CHECK_STMT_RC(hstmt1, SQLBindCol(hstmt1, 2, SQL_C_CHAR, &records[0].Val,
        sizeof(records[0].Val), &records[0].IndVal));

    OK_SIMPLE_STMT(hstmt1, "select id, val from test_tbl_blockcursor order by id");

    int rowcount = 0;

    //while (SQLFetchScroll(hstmt1, SQL_FETCH_NEXT, 0) != SQL_NO_DATA) {
    while (SQL_SUCCEEDED(SQLFetch(hstmt1))) {
        for (int i = 0; i < rowsfetched; i++) {
            if (ArrRowStatus[i] == SQL_ROW_SUCCESS || ArrRowStatus[i] == SQL_ROW_SUCCESS_WITH_INFO) {
                if (records[i].IndId == SQL_NULL_DATA) {
                    printf(" NULL   ");
                }
                else {
                    printf(" %d\t", records[i].Id);
                    is_num(records[i].Id, rowcount + 1);
                }

                if (records[i].IndVal == SQL_NULL_DATA) {
                    printf(" NULL   ");
                }
                else {
                    printf(" %s\t\n", records[i].Val);
                    IS_STR(records[i].Val, ArrVals[rowcount++], sizeof(records[i].Val));
                }
            }
        }
    }
    is_num(rowcount, PARAM_ARRAY_SIZE);
    //}

    cleanup(hstmt1, "test_tbl_blockcursor");

    ODBC_Disconnect(henv1, hdbc1, hstmt1);

    return OK;
}

MA_ODBC_TESTS my_tests[]=
{
    { test_colwise ,"test_colwise" },
    { test_rowwise , "test_rowwise" },
    { NULL, NULL }
};

int main(int argc, char **argv)
{
  int tests= sizeof(my_tests)/sizeof(MA_ODBC_TESTS) - 1;
  get_options(argc, argv);
  plan(tests);
  mark_all_tests_normal(my_tests);
  return run_tests(my_tests);
}

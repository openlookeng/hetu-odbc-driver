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



ODBC_TEST(tests_setup)
{
    //setup  data types test dataset
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_tbl_alldatatypes");
    
    OK_SIMPLE_STMT(Stmt, "create table test_tbl_alldatatypes ("
                           "id  integer,"
                           "dt_boolean  boolean,"
                           "dt_tinyint tinyint,"
                           "dt_smallint smallint,"
                           "dt_integer integer,"
                           "dt_bigint bigint,"
                           "dt_real real,"
                           "dt_double double,"
                           "dt_decimal decimal,"
                           "dt_varchar varchar,"
                           "dt_char char,"
                           "dt_varbinary varbinary,"
                           "dt_date date,"
                           "dt_time time,"
                           "dt_timestamp timestamp,"
                           "dt_interval_year_to_month  INTERVAL YEAR TO MONTH,"
                           "dt_interval_day_to_second  INTERVAL DAY TO SECOND"
                         ") COMMENT 'all datatype support'");
    return OK;
}



ODBC_TEST(tests_cleanup)
{
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_tbl_alldatatypes");
    return OK;
}

//=========================================================
//test for ODBC catalog API relate data types check
//1)SQLGetTypeInfo  DATA_TYPE
//2)SQLColumns   DATA_TYPE
//3)SQLSpecialColumns  NA(no this feature)
//4)SQLProcedureColumns  NA(no this feature)
//=========================================================
ODBC_TEST(test_SQLGetTypeInfo_API)
{
   SQLSMALLINT dataTypeCols;
   SQLLEN  dataTypeRows;
   SQLCHAR params[64];

   //check  all 16 kinds  datatype
   CHECK_STMT_RC(Stmt, SQLGetTypeInfo(Stmt, SQL_ALL_TYPES));
   CHECK_STMT_RC(Stmt, SQLNumResultCols(Stmt, &dataTypeCols));
   CHECK_STMT_RC(Stmt, SQLRowCount(Stmt, &dataTypeRows));

   is_num(dataTypeCols, 19);
   is_num(dataTypeRows, 16);
   CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
   
   //check  boolean data type
   CHECK_STMT_RC(Stmt, SQLGetTypeInfo(Stmt, SQL_BIT));
   CHECK_STMT_RC(Stmt, SQLFetch(Stmt));
   IS_STR(my_fetch_str(Stmt, params, 1), "boolean", sizeof("boolean"));
   CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    return OK;
}


ODBC_TEST(test_SQLColumns_API)
{
    SQLCHAR   TypeName[64];
    SQLSMALLINT    sqldatatype=0;
    SQLLEN    columnsize=0;

    CHECK_STMT_RC(Stmt, SQLColumns(Stmt, NULL, SQL_NTS, NULL, SQL_NTS,
        (SQLCHAR *)"test_tbl_alldatatypes", SQL_NTS,
        NULL, SQL_NTS));

    while (SQLFetch(Stmt) == SQL_SUCCESS)
    {
        CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 6, SQL_C_CHAR, TypeName,
            sizeof(TypeName), NULL));
        if (sizeof(TypeName) != 0 && strstr(TypeName, "char(1)")) { // DB server will automatic complete char to char(1)
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 5, SQL_C_SSHORT, &sqldatatype,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 7, SQL_C_ULONG, &columnsize,
                0, NULL));
            is_num(sqldatatype, SQL_CHAR);
            is_num(columnsize, 1);
        }
        if (sizeof(TypeName) != 0 && strstr(TypeName, "varchar")) {
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 5, SQL_C_SSHORT, &sqldatatype,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 7, SQL_C_ULONG, &columnsize,
                0, NULL));
            is_num(sqldatatype, SQL_VARCHAR);
            is_num(columnsize, 2048);

        }
        if (sizeof(TypeName) != 0 && strstr(TypeName, "interval day to second")) {
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 5, SQL_C_SSHORT, &sqldatatype,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 7, SQL_C_ULONG, &columnsize,
                0, NULL));
            is_num(sqldatatype, SQL_VARCHAR);
            is_num(columnsize, 2048);
        }
        if (sizeof(TypeName) != 0 && strstr(TypeName, "interval year to month")) {
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 5, SQL_C_SSHORT, &sqldatatype,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 7, SQL_C_ULONG, &columnsize,
                0, NULL));
            is_num(sqldatatype, SQL_VARCHAR);
            is_num(columnsize, 2048);
        }
    }

    return OK;
}



//=========================================================

//test for ODBC statement API relate date types check
//1)SQLDescribeParam

//=========================================================

ODBC_TEST(test_SQLDescribeParam_API)
{
    // current SQLDescribeParam is not implement,just check function enable?
    SQLUSMALLINT funcs[1];
    CHECK_DBC_RC(Connection, SQLGetFunctions(Connection, SQL_API_SQLDESCRIBEPARAM, funcs));
    is_num(SQL_FUNC_EXISTS(funcs, 1), 0);
    return OK;
}


//=========================================================

//test for ODBC results API relate date types check
//1)SQLColAttribute
//2)SQLDescribeCol
//=========================================================
ODBC_TEST(test_SQLColAttribute_API)
{
    SQLLEN dataType= 0;

    CHECK_STMT_RC(Stmt, SQLPrepare(Stmt, (SQLCHAR*)"SELECT dt_boolean, dt_varchar, dt_char, dt_interval_year_to_month, dt_interval_day_to_second FROM test_tbl_alldatatypes", SQL_NTS));
    
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, &dataType));
    is_num(dataType, SQL_BIT);

    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 2, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, &dataType));
    is_num(dataType, SQL_VARCHAR);

    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 3, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, &dataType));
    is_num(dataType, SQL_CHAR);

    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 4, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, &dataType));
    is_num(dataType, SQL_VARCHAR);

    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 5, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, &dataType));
    is_num(dataType, SQL_VARCHAR);


    return OK;
}

ODBC_TEST(test_SQLDescribeCol_API)
{
    SQLCHAR   ColumnName[64];
    SQLSMALLINT dataType;

    CHECK_STMT_RC(Stmt, SQLPrepare(Stmt, (SQLCHAR*)"SELECT dt_boolean, dt_varchar, dt_char, dt_interval_year_to_month, dt_interval_day_to_second FROM test_tbl_alldatatypes", SQL_NTS));

    //check boolean type column
    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, 1, ColumnName, 64, &dataType, 0, 0, 0, 0));
    IS_STR(ColumnName, "dt_boolean", sizeof("dt_boolean"));
    is_num(dataType, SQL_BIT);

    //check varchar type column
    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, 2, ColumnName, 64, &dataType, 0, 0, 0, 0));
    IS_STR(ColumnName, "dt_varchar", sizeof("dt_varchar"));
    is_num(dataType, SQL_VARCHAR);

    //check char type column
    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, 3, ColumnName, 64, &dataType, 0, 0, 0, 0));
    IS_STR(ColumnName, "dt_char", sizeof("dt_char"));
    is_num(dataType, SQL_CHAR);

    //check interve type column
    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, 4, ColumnName, 64, &dataType, 0, 0, 0, 0));
    IS_STR(ColumnName, "dt_interval_year_to_month", sizeof("dt_interval_year_to_month"));
    is_num(dataType, SQL_VARCHAR);

    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, 5, ColumnName, 64, &dataType, 0, 0, 0, 0));
    IS_STR(ColumnName, "dt_interval_day_to_second", sizeof("dt_interval_day_to_second"));
    is_num(dataType, SQL_VARCHAR);

    return OK;
}



//  check 16 kind of data types value map to c data type(default conversion)
//
ODBC_TEST(test_data_query)
{
    SQLCHAR dtBooleanValue;
    
    SQLSCHAR dtTinyintValue;
    SQLSMALLINT dtSmallintValue;
    SQLINTEGER dtintegerValue;
    SQLBIGINT  dtBigintValue;

    SQLREAL   dtRealValue;
    SQLDOUBLE  dtDoubleValue;
    SQLCHAR    dtDecimalValue[128];

    SQLCHAR  dtVarcharValue[2048];
    SQLCHAR dtCharValue;
    SQLCHAR  dtVarbinaryValue[2048];

    SQL_DATE_STRUCT dtDateValue;
    SQL_TIME_STRUCT dtTimeValue;
    SQL_TIMESTAMP_STRUCT dtTimestampValue;

    SQLCHAR  dtIntervalY2MValue[2048];
    SQLCHAR  dtIntervalD2SValue[2048];

    //init data
    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                         "1, true, cast(1 as tinyint), cast(2 as smallint), cast(3 as integer), cast(4 as bigint),"
                         "REAL '9.2', DOUBLE '10.3', DECIMAL '11' ,"
                         "'Hello', cast('a' as char), X'65683F',"
                         "DATE '2001-08-22', TIME '01:02:03.456', TIMESTAMP '2001-08-22 03:04:05.321',"
                         "INTERVAL '3' MONTH, INTERVAL '2' DAY)");
    

    OK_SIMPLE_STMT(Stmt, "SELECT dt_boolean,"
                                 "dt_tinyint, dt_smallint, dt_integer, dt_bigint,"
                                 "dt_real, dt_double, dt_decimal,"
                                 "dt_varchar, dt_char, dt_varbinary,"
                                 "dt_date, dt_time, dt_timestamp,"
                                 "dt_interval_year_to_month, dt_interval_day_to_second FROM test_tbl_alldatatypes WHERE id = 1");
                                 
    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 1, SQL_C_BIT, &dtBooleanValue, 0, NULL));

    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 2, SQL_C_STINYINT, &dtTinyintValue, 0, NULL));
    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 3, SQL_C_SSHORT, &dtSmallintValue, 0, NULL));
    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 4, SQL_C_SLONG, &dtintegerValue, 0, NULL));
    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 5, SQL_C_SBIGINT, &dtBigintValue, 0, NULL));
    
    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 6, SQL_C_FLOAT, &dtRealValue, 0, NULL));
    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 7, SQL_C_DOUBLE, &dtDoubleValue, 0, NULL));
    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 8, SQL_CHAR, &dtDecimalValue, 0, NULL));  //?

    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 9,  SQL_C_CHAR, dtVarcharValue, sizeof(dtVarcharValue), NULL));
    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 10, SQL_C_CHAR, &dtCharValue, 0, NULL));
    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 11, SQL_C_BINARY, dtVarbinaryValue, sizeof(dtVarbinaryValue), NULL));

    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 12, SQL_C_TYPE_DATE, &dtDateValue, 0, NULL));
    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 13, SQL_C_TYPE_TIME, &dtTimeValue, 0, NULL));
    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 14, SQL_C_TYPE_TIMESTAMP, &dtTimestampValue, 0, NULL));

    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 15, SQL_C_CHAR, dtIntervalY2MValue, sizeof(dtIntervalY2MValue), NULL));
    CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, 16, SQL_C_CHAR, dtIntervalD2SValue, sizeof(dtIntervalD2SValue), NULL));


    CHECK_STMT_RC(Stmt, SQLFetchScroll(Stmt, SQL_FETCH_FIRST, 1L));

    FAIL_IF(dtBooleanValue != 1, "");
    
    FAIL_IF(dtTinyintValue != 1, "");
    FAIL_IF(dtSmallintValue != 2, "");
    FAIL_IF(dtintegerValue != 3, "");
    FAIL_IF(dtBigintValue != 4, "");


    IS_STR(dtVarcharValue, "Hello", sizeof("Hello"));
   // IS_STR(dtVarbinaryValue, "Hello", sizeof("Hello"));

    IS_STR(dtIntervalY2MValue, "0-3", sizeof("0-3"));
    IS_STR(dtIntervalD2SValue, "2 00:00:00.000", sizeof("2 00:00:00.000"));

    return OK;
}

#define BUFF_LEN 255

typedef struct
{
    SQLLEN      id;
    SQL_DATE_STRUCT date;
    SQL_TIME_STRUCT  time;
    SQL_TIMESTAMP_STRUCT timestamp;

} DateAsserts;

ODBC_TEST(test_DatesAndTime)
{
    /*Prepare Working Table*/
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_tbl_datesandtime");

    OK_SIMPLE_STMT(Stmt, "create table test_tbl_datesandtime (\
                           id  integer,\
                           dt_date date,\
                           dt_time time,\
                           dt_timestamp timestamp) COMMENT 'date and time types'");
    /*Init Variables*/
    SQLLEN    nlen = 0;
    SQLLEN    colnum = 0;

    
    SQLLEN id=0;
    SQL_DATE_STRUCT val_date = { 0,0,0 };
    SQL_TIME_STRUCT val_time = { 0,0,0 };
    SQL_TIMESTAMP_STRUCT val_timestamp = { 0,0,0,0,0,0,0 };

    SQLSMALLINT TargetType[4] = { SQL_C_ULONG, SQL_C_TYPE_DATE, SQL_C_TYPE_TIME, SQL_C_TYPE_TIMESTAMP };

    SQLPOINTER valptr[4] = { &id,  &val_date, &val_time, &val_timestamp };


    /*Prepare Data*/
    DateAsserts record[] =
    {
        { 0, { 2001, 8, 22 }, { 1,2,3 }, { 2001,8,22,3,4,5,321000 } },

        { 1, { 0001, 01, 01 }, { 1,1,1 }, { 0001,01,01,1,1,1,1000 } },

        { 2, { 0000, 12, 30 }, { 0,0,0 }, { 0000,01,01,0,0,0,1000 } },

        { 3, { -0001, 01, 20 }, { 12,30,30 },{ -0001,12,31,13,29,31,499000 } },

        { 4, { 1900, 02, 28 },{ 18,00,00 },{ 1970,01,01,23,59,59,999000 } },

        { 5, { 1970, 01, 02 },{ 23,59,59 },{ 2038,01,19,03,14,07,999000 } },
    };


    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_datesandtime values(\
                          0, DATE '2001-08-22', TIME '01:02:03.123', TIMESTAMP '2001-08-22 03:04:05.321')");

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_datesandtime values(\
                          1, DATE '0001-01-01', TIME '01:01:01.001', TIMESTAMP '0001-01-01 01:01:01.001')");

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_datesandtime values(\
                          2, DATE '0000-12-30', TIME '00:00:00.000', TIMESTAMP '0000-01-01 00:00:00.001')");

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_datesandtime values(\
                          3, DATE '-0001-01-20', TIME '12:30:30.499', TIMESTAMP '-0001-12-31 13:29:31.499')");

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_datesandtime values(\
                          4, DATE '1900-02-28', TIME '18:00:00.000', TIMESTAMP '1970-01-01 23:59:59.999')");

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_datesandtime values(\
                          5, DATE '1970-01-02', TIME '23:59:59.000', TIMESTAMP '2038-01-19 03:14:07.999')");


    OK_SIMPLE_STMT(Stmt, "SELECT id, dt_date date, dt_time time, dt_timestamp timestamp FROM test_tbl_datesandtime order by id");


    CHECK_STMT_RC(Stmt, SQLNumResultCols(Stmt, &colnum));
    printf("colnum:%d\n", colnum);
    is_num(sizeof(TargetType) / sizeof(SQLSMALLINT), colnum);
    is_num(sizeof(valptr) / sizeof(void*), colnum);

    while (SQL_SUCCEEDED(SQLFetch(Stmt)))
    {
        for (int i = 0; i < colnum; i++) {
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, i + 1, TargetType[i], valptr[i],
                BUFF_LEN, &nlen));
            //printf("nlen:%d\n", nlen);
        }

        printf("%d\n", id);
        is_num(id, record[id].id);

        printf("%04d-", val_date.year);
        is_num(val_date.year, record[id].date.year);
        printf("%02d-", val_date.month);
        FAIL_IF(val_date.month > 12, "month can't larger than 12!");
        is_num(val_date.month, record[id].date.month);
        printf("%02d\n", val_date.day);
        FAIL_IF(val_date.day > 31, "day can't larger than 31!");
        is_num(val_date.day, record[id].date.day);

        printf("%02d:", val_time.hour);
        FAIL_IF(val_time.hour > 23, "hour can't larger than 23!");
        is_num(val_time.hour, record[id].time.hour);
        printf("%02d:", val_time.minute);
        FAIL_IF(val_time.minute > 59, "minute can't larger than 59!");
        is_num(val_time.minute, record[id].time.minute);
        printf("%02d\n", val_time.second);
        FAIL_IF(val_time.second > 59, "second can't larger than 59!");
        is_num(val_time.second, record[id].time.second);

        printf("%04d-", val_timestamp.year);
        is_num(val_timestamp.year, record[id].timestamp.year);
        printf("%02d-", val_timestamp.month);
        FAIL_IF(val_timestamp.month > 12, "month can't larger than 12!");
        is_num(val_timestamp.month, record[id].timestamp.month);
        printf("%02d ", val_timestamp.day);
        FAIL_IF(val_timestamp.day > 31, "day can't larger than 31!");
        is_num(val_timestamp.day, record[id].timestamp.day);
        printf("%02d:", val_timestamp.hour);
        FAIL_IF(val_timestamp.hour > 23, "hour can't larger than 23!");
        is_num(val_timestamp.hour, record[id].timestamp.hour);
        printf("%02d:", val_timestamp.minute);
        FAIL_IF(val_timestamp.minute > 59, "minute can't larger than 59!");
        is_num(val_timestamp.minute, record[id].timestamp.minute);
        printf("%02d\.", val_timestamp.second);
        FAIL_IF(val_timestamp.second > 59, "second can't larger than 59!");
        is_num(val_timestamp.second, record[id].timestamp.second);
        printf("%06d\n", val_timestamp.fraction);
        is_num(val_timestamp.fraction, record[id].timestamp.fraction);
    }

    OK_SIMPLE_STMT(Stmt, "drop table if exists test_tbl_datesandtime");

    return OK;
}


/*****************************************************************/
// data type for metadata information relate apis:
//SQLGetTypeInfo[CATALOG]  db's support data types
//SQLColumns[CATALOG]   table's columns metadata
//SQLSpecialColumns[CATALOG]    rowid etc spacial column(not support)
//SQLProcedureColumns[CATALOG]  proceduce(not support)

//SQLColAttribute[results]  attribute of SQL_DESC_CONCISE_TYPE
//SQLDescribeCol[results]  resultset's metadata
//SQLDescribeParam[stmt]   intput paramete metadata
/*****************************************************************/

MA_ODBC_TESTS my_tests[]=
{
    {tests_setup,                "setup_dataset"},
    {test_SQLGetTypeInfo_API,    "test_SQLGetTypeInfo_API"},
    {test_SQLColumns_API,        "test_SQLColumns_API"},
    {test_SQLColAttribute_API,   "test_SQLColAttribute_API"},
    {test_SQLDescribeParam_API,  "test_SQLDescribeCol_API"},
    {test_data_query,            "test_data_query"},
    {test_DatesAndTime,          "test_DatesAndTime"},
    {tests_cleanup,              "clean_up_dataset"},
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

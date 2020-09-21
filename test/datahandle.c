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
#include "math.h"


#define BUFF_LEN 255

void preparedata()
{
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_tbl_alldatatypes");

    OK_SIMPLE_STMT(Stmt, "create table test_tbl_alldatatypes (\
                           id  integer,\
                           dt_boolean  boolean,\
                           dt_tinyint tinyint,\
                           dt_smallint smallint,\
                           dt_integer integer,\
                           dt_bigint bigint,\
                           dt_real real,\
                           dt_double double,\
                           dt_decimal decimal(10,6),\
                           dt_varchar varchar,\
                           dt_char char,\
                           dt_varbinary varbinary,\
                           dt_date date,\
                           dt_time time,\
                           dt_timestamp timestamp,\
                           dt_interval_year_to_month  INTERVAL YEAR TO MONTH,\
                           dt_interval_day_to_second  INTERVAL DAY TO SECOND\
                         ) COMMENT 'all datatype support'");
}

void cleanup()
{
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_tbl_alldatatypes");
}

typedef struct
{
    SQLLEN      id;
    SQLSMALLINT boolval;
    SQLSMALLINT tinyval;
    SQLSMALLINT smallval;
    SQLINTEGER  intval;
    SQLLEN      bigval;
    SQLDOUBLE   realval;
    SQLDOUBLE   doubleval;
    char *decimal;
    char *varcharval;
    char *charval;
    char *varbin;
    SQL_DATE_STRUCT date;
    SQL_TIME_STRUCT  time;
    SQL_TIMESTAMP_STRUCT timestamp;
    char *interval_year_to_month;
    char *interval_day_to_second;

} assertvalues;


ODBC_TEST(test_getdata_row)
{
    /*Prepare Data in first case*/
    preparedata();

    SQLLEN    nlen = 0;
    SQLLEN    colnum = 0;
    SQLLEN    check_double = -1;

    /*SQLCHAR dt_varbinary1[4];
    memcpy(dt_varbinary1, "\x65\x68\x3f\x97", 4);*/

    SQLLEN id = 0, val_int = 0, val_big=0;
    SQLSMALLINT val_boolean=0, val_tiny=0, val_small=0;
    SQLDOUBLE val_real = 0, val_double = 0;
    SQLCHAR val_decimal[BUFF_LEN], val_varchar[BUFF_LEN], val_char[BUFF_LEN], val_interval_year[BUFF_LEN], val_interval_day[BUFF_LEN];
    SQL_DATE_STRUCT val_date = {0,0,0};
    SQL_TIME_STRUCT val_time = {0,0,0};
    SQL_TIMESTAMP_STRUCT val_timestamp = { 0,0,0,0,0,0,0 };

    SQLCHAR val_varbin[BUFF_LEN];
    memset(val_varbin, 0, BUFF_LEN);

    SQLSMALLINT TargetType[17] = { SQL_C_ULONG, SQL_C_BIT, SQL_C_TINYINT, SQL_C_SHORT, SQL_C_LONG,
                                     SQL_C_SBIGINT, SQL_C_DOUBLE, SQL_C_DOUBLE, SQL_C_CHAR,
                                     SQL_C_CHAR, SQL_C_CHAR, SQL_C_BINARY,
                                     SQL_C_TYPE_DATE, SQL_C_TYPE_TIME, SQL_C_TYPE_TIMESTAMP,
                                     SQL_C_CHAR, SQL_C_CHAR};

    SQLPOINTER valptr[17] = { &id, &val_boolean, &val_tiny, &val_small, &val_int, 
                                &val_big, &val_real, &val_double, &val_decimal,
                                &val_varchar, &val_char, &val_varbin,
                                &val_date, &val_time, &val_timestamp,
                                &val_interval_year, &val_interval_day};

    assertvalues record[] = 
    {
        { 0,1,1,1,1,1,10.3,10.3,"10.123450","Hello","a", "\x65\x68\x3f\x97",
            {0000, 8, 22},{1,2,3}, {0000,8,22,3,4,5,321000}, "0-3", "2 00:00:00.000" }, 

        { 1,0,127,1,2147483647,1,10.3,10.3,"9999.123456","Hello","a","\xca\xfe\xba\xbe",
            { -10, 8, 22 },{ 1,2,3 },{ -10,8,22,3,4,5,999000 }, "3-0", "0 02:00:00.000" }, 
    };

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values(\
                          0, true, cast(1 as tinyint), cast(1 as smallint), cast(1 as integer), cast(1 as bigint),\
                          REAL '10.3', DOUBLE '10.3', DECIMAL '10.12345' ,\
                          'Hello', cast('a' as char), X'65683F97',\
                          DATE '0000-08-22', TIME '01:02:03.456', TIMESTAMP '0000-08-22 03:04:05.321',\
                          INTERVAL '3' MONTH, INTERVAL '2' DAY)");

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values(\
                          1, false, cast(127 as tinyint), cast(1 as smallint), 2147483647, cast(1 as bigint),\
                          REAL '10.3', DOUBLE '10.3', DECIMAL '9999.123456' ,\
                          'Hello', cast('a' as char), X'cafebabe',\
                          DATE '-0010-08-22', TIME '01:02:03.456', TIMESTAMP '-0010-08-22 03:04:05.999',\
                          INTERVAL '3' YEAR, INTERVAL '2' HOUR)");

    OK_SIMPLE_STMT(Stmt, "SELECT id, dt_boolean, dt_tinyint, dt_smallint, dt_integer integer,\
                          dt_bigint bigint, dt_real real, dt_double double, dt_decimal decimal,\
                          dt_varchar, dt_char, dt_varbinary varbinary,\
                          dt_date date, dt_time time, dt_timestamp timestamp,\
                          dt_interval_year_to_month, dt_interval_day_to_second FROM test_tbl_alldatatypes");


    CHECK_STMT_RC(Stmt, SQLNumResultCols(Stmt, &colnum));
    printf("colnum:%d\n", colnum);
    is_num(sizeof(TargetType)/sizeof(SQLSMALLINT), colnum);
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
        printf("%d\n", val_boolean);
        is_num(val_boolean, record[id].boolval);
        printf("%d\n", val_tiny);
        is_num(val_tiny, record[id].tinyval);
        printf("%d\n", val_small);
        is_num(val_small, record[id].smallval);
        printf("%d\n", val_int);
        is_num(val_int, record[id].intval);
        printf("%d\n", val_big);
        is_num(val_big, record[id].bigval);

        printf("%f\n", val_real);
        check_double = (fabs(val_real - record[id].realval) < 1e-6) ? 0 : -1;
        is_num(check_double, 0);
        printf("%f\n", val_double);
        check_double = (fabs(val_double - record[id].doubleval) < 1e-6) ? 0 : -1;
        is_num(check_double, 0);

        printf("%s\n", val_decimal);
        IS_STR(val_decimal, record[id].decimal, sizeof(record[id].decimal));
        printf("%s\n", val_varchar);
        IS_STR(val_varchar, record[id].varcharval, sizeof(record[id].varcharval));
        printf("%s\n", val_char);
        IS_STR(val_char, record[id].charval, sizeof(record[id].charval));

        int ind = 0;
        while (val_varbin[ind] != 0) {
            printf("%X\n", val_varbin[ind]);
            //Using String, the last byte will be padded with FF in high byte
            is_num(val_varbin[ind], record[id].varbin[ind] & 0X0FF);
            ind++;
        }


        printf("\n%04d-", val_date.year);
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

        printf("%s\n", val_interval_year);
        IS_STR(val_interval_year, record[id].interval_year_to_month, sizeof(record[id].interval_year_to_month));
        printf("%s\n", val_interval_day);
        IS_STR(val_interval_day, record[id].interval_day_to_second, sizeof(record[id].interval_day_to_second));
    }

    return OK;
}

ODBC_TEST(test_null_row)
{

    SQLLEN    colnum = 0;
    SQLLEN    check_double = -1;
    SQLSMALLINT val_tiny = 0;
    SQLLEN val_int = 0, val_big = 0;
    SQLDOUBLE val_double = 0.0;
    SQLCHAR val_varchar[BUFF_LEN];
    memset(val_varchar, 0, BUFF_LEN);

    SQLCHAR val_char[BUFF_LEN];
    memset(val_char, 0, BUFF_LEN);

    SQL_TIME_STRUCT val_time = { 0, 0, 0 };

    SQLSMALLINT TargetType[] = { SQL_C_SHORT, SQL_C_LONG, SQL_C_SBIGINT,
                                  SQL_C_DOUBLE, SQL_C_CHAR,
                                  SQL_C_CHAR, SQL_C_TYPE_TIME };

    SQLPOINTER valptr[] = { &val_tiny, &val_int, &val_big,
                             &val_double, &val_varchar,
                             &val_char, &val_time };

    SQLLEN lenind[] = { 0,0,0,0,0,0,0 };


    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values(\
                          1, true, cast(NULL as tinyint), cast(1 as smallint), cast(NULL as integer), cast(NULL as bigint),\
                          REAL '10.3', cast(NULL as double), DECIMAL '10.12345' ,\
                          '', '', X'65683F97',\
                          DATE '2001-08-22', cast(NULL as TIME), TIMESTAMP '2001-08-22 03:04:05.321',\
                          INTERVAL '3' MONTH, INTERVAL '2' DAY)");

    OK_SIMPLE_STMT(Stmt, "SELECT dt_tinyint, dt_integer integer, dt_bigint bigint,\
                          dt_double double, dt_varchar, dt_char, \
                          dt_time time FROM test_tbl_alldatatypes");

    CHECK_STMT_RC(Stmt, SQLNumResultCols(Stmt, &colnum));
    is_num(sizeof(TargetType) / sizeof(SQLSMALLINT), colnum);
    is_num(sizeof(valptr) / sizeof(void*), colnum);
    is_num(sizeof(lenind) / sizeof(SQLLEN), colnum);

    for(int i = 0; i < colnum; i++)
    {
        CHECK_STMT_RC(Stmt, SQLBindCol(Stmt, i+1, TargetType[i], valptr[i], BUFF_LEN, &lenind[i]));
    }

    while (SQL_SUCCEEDED(SQLFetch(Stmt)))
    {
        if (lenind[0] > 0) continue;

        printf("lenind:%d\n", lenind[0]);
        printf("%d\n", val_tiny);
        FAIL_IF(lenind[0] != SQL_NULL_DATA, "While fetching nulldata StrLen_or_Ind is SQL_NULL_DATA(-1)");

        printf("lenind:%d\n", lenind[1]);
        printf("%d\n", val_int);
        FAIL_IF(lenind[1] != SQL_NULL_DATA, "While fetching nulldata StrLen_or_Ind is SQL_NULL_DATA(-1)");

        printf("lenind:%d\n", lenind[2]);
        printf("%d\n", val_big);
        FAIL_IF(lenind[2] != SQL_NULL_DATA, "While fetching nulldata StrLen_or_Ind is SQL_NULL_DATA(-1)");

        printf("lenind:%d\n", lenind[3]);
        printf("%f\n", val_double);
        FAIL_IF(lenind[3] != SQL_NULL_DATA, "While fetching nulldata StrLen_or_Ind is SQL_NULL_DATA(-1)");

        printf("lenind:%d\n", lenind[4]);
        printf("%s\n", val_varchar);
        FAIL_IF(lenind[4] != 0, "while fetching an empty string like \"\" of varchar StrLen_or_Ind will be 0");

        printf("lenind:%d\n", lenind[5]);
        printf("%s\n", val_char);
        FAIL_IF(lenind[5] != 1, "while fetching an empty character like '' of char StrLen_or_Ind will be 1");

        printf("lenind:%d\n", lenind[6]);
        printf("%02d:", val_time.hour);
        printf("%02d:", val_time.minute);
        printf("%02d\n", val_time.second);
        FAIL_IF(lenind[6] != SQL_NULL_DATA, "While fetching nulldata StrLen_or_Ind is SQL_NULL_DATA(-1)");
    }

    cleanup();
    return OK;
}

MA_ODBC_TESTS my_tests[]=
{
    { test_getdata_row, "test_getdata_row" },
    { test_null_row, "test_null_row" },
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

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
                           "dt_timestamp1 timestamp,"
                           "dt_timestamp2 timestamp,"
                           "dt_interval_year_to_month  INTERVAL YEAR TO MONTH,"
                           "dt_interval_day_to_second  INTERVAL DAY TO SECOND"
                         ") COMMENT 'all datatype support'");

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                         "1, true, cast(1 as tinyint), cast(2 as smallint), cast(3 as integer), cast(4 as bigint),"
                         "REAL '9.2', DOUBLE '10.3', DECIMAL '11' ,"
                         "'Hello', cast('a' as char), X'65683F',"
                         "DATE '2001-08-22', TIME '01:02:03.456', TIMESTAMP '2001-08-22 03:04:05.321', TIMESTAMP '2002-09-22 03:04:05.321',"
                         "INTERVAL '3' MONTH, INTERVAL '2' DAY)");

    return OK;
}


ODBC_TEST(tests_cleanup)
{
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_tbl_alldatatypes");
    return OK;
}


//=========================================================
//Date, Time, and Timestamp Escape Sequences testcase
//=========================================================
ODBC_TEST(tests_dateEscapeSequences)
{
    SQLCHAR rsValueBuf1[1000] = {'\0'};

    //==========================   {d 'value'}    ==========================
    OK_SIMPLE_STMT(Stmt, "select {d '2010-10-02'}");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2010-10-02", sizeof("2010-10-02"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   {t 'value'}   ==========================
    OK_SIMPLE_STMT(Stmt, "select {t '01:02:03.456'}");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "01:02:03", sizeof("01:02:03"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   {ts 'value'}   ==========================
    OK_SIMPLE_STMT(Stmt, "select {ts '2011-12-23 23:59:59.999'}");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2011-12-23 23:59:59", sizeof("2011-12-23 23:59:59"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    return OK;
}

//=========================================================
//LIKE Escape Sequence testcase
//1) function test
//2)relate capabilities setting:
//2.1)SQL_LIKE_ESCAPE_CLAUSE
//=========================================================
ODBC_TEST(tests_likeEscapeSequences)
{
    SQLCHAR rsValueBuf1[1000] = {'\0'};
    
    SQLCHAR stringValue4InfoValue[1024] = {'\0'};
    SQLSMALLINT length;

    //==========================   escape sequences    ==========================
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_LIKE_ESCAPE_CLAUSE, &stringValue4InfoValue,
                        sizeof(stringValue4InfoValue), &length));
    IS_STR(stringValue4InfoValue, "Y", 1);
    IS(length == 1);

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                             "201, null, null, null, null, null, null, null, null,"
                             "'test_stmtadesc_attr',"
                             "null, null, null, null, null, null, null, null)");
    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                             "202, null, null, null, null,null, null, null, null,"
                             "'testx_stmt_desc_attr'," 
                             "null, null, null, null, null, null, null, null)");
    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                             "203, null, null, null, null, null, null, null, null,"
                             "'test_stmt_%_attr',"
                             "null, null, null, null, null, null, null, null)");

    OK_SIMPLE_STMT(Stmt, "select dt_varchar from test_tbl_alldatatypes where id in (201,202,203) and dt_varchar like 'testx_stmt_desc_attr'");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "testx_stmt_desc_attr", sizeof("testx_stmt_desc_attr"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select dt_varchar from test_tbl_alldatatypes where id in (201,202,203) and dt_varchar like 'testx_stmt_desc_attr' {escape 'x'}");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "test_stmtadesc_attr", sizeof("test_stmtadesc_attr"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select dt_varchar from test_tbl_alldatatypes where id in (201,202,203) and dt_varchar like 'test\\_stmt\\_\\%\\_attr' {escape '\\'}");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "test_stmt_%_attr", sizeof("test_stmt_%_attr"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select dt_varchar from test_tbl_alldatatypes where id in (201,202,203) and dt_varchar like 'testx_stmt%'");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "testx_stmt_desc_attr", sizeof("testx_stmt_desc_attr"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    return OK;
}


//=========================================================
//String Functions testcase
//1) function test
//2)relate capabilities setting:
//2.1)SQL_STRING_FUNCTIONS and bitmask 
//         SQL_FN_STR_ASCII (ODBC 1.0)
//         SQL_FN_STR_BIT_LENGTH (ODBC 3.0)
//         SQL_FN_STR_CHAR (ODBC 1.0)
//         SQL_FN_STR_CHAR_LENGTH (ODBC 3.0)
//         SQL_FN_STR_CHARACTER_LENGTH (ODBC 3.0)
//         SQL_FN_STR_CONCAT (ODBC 1.0)
//         SQL_FN_STR_DIFFERENCE (ODBC 2.0)
//         SQL_FN_STR_INSERT (ODBC 1.0)
//         SQL_FN_STR_LCASE (ODBC 1.0)
//         SQL_FN_STR_LEFT (ODBC 1.0)
//         SQL_FN_STR_LENGTH (ODBC 1.0)
//         SQL_FN_STR_LOCATE (ODBC 1.0)
//         SQL_FN_STR_LTRIM (ODBC 1.0) 
//         SQL_FN_STR_OCTET_LENGTH (ODBC 3.0) 
//         SQL_FN_STR_POSITION (ODBC 3.0)
//         SQL_FN_STR_REPEAT (ODBC 1.0)
//         SQL_FN_STR_REPLACE (ODBC 1.0)
//         SQL_FN_STR_RIGHT (ODBC 1.0)
//         SQL_FN_STR_RTRIM (ODBC 1.0)
//         SQL_FN_STR_SOUNDEX (ODBC 2.0)
//         SQL_FN_STR_SPACE (ODBC 2.0)
//         SQL_FN_STR_SUBSTRING (ODBC 1.0)
//         SQL_FN_STR_UCASE (ODBC 1.0)
//=========================================================
ODBC_TEST(tests_stringScalarFuncES)
{
    SQLCHAR rsValueBuf1[1000] = {'\0'};

    SQLUINTEGER unitBitmask4InfoValue;
    SQLSMALLINT length;

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                             "301, null, null, null, null, null, null, null, null,"
                             "'test_stmt_attr',"
                             "null, null, null, null, null, null, null, null)");
    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                             "302, null, null, null, null, null, null, null, null,"
                             "'UPPERCASE',"
                             "null, null, null, null, null, null, null, null)");
    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                             "303, null, null, null, null,null,null, null, null,"
                             "'lowercase',"
                             "null, null, null, null, null, null, null, null)");
    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                             "304, null, null, null, null, null, null, null, null,"
                             "'1234567890',"
                             "null, null, null, null, null, null, null, null)");
    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                             "305, null, null, null, null, null, null, null, null,"
                             "'12345 67890 ',"
                             "null, null, null, null, null, null, null, null)");
    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                             "306, null, null, null, null, null, null, null, null,"
                             "'  abc efg ',"
                             "null, null, null, null, null, null, null, null)");
    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                             "307, null, null, null, null, null, null, null, null,"
                             "'abc',"
                             "null, null, null, null, null, null, null, null)");
    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                             "308, null, null, null, null, null, null, null, null,"
                             "'i tell mike and i will back',"
                             "null, null, null, null, null, null, null, null)");
    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                             "309, null, null, null, null, null, null, null, null,"
                             "'jack tell mike and jack will go to school',"
                             "null, null, null, null, null, null, null, null)");

    //==========================   ASCII   ==========================
    //ODBC 1.0 ASCII(string_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_ASCII, SQL_FN_STR_ASCII);

    OK_SIMPLE_STMT(Stmt, "select {fn ASCII(dt_varchar)} from test_tbl_alldatatypes where id = 301");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 116);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   CHAR   ==========================
    //ODBC 1.0 CHAR(code)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_CHAR, SQL_FN_STR_CHAR);
    
    OK_SIMPLE_STMT(Stmt, "select {fn CHAR(36)}");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "$", sizeof("$"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   CONCAT   ==========================
    //ODBC 1.0 CONCAT(string_exp1, string_exp2)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_CONCAT, SQL_FN_STR_CONCAT);
    
    OK_SIMPLE_STMT(Stmt, "select {fn CONCAT(dt_varchar, '--')} from test_tbl_alldatatypes where id = 301");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "test_stmt_attr--", sizeof("test_stmt_attr--"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   LCASE   ==========================
    //ODBC 1.0     LCASE(string_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_LCASE, SQL_FN_STR_LCASE);

    OK_SIMPLE_STMT(Stmt, "select {fn LCASE(dt_varchar)} from test_tbl_alldatatypes where id = 302");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "uppercase", sizeof("uppercase"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   UCASE   ==========================
    //ODBC 1.0 UCASE(string_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_UCASE, SQL_FN_STR_UCASE);


    OK_SIMPLE_STMT(Stmt, "select {fn UCASE(dt_varchar)} from test_tbl_alldatatypes where id = 303");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "LOWERCASE", sizeof("LOWERCASE"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   LEFT   ==========================
    //ODBC 1.0 LEFT(string_exp, count)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_LEFT, SQL_FN_STR_LEFT);

    OK_SIMPLE_STMT(Stmt, "select {fn LEFT(dt_varchar, 3)} from test_tbl_alldatatypes where id = 304");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "3", sizeof("3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   RIGHT   ==========================
    //ODBC 1.0 RIGHT(string_exp, count)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_RIGHT, SQL_FN_STR_RIGHT);
    
    OK_SIMPLE_STMT(Stmt, "select {fn RIGHT(dt_varchar, 3)} from test_tbl_alldatatypes where id = 304");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "8", sizeof("8"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   LENGTH   ==========================
    //ODBC 1.0 LENGTH(string_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_LENGTH, SQL_FN_STR_LENGTH);

    OK_SIMPLE_STMT(Stmt, "select {fn LENGTH(dt_varchar)} from test_tbl_alldatatypes where id = 305");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 11);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn LENGTH(dt_varchar)} from test_tbl_alldatatypes where id = 306");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 9);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   LTRIM   ==========================
    //ODBC 1.0 LTRIM(string_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_LTRIM, SQL_FN_STR_LTRIM);
    
    OK_SIMPLE_STMT(Stmt, "select {fn LTRIM(dt_varchar)} from test_tbl_alldatatypes where id = 305");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "12345 67890 ", sizeof("12345 67890 "));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn LTRIM(dt_varchar)} from test_tbl_alldatatypes where id = 306");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "abc efg ", sizeof("abc efg "));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   RTRIM   ==========================
    //ODBC 1.0 REPLACE(string_exp1, string_exp2, string_exp3)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_RTRIM, SQL_FN_STR_RTRIM);
    
    OK_SIMPLE_STMT(Stmt, "select {fn RTRIM(dt_varchar)} from test_tbl_alldatatypes where id = 305");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "12345 67890", sizeof("12345 67890"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
    OK_SIMPLE_STMT(Stmt, "select {fn RTRIM(dt_varchar)} from test_tbl_alldatatypes where id = 306");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "  abc efg", sizeof("  abc efg"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

     //==========================   POSITION   ==========================
     //ODBC 3.0 POSITION(character_exp IN character_exp)
     CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                         sizeof(unitBitmask4InfoValue), &length));
     is_num(unitBitmask4InfoValue & SQL_FN_STR_POSITION, SQL_FN_STR_POSITION);
    
     OK_SIMPLE_STMT(Stmt, "select {fn POSITION('efg' IN dt_varchar)} from test_tbl_alldatatypes where id = 306");
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
     is_num(my_fetch_int(Stmt, 1), 7);
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   REPEAT   ==========================
    //ODBC 1.0 REPEAT(string_exp, count)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_REPEAT, SQL_FN_STR_REPEAT);

    OK_SIMPLE_STMT(Stmt, "select {fn REPEAT(dt_varchar, 2)} from test_tbl_alldatatypes where id = 307");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "abcabc", sizeof("abcabc"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   REPLACE   ==========================
    //ODBC 1.0 REPLACE(string_exp1, string_exp2, string_exp3)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_REPLACE, SQL_FN_STR_REPLACE);

    OK_SIMPLE_STMT(Stmt, "select {fn REPLACE(dt_varchar, 'mike', 'you')} from test_tbl_alldatatypes where id = 308");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "i tell you and i will back", sizeof("i tell you and i will back"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   SPACE   ==========================
    //ODBC 1.0 RIGHT(string_exp, count)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_SPACE, SQL_FN_STR_SPACE);
    
    OK_SIMPLE_STMT(Stmt, "select {fn SPACE(2)}");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "  ", sizeof("  "));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   REPLACE   ==========================
    //ODBC 1.0 REPLACE(string_exp1, string_exp2, string_exp3)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_SUBSTRING, SQL_FN_STR_SUBSTRING);
    
    OK_SIMPLE_STMT(Stmt, "select {fn SUBSTRING(dt_varchar, 17, 11)} from test_tbl_alldatatypes where id = 308");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "i will back", sizeof("i will back"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   LOCATE   ==========================
    //ODBC 1.0 LOCATE(string_exp1, string_exp2[, start])
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_LOCATE, SQL_FN_STR_LOCATE);

    OK_SIMPLE_STMT(Stmt, "select {fn LOCATE(dt_varchar, 'jack')} from test_tbl_alldatatypes where id = 309");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn LOCATE(dt_varchar, 'jack', 5)} from test_tbl_alldatatypes where id = 309");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 20);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   not support   ==========================
    //ODBC 2.0      DIFFERENCE(string_exp1, string_exp2)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_DIFFERENCE, 0);

    //ODBC 1.0      INSERT(string_exp1, start, length, string_exp2)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_INSERT, 0);

    //ODBC 2.0         SOUNDEX(string_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_SOUNDEX, 0);

    //ODBC 3.0 BIT_LENGTH(string_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_BIT_LENGTH, 0);

    //==========================   CHAR_LENGTH   ==========================
    //ODBC 3.0 CHAR_LENGTH(string_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_CHAR_LENGTH, 0);

    //==========================   CHARACTER_LENGTH   ==========================
    //ODBC 3.0 CHARACTER_LENGTH(string_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_CHARACTER_LENGTH, 0);

    //==========================   OCTET_LENGTH   ==========================
    //ODBC 3.0 OCTET_LENGTH(string_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_STR_OCTET_LENGTH, 0);

    return OK;
}

//=========================================================
//Numeric Functions testcase
//1) function test
//2)relate capabilities setting:
//2.1)SQL_NUMERIC_FUNCTIONS and bitmask 
//         SQL_FN_NUM_ABS (ODBC 1.0)
//         SQL_FN_NUM_ACOS (ODBC 1.0)
//         SQL_FN_NUM_ASIN (ODBC 1.0)
//         SQL_FN_NUM_ATAN (ODBC 1.0)
//         SQL_FN_NUM_ATAN2 (ODBC 1.0)
//         SQL_FN_NUM_CEILING (ODBC 1.0)
//         SQL_FN_NUM_COS (ODBC 1.0)
//         SQL_FN_NUM_COT (ODBC 1.0)
//         SQL_FN_NUM_DEGREES (ODBC 2.0)
//         SQL_FN_NUM_EXP (ODBC 1.0)
//         SQL_FN_NUM_FLOOR (ODBC 1.0)
//         SQL_FN_NUM_LOG (ODBC 1.0)
//         SQL_FN_NUM_LOG10 (ODBC 2.0)
//         SQL_FN_NUM_MOD (ODBC 1.0)
//         SQL_FN_NUM_PI (ODBC 1.0)
//         SQL_FN_NUM_POWER (ODBC 2.0)
//         SQL_FN_NUM_RADIANS (ODBC 2.0)
//         SQL_FN_NUM_RAND (ODBC 1.0)
//         SQL_FN_NUM_ROUND (ODBC 2.0)
//         SQL_FN_NUM_SIGN (ODBC 1.0)
//         SQL_FN_NUM_SIN (ODBC 1.0)
//         SQL_FN_NUM_SQRT (ODBC 1.0)
//         SQL_FN_NUM_TAN (ODBC 1.0)
//         SQL_FN_NUM_TRUNCATE (ODBC 2.0)
ODBC_TEST(tests_numericScalarFuncES)
{
    SQLCHAR rsValueBuf1[1000] = {'\0'};

    SQLUINTEGER unitBitmask4InfoValue;
    SQLSMALLINT length;

    //just invoke function not validate result
    //==========================   ABS   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_ABS, SQL_FN_NUM_ABS);

    OK_SIMPLE_STMT(Stmt, "select {fn ABS(dt_integer)} from test_tbl_alldatatypes where id = 1");


    //==========================   ACOS   ==========================
    //ODBC 1.0 ACOS(float_exp) 
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_ACOS, SQL_FN_NUM_ACOS);

    OK_SIMPLE_STMT(Stmt, "select {fn ACOS(dt_real)} from test_tbl_alldatatypes where id = 1");

    
    //==========================   ASIN   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_ASIN, SQL_FN_NUM_ASIN);
    
    OK_SIMPLE_STMT(Stmt, "select {fn ASIN(dt_real)} from test_tbl_alldatatypes where id = 1");

    //==========================   ATAN   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_ATAN, SQL_FN_NUM_ATAN);
    
    OK_SIMPLE_STMT(Stmt, "select {fn ATAN(dt_real)} from test_tbl_alldatatypes where id = 1");
    //==========================   ATAN2   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_ATAN2, SQL_FN_NUM_ATAN2);
    
    OK_SIMPLE_STMT(Stmt, "select {fn ATAN2(dt_real, dt_double)} from test_tbl_alldatatypes where id = 1");

    //==========================   CEILING   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_CEILING, SQL_FN_NUM_CEILING);
    
    OK_SIMPLE_STMT(Stmt, "select {fn CEILING(dt_tinyint)} from test_tbl_alldatatypes where id = 1");
    
    //==========================   COS   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_COS, SQL_FN_NUM_COS);
    
    OK_SIMPLE_STMT(Stmt, "select {fn COS(dt_real)} from test_tbl_alldatatypes where id = 1");

    //==========================   DEGREES   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_DEGREES, SQL_FN_NUM_DEGREES);
    
    OK_SIMPLE_STMT(Stmt, "select {fn DEGREES(dt_integer)} from test_tbl_alldatatypes where id = 1");

    //==========================   EXP   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_EXP, SQL_FN_NUM_EXP);
    
    OK_SIMPLE_STMT(Stmt, "select {fn EXP(dt_real)} from test_tbl_alldatatypes where id = 1");

    //==========================   FLOOR   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_FLOOR, SQL_FN_NUM_FLOOR);
    
    OK_SIMPLE_STMT(Stmt, "select {fn FLOOR(dt_integer)} from test_tbl_alldatatypes where id = 1");
    
    //==========================   LOG   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_LOG, SQL_FN_NUM_LOG);
    
    OK_SIMPLE_STMT(Stmt, "select {fn LOG(dt_real)} from test_tbl_alldatatypes where id = 1");

    //==========================   LOG10   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_LOG10, SQL_FN_NUM_LOG10);
    
    OK_SIMPLE_STMT(Stmt, "select {fn LOG10(dt_real)} from test_tbl_alldatatypes where id = 1");

    //==========================   MOD   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_MOD, SQL_FN_NUM_MOD);
    
    OK_SIMPLE_STMT(Stmt, "select {fn MOD(dt_integer,2)} from test_tbl_alldatatypes where id = 1");

    //==========================   PI   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_PI, SQL_FN_NUM_PI);
    
    OK_SIMPLE_STMT(Stmt, "select {fn PI()}");
    
    //==========================   POWER   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_POWER, SQL_FN_NUM_POWER);
    
    OK_SIMPLE_STMT(Stmt, "select {fn POWER(dt_integer,2)} from test_tbl_alldatatypes where id = 1");

    //==========================   RADIANS   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_RADIANS, SQL_FN_NUM_RADIANS);
    
    OK_SIMPLE_STMT(Stmt, "select {fn RADIANS(dt_real)} from test_tbl_alldatatypes where id = 1");

    //==========================   RAND   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_RAND, SQL_FN_NUM_RAND);
    
    OK_SIMPLE_STMT(Stmt, "select {fn RAND(dt_integer)} from test_tbl_alldatatypes where id = 1");
    
    //==========================   SIGN   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_SIGN, SQL_FN_NUM_SIGN);
    
    OK_SIMPLE_STMT(Stmt, "select {fn SIGN(dt_real)} from test_tbl_alldatatypes where id = 1");

    //==========================   SIN   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_SIN, SQL_FN_NUM_SIN);
    
    OK_SIMPLE_STMT(Stmt, "select {fn SIN(dt_real)} from test_tbl_alldatatypes where id = 1");

    //==========================   SQRT   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_SQRT, SQL_FN_NUM_SQRT);
    
    OK_SIMPLE_STMT(Stmt, "select {fn SQRT(dt_real)} from test_tbl_alldatatypes where id = 1");

    //==========================   TAN   ==========================
    //ODBC 1.0 ABS(numeric_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_TAN, SQL_FN_NUM_TAN);
    
    OK_SIMPLE_STMT(Stmt, "select {fn TAN(dt_real)} from test_tbl_alldatatypes where id = 1");
    
    //==========================   not support   ==========================
    //ODBC 1.0      COT(float_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_COT, 0);

    //ODBC 2.0     TRUNCATE(numeric_exp, integer_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_NUM_TRUNCATE, 0);

    return OK;
}

//=========================================================
//Time, Date, and Interval Functions testcase
//1) function test
//2)relate capabilities setting:
//2.1)SQL_SQL92_DATETIME_FUNCTIONS and bitmask 
//         SQL_SDF_CURRENT_DATE
//         SQL_SDF_CURRENT_TIME
//         SQL_SDF_CURRENT_TIMESTAMP
//2.2)SQL_TIMEDATE_FUNCTIONS and bitmask 
//         SQL_FN_TD_CURRENT_DATE ODBC 3.0)
//         SQL_FN_TD_CURRENT_TIME (ODBC 3.0)
//         SQL_FN_TD_CURRENT_TIMESTAMP (ODBC 3.0)
//         SQL_FN_TD_CURDATE (ODBC 1.0)
//         SQL_FN_TD_CURTIME (ODBC 1.0) 
//         SQL_FN_TD_DAYNAME (ODBC 2.0)
//         SQL_FN_TD_DAYOFMONTH (ODBC 1.0)
//         SQL_FN_TD_DAYOFWEEK (ODBC 1.0)
//         SQL_FN_TD_DAYOFYEAR (ODBC 1.0) 
//         SQL_FN_TD_EXTRACT (ODBC 3.0)
//         SQL_FN_TD_HOUR (ODBC 1.0)
//         SQL_FN_TD_MINUTE (ODBC 1.0)
//         SQL_FN_TD_MONTH (ODBC 1.0)
//         SQL_FN_TD_MONTHNAME (ODBC 2.0)
//         SQL_FN_TD_NOW (ODBC 1.0)
//         SQL_FN_TD_QUARTER (ODBC 1.0)
//         SQL_FN_TD_SECOND (ODBC 1.0)
//         SQL_FN_TD_TIMESTAMPADD (ODBC 2.0)
//         SQL_FN_TD_TIMESTAMPDIFF (ODBC 2.0)
//         SQL_FN_TD_WEEK (ODBC 1.0)
//         SQL_FN_TD_YEAR (ODBC 1.0)
//=========================================================
ODBC_TEST(tests_dateAndTimeScalarFuncES)
{
    SQLCHAR rsValueBuf1[1000] = {'\0'};
    SQLCHAR rsValueBuf2[1000] = {'\0'};
    SQLCHAR rsValueBuf3[1000] = {'\0'};

    SQLUINTEGER unitBitmask4InfoValue;
    SQLSMALLINT length;

    //==========================   current date   ==========================
    //current date get server's runtime date, invoke server's current date by three grammar style method
    //ODBC 3.0 current date
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_CURRENT_DATE, SQL_FN_TD_CURRENT_DATE);

    OK_SIMPLE_STMT(Stmt, "select {fn CURRENT_DATE()}");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf1, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //ODBC 1.0 current date
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_CURDATE, SQL_FN_TD_CURDATE);

    OK_SIMPLE_STMT(Stmt, "select {fn CURDATE()}");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf2, 1), rsValueBuf1, sizeof(rsValueBuf1));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //SQL-92 current date
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_SQL92_DATETIME_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_SDF_CURRENT_DATE, SQL_SDF_CURRENT_DATE);
    
    OK_SIMPLE_STMT(Stmt, "select CURRENT_DATE");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf3, 1), rsValueBuf1, sizeof(rsValueBuf1));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   current time   ==========================
    //current_time get server's runtime date, invoke server's current time three two grammar method
    //testcase just invoke function, not validate value
    //ODBC 3.0 current time
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_CURRENT_TIME, SQL_FN_TD_CURRENT_TIME);

    OK_SIMPLE_STMT(Stmt, "select {fn CURRENT_TIME()}");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf1, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
    //ODBC 1.0 current time
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_CURTIME, SQL_FN_TD_CURTIME);

    OK_SIMPLE_STMT(Stmt, "select {fn CURTIME()}");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf2, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
    //SQL-92 current time
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_SQL92_DATETIME_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_SDF_CURRENT_TIME, SQL_SDF_CURRENT_TIME);
    
    OK_SIMPLE_STMT(Stmt, "select CURRENT_TIME");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf3, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   current timestamp   ==========================
    //current_timestamp get server's runtime date, invoke server's current timestamp by two grammar method
    //ODBC 3.0 current timestamp
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_CURRENT_TIMESTAMP, SQL_FN_TD_CURRENT_TIMESTAMP);

    OK_SIMPLE_STMT(Stmt, "select {fn CURRENT_TIMESTAMP()}");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf1, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
    //SQL-92 current timestamp
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_SQL92_DATETIME_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_SDF_CURRENT_TIMESTAMP, SQL_SDF_CURRENT_TIMESTAMP);

    OK_SIMPLE_STMT(Stmt, "select CURRENT_TIMESTAMP");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf2, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   day of month   ==========================
    //ODBC 1.0  DAYOFMONTH
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_DAYOFMONTH, SQL_FN_TD_DAYOFMONTH);

    OK_SIMPLE_STMT(Stmt, "select {fn DAYOFMONTH(dt_date)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 22);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   day of week   ==========================
    //ODBC 1.0  DAYOFWEEK
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_DAYOFWEEK, SQL_FN_TD_DAYOFWEEK);

    OK_SIMPLE_STMT(Stmt, "select {fn DAYOFWEEK(dt_date)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 3);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   day of year   ==========================
    //ODBC 1.0  DAYOFYEAR
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_DAYOFYEAR, SQL_FN_TD_DAYOFYEAR);

    OK_SIMPLE_STMT(Stmt, "select {fn DAYOFYEAR(dt_date)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 234);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   extract spacil filed of    ==========================
    //ODBC 3.0  EXTRACT(extract-field FROM extract-source)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_EXTRACT, SQL_FN_TD_EXTRACT);

    OK_SIMPLE_STMT(Stmt, "select {fn EXTRACT(YEAR FROM dt_date)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 2001);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn EXTRACT(DAY FROM dt_date)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 22);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn EXTRACT(MONTH FROM dt_date)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 8);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn EXTRACT(HOUR FROM dt_time)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn EXTRACT(MINUTE FROM dt_time)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 2);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn EXTRACT(SECOND FROM dt_time)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 3);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   hour    ==========================
    //ODBC 1.0      HOUR(time_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_HOUR, SQL_FN_TD_HOUR);

    OK_SIMPLE_STMT(Stmt, "select {fn HOUR(dt_time)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   minute    ==========================
    //ODBC 1.0      MINUTE(time_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_MINUTE, SQL_FN_TD_MINUTE);

    OK_SIMPLE_STMT(Stmt, "select {fn MINUTE(dt_time)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 2);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   month    ==========================
    //ODBC 1.0      MONTH(date_exp) 
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_MONTH, SQL_FN_TD_MONTH);

    OK_SIMPLE_STMT(Stmt, "select {fn MONTH(dt_date)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 8);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    

    //==========================   now   ==========================
    //ODBC 1.0          NOW( ) 
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_NOW, SQL_FN_TD_NOW);
    
    //testcase just invoke function, not validate value
    OK_SIMPLE_STMT(Stmt, "select {fn NOW()}");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_int(Stmt, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   quarter    ==========================
    //ODBC 1.0      QUARTER(date_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_QUARTER, SQL_FN_TD_QUARTER);

    OK_SIMPLE_STMT(Stmt, "select {fn QUARTER(dt_date)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 3);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   second    ==========================
    //ODBC 1.0      SECOND(time_exp) 
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_SECOND, SQL_FN_TD_SECOND);

    OK_SIMPLE_STMT(Stmt, "select {fn SECOND(dt_time)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 3);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   timestamp diff   ==========================
    //ODBC 2.0      TIMESTAMPDIFF(interval, timestamp_exp1, timestamp_exp2) 
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_TIMESTAMPDIFF, SQL_FN_TD_TIMESTAMPDIFF);

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                         "101, null, null, null, null, null, null, null, null, null, null, null, null, null,"
                         "TIMESTAMP '2001-08-22 03:04:05.321', TIMESTAMP '2001-08-22 03:04:05.322',"
                         "null, null)");
    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPDIFF(SQL_TSI_FRAC_SECOND, dt_timestamp1, dt_timestamp2)} from test_tbl_alldatatypes where id = 101");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                         "102, null, null, null, null, null, null, null, null, null, null, null, null, null,"
                         "TIMESTAMP '2001-08-22 03:04:05.321', TIMESTAMP '2001-08-22 03:04:06.321',"
                         "null, null)");
    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPDIFF(SQL_TSI_SECOND, dt_timestamp1, dt_timestamp2)} from test_tbl_alldatatypes where id = 102");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                         "103, null, null, null, null, null, null, null, null, null, null, null, null, null,"
                         "TIMESTAMP '2001-08-22 03:04:05.321', TIMESTAMP '2001-08-22 03:05:05.321',"
                         "null, null)");
    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPDIFF(SQL_TSI_MINUTE, dt_timestamp1, dt_timestamp2)} from test_tbl_alldatatypes where id = 103");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                         "104, null, null, null, null, null, null, null, null, null, null, null, null, null,"
                         "TIMESTAMP '2001-08-22 03:04:05.321', TIMESTAMP '2001-08-22 04:04:05.321',"
                         "null, null)");
    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPDIFF(SQL_TSI_HOUR, dt_timestamp1, dt_timestamp2)} from test_tbl_alldatatypes where id = 104");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                         "105, null, null, null, null, null, null, null, null, null, null, null, null, null,"
                         "TIMESTAMP '2001-08-22 03:04:05.321', TIMESTAMP '2001-08-23 03:04:05.321',"
                         "null, null)");
    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPDIFF(SQL_TSI_DAY, dt_timestamp1, dt_timestamp2)} from test_tbl_alldatatypes where id = 105");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                         "106, null, null, null, null, null, null, null, null, null, null, null, null, null,"
                         "TIMESTAMP '2001-08-22 03:04:05.321', TIMESTAMP '2001-08-29 03:04:05.321',"
                         "null, null)");
    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPDIFF(SQL_TSI_WEEK, dt_timestamp1, dt_timestamp2)} from test_tbl_alldatatypes where id = 106");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                         "107, null, null, null, null, null, null, null, null, null, null, null, null, null,"
                         "TIMESTAMP '2001-08-22 03:04:05.321', TIMESTAMP '2001-09-22 03:04:05.321',"
                         "null, null)");
    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPDIFF(SQL_TSI_MONTH, dt_timestamp1, dt_timestamp2)} from test_tbl_alldatatypes where id = 107");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                         "108, null, null, null, null, null, null, null, null, null, null, null, null, null,"
                         "TIMESTAMP '2001-08-22 03:04:05.321', TIMESTAMP '2001-11-22 03:04:05.321',"
                         "null, null)");
    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPDIFF(SQL_TSI_QUARTER, dt_timestamp1, dt_timestamp2)} from test_tbl_alldatatypes where id = 108");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                         "109, null, null, null, null, null, null, null, null, null, null, null, null, null,"
                         "TIMESTAMP '2001-08-22 03:04:05.321', TIMESTAMP '2002-08-22 03:04:05.321',"
                         "null, null)");
    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPDIFF(SQL_TSI_YEAR, dt_timestamp1, dt_timestamp2)} from test_tbl_alldatatypes where id = 109");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   timestamp add   ==========================
    //ODBC 2.0      TIMESTAMPADD(interval, integer_exp , timestamp_exp) 
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_TIMESTAMPADD, SQL_FN_TD_TIMESTAMPADD);
    
    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPADD(SQL_TSI_FRAC_SECOND, 1, dt_timestamp1)} from test_tbl_alldatatypes where id = 101");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf1, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select dt_timestamp2 from test_tbl_alldatatypes where id = 101");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf2, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    IS_STR(rsValueBuf1, rsValueBuf2, sizeof(rsValueBuf2));

    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPADD(SQL_TSI_SECOND, 1, dt_timestamp1)} from test_tbl_alldatatypes where id = 102");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf1, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select dt_timestamp2 from test_tbl_alldatatypes where id = 102");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf2, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    IS_STR(rsValueBuf1, rsValueBuf2, sizeof(rsValueBuf2));

    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPADD(SQL_TSI_MINUTE, 1, dt_timestamp1)} from test_tbl_alldatatypes where id = 103");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf1, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select dt_timestamp2 from test_tbl_alldatatypes where id = 103");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf2, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    IS_STR(rsValueBuf1, rsValueBuf2, sizeof(rsValueBuf2));

    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPADD(SQL_TSI_HOUR, 1, dt_timestamp1)} from test_tbl_alldatatypes where id = 104");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf1, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select dt_timestamp2 from test_tbl_alldatatypes where id = 104");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf2, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    IS_STR(rsValueBuf1, rsValueBuf2, sizeof(rsValueBuf2));

    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPADD(SQL_TSI_DAY, 1, dt_timestamp1)} from test_tbl_alldatatypes where id = 105");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf1, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select dt_timestamp2 from test_tbl_alldatatypes where id = 105");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf2, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    IS_STR(rsValueBuf1, rsValueBuf2, sizeof(rsValueBuf2));

    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPADD(SQL_TSI_WEEK, 1, dt_timestamp1)} from test_tbl_alldatatypes where id = 106");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf1, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select dt_timestamp2 from test_tbl_alldatatypes where id = 106");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf2, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    IS_STR(rsValueBuf1, rsValueBuf2, sizeof(rsValueBuf2));

    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPADD(SQL_TSI_MONTH, 1, dt_timestamp1)} from test_tbl_alldatatypes where id = 107");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf1, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select dt_timestamp2 from test_tbl_alldatatypes where id = 107");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf2, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    IS_STR(rsValueBuf1, rsValueBuf2, sizeof(rsValueBuf2));

    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPADD(SQL_TSI_QUARTER, 1, dt_timestamp1)} from test_tbl_alldatatypes where id = 108");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf1, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select dt_timestamp2 from test_tbl_alldatatypes where id = 108");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf2, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    IS_STR(rsValueBuf1, rsValueBuf2, sizeof(rsValueBuf2));

    OK_SIMPLE_STMT(Stmt, "select {fn TIMESTAMPADD(SQL_TSI_YEAR, 1, dt_timestamp1)} from test_tbl_alldatatypes where id = 109");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf1, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select dt_timestamp2 from test_tbl_alldatatypes where id = 109");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    my_fetch_str(Stmt, rsValueBuf2, 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    IS_STR(rsValueBuf1, rsValueBuf2, sizeof(rsValueBuf2));

    //==========================   extract spacil filed of    ==========================
    //ODBC 1.0      WEEK(date_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_WEEK, SQL_FN_TD_WEEK);

    OK_SIMPLE_STMT(Stmt, "select {fn WEEK(dt_date)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 34);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   extract spacil filed of    ==========================
    //ODBC 1.0      YEAR(date_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_YEAR, SQL_FN_TD_YEAR);

    OK_SIMPLE_STMT(Stmt, "select {fn YEAR(dt_date)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 2001);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   not support   ==========================
    //ODBC 2.0      DAYNAME(date_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_DAYNAME, 0);

    //ODBC 2.0     MONTHNAME(date_exp)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_TD_MONTHNAME, 0);


    return OK;
}


//=========================================================
//System Functions testcase
//1) function test
//2)relate capabilities setting:
//2.1)SQL_SYSTEM_FUNCTIONS and bitmask 
//         SQL_FN_SYS_DBNAME
//         SQL_FN_SYS_IFNULL
//         SQL_FN_SYS_USERNAME
ODBC_TEST(tests_systemScalarFuncES)
{
    SQLCHAR rsValueBuf1[1000] = {'\0'};

    SQLUINTEGER unitBitmask4InfoValue;
    SQLSMALLINT length;

    //ODBC 1.0     IFNULL(exp, value)
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_SYSTEM_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_SYS_IFNULL, SQL_FN_SYS_IFNULL);

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                         "401, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)");
    OK_SIMPLE_STMT(Stmt, "select {fn IFNULL(dt_boolean, true)} from test_tbl_alldatatypes where id = 401");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    is_num(my_fetch_int(Stmt, 1), 1);
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn IFNULL(dt_varchar, 'dt_varchar is null')} from test_tbl_alldatatypes where id = 401");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "dt_varchar is null", sizeof("dt_varchar is null"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //ODBC 1.0)     USER( )
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_SYSTEM_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_SYS_USERNAME, SQL_FN_SYS_USERNAME);

    OK_SIMPLE_STMT(Stmt, "select {fn USER()}");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   not support   ==========================
    //ODBC 1.0     DATABASE()
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_SYSTEM_FUNCTIONS, &unitBitmask4InfoValue,
                        sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_SYS_DBNAME, 0);

    return OK;
}


//=========================================================
//ODBC convert Functions testcase
//1) function test
//2)relate capabilities setting:
ODBC_TEST(tests_odbcConvertScalarFuncES)
{

    SQLCHAR rsValueBuf1[1000] = {'\0'};

    SQLUINTEGER unitBitmask4InfoValue;
    SQLSMALLINT length;

    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_FUNCTIONS, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_CVT_CONVERT , SQL_FN_CVT_CONVERT);

    //ODBC 1.0)    SQL_CONVERT_DECIMAL
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_DECIMAL, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  , SQL_CVT_REAL | SQL_CVT_INTEGER | SQL_CVT_SMALLINT |
                                    SQL_CVT_DECIMAL | SQL_CVT_TINYINT | SQL_CVT_BIGINT |
                                    SQL_CVT_SMALLINT | SQL_CVT_VARCHAR);
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_decimal, SQL_REAL)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_decimal, SQL_INTEGER)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_decimal, SQL_SMALLINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_decimal, SQL_DECIMAL)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_decimal, SQL_TINYINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_decimal, SQL_BIGINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_decimal, SQL_SMALLINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_decimal, SQL_VARCHAR)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //ODBC 1.0)    SQL_CONVERT_DOUBLE
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_DOUBLE, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue , SQL_CVT_REAL | SQL_CVT_INTEGER | SQL_CVT_SMALLINT |
                                   SQL_CVT_DOUBLE | SQL_CVT_TINYINT | SQL_CVT_BIGINT |
                                   SQL_CVT_SMALLINT | SQL_CVT_VARCHAR);
    
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_double, SQL_REAL)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10.3", sizeof("10.3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_double, SQL_INTEGER)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10", sizeof("10"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_double, SQL_SMALLINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10", sizeof("10"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_double, SQL_DOUBLE)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10.3", sizeof("10.3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_double, SQL_TINYINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10", sizeof("10"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_double, SQL_BIGINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10", sizeof("10"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_double, SQL_SMALLINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10", sizeof("10"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_double, SQL_VARCHAR)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10.3", sizeof("10.3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //ODBC 1.0)    SQL_CONVERT_REAL
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_REAL, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue , SQL_CVT_REAL | SQL_CVT_INTEGER | SQL_CVT_SMALLINT |
                                   SQL_CVT_DOUBLE | SQL_CVT_TINYINT | SQL_CVT_BIGINT |
                                   SQL_CVT_SMALLINT | SQL_CVT_VARCHAR);
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_real, SQL_REAL)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9.2", sizeof("9.2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_real, SQL_INTEGER)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9", sizeof("9"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_real, SQL_SMALLINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9", sizeof("9"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_real, SQL_DOUBLE)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9.199999809265137", sizeof("9.199999809265137"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_real, SQL_TINYINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9", sizeof("9"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_real, SQL_BIGINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9", sizeof("9"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_real, SQL_SMALLINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9", sizeof("9"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_real, SQL_VARCHAR)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9.2", sizeof("9.2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //ODBC 1.0)    SQL_CONVERT_TINYINT
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_TINYINT, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue , SQL_CVT_REAL | SQL_CVT_INTEGER | SQL_CVT_SMALLINT |
                                   SQL_CVT_DOUBLE | SQL_CVT_TINYINT | SQL_CVT_BIGINT |
                                   SQL_CVT_SMALLINT | SQL_CVT_VARCHAR);
    
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_tinyint, SQL_REAL)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_tinyint, SQL_INTEGER)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_tinyint, SQL_SMALLINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_tinyint, SQL_DOUBLE)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_tinyint, SQL_TINYINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_tinyint, SQL_BIGINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_tinyint, SQL_SMALLINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_tinyint, SQL_VARCHAR)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //ODBC 1.0)    SQL_CONVERT_SMALLINT
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_SMALLINT, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue , SQL_CVT_INTEGER | SQL_CVT_SMALLINT | SQL_CVT_REAL |
                                   SQL_CVT_DOUBLE | SQL_CVT_TINYINT | SQL_CVT_BIGINT |
                                   SQL_CVT_SMALLINT | SQL_CVT_VARCHAR);

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_smallint, SQL_INTEGER)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2", sizeof("2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_smallint, SQL_SMALLINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2", sizeof("2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_smallint, SQL_REAL)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2", sizeof("2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_smallint, SQL_DOUBLE)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2", sizeof("2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_smallint, SQL_TINYINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2", sizeof("2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_smallint, SQL_BIGINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2", sizeof("2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_smallint, SQL_VARCHAR)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2", sizeof("2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //ODBC 1.0)    SQL_CONVERT_INTEGER
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_INTEGER, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue , SQL_CVT_INTEGER | SQL_CVT_SMALLINT | SQL_CVT_REAL |
                                   SQL_CVT_DOUBLE | SQL_CVT_TINYINT | SQL_CVT_BIGINT |
                                   SQL_CVT_SMALLINT | SQL_CVT_VARCHAR);

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_integer, SQL_INTEGER)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "3", sizeof("3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_integer, SQL_SMALLINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "3", sizeof("3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_integer, SQL_REAL)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "3", sizeof("3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_integer, SQL_DOUBLE)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "3", sizeof("3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_integer, SQL_TINYINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "3", sizeof("3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_integer, SQL_BIGINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "3", sizeof("3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_integer, SQL_VARCHAR)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "3", sizeof("3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //ODBC 1.0)    SQL_CONVERT_INTEGER
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_BIGINT, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue , SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_DOUBLE | SQL_CVT_REAL |
                                   SQL_CVT_BIGINT | SQL_CVT_VARCHAR | SQL_CVT_TINYINT);
    
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_bigint, SQL_SMALLINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "4", sizeof("4"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_bigint, SQL_INTEGER)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "4", sizeof("4"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_bigint, SQL_DOUBLE)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "4", sizeof("4"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_bigint, SQL_REAL)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "4", sizeof("4"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_bigint, SQL_BIGINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "4", sizeof("4"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_bigint, SQL_VARCHAR)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "4", sizeof("4"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_bigint, SQL_TINYINT)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "4", sizeof("4"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));


    //ODBC 1.0)    SQL_CONVERT_DATE
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_DATE, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue , SQL_CVT_DATE | SQL_CVT_TIMESTAMP | SQL_CVT_VARCHAR);

    //odbc 3.0
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_date, SQL_TYPE_DATE)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22", sizeof("2001-08-22"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //odbc 2.0
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_date, SQL_DATE)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22", sizeof("2001-08-22"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //odbc 3.0
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_date, SQL_TYPE_TIMESTAMP)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22 00:00:00", sizeof("2001-08-22 00:00:00"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //odbc 2.0
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_date, SQL_TIMESTAMP)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22 00:00:00", sizeof("2001-08-22 00:00:00"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_date, SQL_VARCHAR)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22", sizeof("2001-08-22"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //ODBC 1.0)    SQL_CONVERT_TIME
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_TIME, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  , SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_VARCHAR);

    //odbc 3.0
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_time, SQL_TYPE_TIME)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "01:02:03", sizeof("01:02:03"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //odbc 2.0
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_time, SQL_TIME)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "01:02:03", sizeof("01:02:03"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //odbc 3.0
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_time, SQL_TYPE_TIMESTAMP)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1970-01-01 01:02:03", sizeof("1970-01-01 01:02:03"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //odbc 2.0
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_time, SQL_TIMESTAMP)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1970-01-01 01:02:03", sizeof("1970-01-01 01:02:03"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_time, SQL_VARCHAR)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "01:02:03.456", sizeof("01:02:03.456"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));


    //ODBC 1.0)    SQL_CONVERT_TIMESTAMP
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_TIMESTAMP, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  , SQL_CVT_DATE | SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_VARCHAR);

    //odbc 3.0
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_timestamp1, SQL_TYPE_DATE)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22", sizeof("2001-08-22"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //odbc 2.0
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_timestamp1, SQL_DATE)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22", sizeof("2001-08-22"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //odbc 3.0
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_timestamp1, SQL_TYPE_TIME)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "03:04:05", sizeof("03:04:05"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //odbc 2.0
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_timestamp1, SQL_TIME)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "03:04:05", sizeof("03:04:05"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //odbc 3.0
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_timestamp1, SQL_TYPE_TIMESTAMP)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22 03:04:05", sizeof("2001-08-22 03:04:05"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //odbc 2.0
    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_timestamp1, SQL_TIMESTAMP)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22 03:04:05", sizeof("2001-08-22 03:04:05"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_timestamp1, SQL_VARCHAR)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22 03:04:05.321", sizeof("2001-08-22 03:04:05.321"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));


    //ODBC 1.0)    SQL_CONVERT_VARBINARY
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_VARBINARY, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  , SQL_CVT_VARBINARY);

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_varbinary, SQL_VARBINARY)} from test_tbl_alldatatypes where id = 1");    

    //ODBC 1.0)    SQL_CONVERT_VARCHAR
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_VARCHAR, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  , SQL_CVT_VARCHAR | SQL_CVT_VARBINARY | SQL_CVT_CHAR | SQL_CVT_SMALLINT | 
                                    SQL_CVT_INTEGER | SQL_CVT_DOUBLE | SQL_CVT_REAL |SQL_CVT_BIGINT | SQL_CVT_TINYINT);

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                         "502, null, null, null, null, null, null, null, null,"
                         "'b',"
                         "null, null, null, null, null, null, null, null)");

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_varchar, SQL_CHAR)} from test_tbl_alldatatypes where id = 502");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "b", sizeof("b"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_varchar, SQL_VARCHAR)} from test_tbl_alldatatypes where id = 502");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "b", sizeof("b"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_varchar, SQL_VARBINARY)} from test_tbl_alldatatypes where id = 502");


    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                             "503, null, null, null, null, null, null, null, null,"
                             "'12',"
                             "null, null, null, null, null, null, null, null)");

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_varchar, SQL_INTEGER)} from test_tbl_alldatatypes where id = 503");
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
     IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "12", sizeof("12"));
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
     OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_varchar, SQL_SMALLINT)} from test_tbl_alldatatypes where id = 503");
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
     IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "12", sizeof("12"));
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
     OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_varchar, SQL_REAL)} from test_tbl_alldatatypes where id = 503");
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
     IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "12", sizeof("12"));
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
     OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_varchar, SQL_DOUBLE)} from test_tbl_alldatatypes where id = 503");
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
     IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "12", sizeof("12"));
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
     OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_varchar, SQL_TINYINT)} from test_tbl_alldatatypes where id = 503");
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
     IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "12", sizeof("12"));
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
     OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_varchar, SQL_BIGINT)} from test_tbl_alldatatypes where id = 503");
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
     IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "12", sizeof("12"));
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //ODBC 1.0)    SQL_CONVERT_CHAR
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_CHAR, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  ,  SQL_CVT_CHAR | SQL_CVT_VARCHAR );

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_char, SQL_CHAR)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "a", sizeof("a"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select {fn CONVERT(dt_char, SQL_VARCHAR)} from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "a", sizeof("a"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    //==========================   not support   ==========================
        //ODBC 1.0)    SQL_CONVERT_FLOAT
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_FLOAT, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue , 0);
    
        //ODBC 1.0)    SQL_CONVERT_BINARY
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_BINARY, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  , 0);
    
    //ODBC 1.0)    SQL_CONVERT_INTERVAL_YEAR_MONTH
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_INTERVAL_YEAR_MONTH, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  , 0);

    //ODBC 1.0)    SQL_CONVERT_INTERVAL_DAY_TIME
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_INTERVAL_DAY_TIME, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  , 0);

    //ODBC 1.0)    SQL_CONVERT_BIT
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_BIT, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  , 0);

    //ODBC 1.0)    SQL_CONVERT_LONGVARBINARY
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_LONGVARBINARY, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  , 0);

    //ODBC 1.0)    SQL_CONVERT_LONGVARCHAR
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_LONGVARCHAR, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  , 0);

    //ODBC 1.0)    SQL_CONVERT_NUMERIC
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_NUMERIC, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  , 0);

    //ODBC 1.0)    SQL_CONVERT_WCHAR
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_WCHAR, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  , 0);

    //ODBC 1.0)    SQL_CONVERT_WLONGVARCHAR
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_WLONGVARCHAR, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  , 0);

    //ODBC 1.0)    SQL_CONVERT_WVARCHAR
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_WVARCHAR, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue  , 0);

    return OK;
}


//=========================================================
//SQL92 Cast Functions testcase
//1) function test
//2)relate capabilities setting:
//2.1) and bitmask 
ODBC_TEST(tests_92CastScalarFuncES)
{
    SQLCHAR rsValueBuf1[1000] = {'\0'};

    SQLUINTEGER unitBitmask4InfoValue;
    SQLSMALLINT length;

    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_FUNCTIONS, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue & SQL_FN_CVT_CAST , SQL_FN_CVT_CAST);

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_decimal AS real) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_decimal AS integer) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_decimal AS smallint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_decimal AS decimal) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_decimal AS tinyint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_decimal AS bigint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_decimal AS smallint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_decimal AS varchar) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "11", sizeof("11"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
    OK_SIMPLE_STMT(Stmt, "select CAST(dt_double AS real) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10.3", sizeof("10.3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_double AS integer) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10", sizeof("10"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_double AS smallint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10", sizeof("10"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_double AS double) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10.3", sizeof("10.3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_double AS tinyint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10", sizeof("10"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_double AS bigint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10", sizeof("10"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_double AS smallint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10", sizeof("10"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_double AS varchar) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "10.3", sizeof("10.3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_real AS real) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9.2", sizeof("9.2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_real AS integer) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9", sizeof("9"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_real AS smallint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9", sizeof("9"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_real AS double) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9.199999809265137", sizeof("9.199999809265137"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_real AS tinyint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9", sizeof("9"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_real AS bigint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9", sizeof("9"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_real AS smallint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9", sizeof("9"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_real AS varchar) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "9.2", sizeof("9.2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    
    OK_SIMPLE_STMT(Stmt, "select CAST(dt_tinyint AS real) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_tinyint AS integer) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_tinyint AS smallint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_tinyint AS double) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_tinyint AS tinyint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_tinyint AS bigint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_tinyint AS smallint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_tinyint AS varchar) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1", sizeof("1"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_smallint AS integer) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2", sizeof("2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_smallint AS smallint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2", sizeof("2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_smallint AS real) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2", sizeof("2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_smallint AS double) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2", sizeof("2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_smallint AS tinyint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2", sizeof("2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_smallint AS bigint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2", sizeof("2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_smallint AS varchar) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2", sizeof("2"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_integer AS integer) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "3", sizeof("3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_integer AS smallint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "3", sizeof("3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_integer AS real) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "3", sizeof("3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_integer AS double) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "3", sizeof("3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_integer AS tinyint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "3", sizeof("3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_integer AS bigint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "3", sizeof("3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_integer AS varchar) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "3", sizeof("3"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
    OK_SIMPLE_STMT(Stmt, "select CAST(dt_bigint AS smallint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "4", sizeof("4"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_bigint AS integer) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "4", sizeof("4"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_bigint AS double) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "4", sizeof("4"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
    OK_SIMPLE_STMT(Stmt, "select CAST(dt_bigint AS real) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "4", sizeof("4"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_bigint AS bigint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "4", sizeof("4"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_bigint AS varchar) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "4", sizeof("4"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_bigint AS tinyint) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "4", sizeof("4"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_date AS date) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22", sizeof("2001-08-22"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_date AS timestamp) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22 00:00:00", sizeof("2001-08-22 00:00:00"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_date AS varchar) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22", sizeof("2001-08-22"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_time AS time) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "01:02:03", sizeof("01:02:03"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_time AS timestamp) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "1970-01-01 01:02:03", sizeof("1970-01-01 01:02:03"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_time AS varchar) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "01:02:03.456", sizeof("01:02:03.456"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_timestamp1 AS date) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22", sizeof("2001-08-22"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_timestamp1 AS time) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "03:04:05", sizeof("03:04:05"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_timestamp1 AS timestamp) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22 03:04:05", sizeof("2001-08-22 03:04:05"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_timestamp1 AS varchar) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "2001-08-22 03:04:05.321", sizeof("2001-08-22 03:04:05.321"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_varbinary AS varbinary) from test_tbl_alldatatypes where id = 1");    

    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                         "602, null, null, null, null, null, null, null, null,"
                         "'b',"
                         "null, null, null, null, null, null, null, null)");

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_varchar AS char) from test_tbl_alldatatypes where id = 602");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "b", sizeof("b"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_varchar AS varchar) from test_tbl_alldatatypes where id = 602");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "b", sizeof("b"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_varchar AS varbinary) from test_tbl_alldatatypes where id = 602");


    OK_SIMPLE_STMT(Stmt, "insert into test_tbl_alldatatypes values("
                             "603, null, null, null, null, null, null, null, null,"
                             "'12',"
                             "null, null, null, null, null, null, null, null)");

     OK_SIMPLE_STMT(Stmt, "select CAST(dt_varchar AS integer) from test_tbl_alldatatypes where id = 603");
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
     IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "12", sizeof("12"));
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
     OK_SIMPLE_STMT(Stmt, "select CAST(dt_varchar AS smallint) from test_tbl_alldatatypes where id = 603");
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
     IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "12", sizeof("12"));
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
     OK_SIMPLE_STMT(Stmt, "select CAST(dt_varchar AS real) from test_tbl_alldatatypes where id = 603");
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
     IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "12", sizeof("12"));
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
     OK_SIMPLE_STMT(Stmt, "select CAST(dt_varchar AS double) from test_tbl_alldatatypes where id = 603");
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
     IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "12", sizeof("12"));
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
     OK_SIMPLE_STMT(Stmt, "select CAST(dt_varchar AS tinyint) from test_tbl_alldatatypes where id = 603");
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
     IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "12", sizeof("12"));
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
    
     OK_SIMPLE_STMT(Stmt, "select CAST(dt_varchar AS bigint) from test_tbl_alldatatypes where id = 603");
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
     IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "12", sizeof("12"));
     CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_char AS char) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "a", sizeof("a"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    OK_SIMPLE_STMT(Stmt, "select CAST(dt_char AS varchar) from test_tbl_alldatatypes where id = 1");
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFetch(Stmt));
    IS_STR(my_fetch_str(Stmt, rsValueBuf1, 1), "a", sizeof("a"));
    CHECK_HANDLE_RC(SQL_HANDLE_STMT, Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));


    return OK;
}


//*****************************************************************
// Test case for SQL Grammar
// 1)Escape Sequences
// 2)Scalar Function
//*****************************************************************

MA_ODBC_TESTS my_tests[]=
{
    {tests_setup,                    "setup_dataset"},
    {tests_dateEscapeSequences,      "tests_dateEscapeSequences"},
    {tests_likeEscapeSequences,      "tests_likeEscapeSequences"},
    {tests_stringScalarFuncES,       "tests_stringScalarFuncES"},
    {tests_numericScalarFuncES,      "tests_numericScalarFuncES"},
    {tests_dateAndTimeScalarFuncES,  "tests_dateAndTimeScalarFuncES"},
    {tests_systemScalarFuncES,       "tests_systemScalarFuncES"},
    {tests_odbcConvertScalarFuncES,  "tests_odbcConvertScalarFuncES"},
    {tests_92CastScalarFuncES,       "tests_92CastScalarFuncES"},
    {tests_cleanup,                  "clean_up_dataset"},
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

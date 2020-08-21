/*
  Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.

  Copyright (c) 2001, 2012, Oracle and/or its affiliates. All rights reserved.
                2013, 2019 MariaDB Corporation AB

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

#include <sqlucode.h>

#define BUFF_LEN 255

void preparedata()
{
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_kanji (c1 varchar(100), c2 varchar(100))");
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_ascii (c1 varchar(100), c2 varchar(100))");
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_emoji (c1 varchar(100), c2 varchar(100))");
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_latin1 (c1 varchar(100), c2 varchar(100))");
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_CentralEuro (c1 varchar(100), c2 varchar(100))");
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_latin5 (c1 varchar(100), c2 varchar(100))");
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_greek (c1 varchar(100), c2 varchar(100))");
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_hebrew (c1 varchar(100), c2 varchar(100))");
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_tis620 (c1 varchar(100), c2 varchar(100))");
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_russian (c1 varchar(100), c2 varchar(100))");
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_koi8u (c1 varchar(100), c2 varchar(100))");
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_arabic (c1 varchar(100), c2 varchar(100))");

    OK_SIMPLE_STMT(Stmt, "insert into test_kanji (c1, c2) values ('中文', '中文')");
    OK_SIMPLE_STMT(Stmt, "insert into test_ascii (c1, c2) values ('L', 'L')");
}

void cleanup()
{
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_kanji");
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_ascii");
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_emoji");
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_latin1");
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_CentralEuro");
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_latin5");
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_greek");
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_hebrew");
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_tis620");
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_russian");
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_koi8u");
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_arabic");
}

ODBC_TEST(test_ConnectA)
{
    SQLHANDLE henv1;
    SQLHANDLE hdbc1;
    SQLHANDLE hstmt1;

    SQLLEN    colnum = 0;
    SQLLEN    nlen[2] = {0,0};

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);

    preparedata();

    ODBC_Connect(&henv1, &hdbc1, &hstmt1);

    OK_SIMPLE_STMT(hstmt1, "select * from test_kanji");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr[2] = { &c1_buff, &c2_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)))
    {
        for (int i = 0; i < colnum; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr[i],
                BUFF_LEN, &nlen[i]));
        }
        diag(my_dsn);
        printf("\n%s\n", c1_buff);
        printf("\n%lld\n", nlen[0]);
        IS_STR(c1_buff, "中文", nlen[0]);
        printHex(c2_buff, nlen[1]);
        printf("\n%lld\n", nlen[1]);
        IS_WSTR(c2_buff, W(L"\x4e2d\x6587"), 1+nlen[1]/sizeof(SQLWCHAR));
        
    }

    ODBC_Disconnect(henv1, hdbc1, hstmt1);

    return OK;
}


ODBC_TEST(test_ConnectW)
{
    HDBC hdbc1;
    HSTMT hstmt1;
    
    SQLLEN    colnum = 0;
    SQLLEN    nlen[2] = {0,0};

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);

    CHECK_ENV_RC(Env, SQLAllocConnect(Env, &hdbc1));
    CHECK_DBC_RC(hdbc1, SQLConnectW(hdbc1,
        wdsn, SQL_NTS,
        wuid, SQL_NTS,
        wpwd, SQL_NTS)); 

    CHECK_DBC_RC(Connection, SQLAllocStmt(hdbc1, &hstmt1));

    OK_SIMPLE_STMTW(hstmt1, L"select * from test_kanji");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr[2] = { &c1_buff, &c2_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)))
    {
        for (int i = 0; i < colnum; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr[i],
                BUFF_LEN, &nlen[i]));
        }
        diag(my_dsn);
        wprintf(wdsn);
        printf("\n%s\n", c1_buff);
        printf("\n%lld\n", nlen[0]);
        IS_STR(c1_buff, "\xe4\xb8\xad\xe6\x96\x87", nlen[0]);
        printHex(c2_buff, nlen[1]);
        printf("\n%lld\n", nlen[1]);
        IS_WSTR(c2_buff, W(L"\x4e2d\x6587"), 1 + nlen[1] / sizeof(SQLWCHAR));

    }

    //Using this unicode connection to prepare other data
    OK_SIMPLE_STMTW(hstmt1, L"insert into test_emoji (c1, c2) values ('\xd83d\xde02', '\xd83d\xde02')");
    OK_SIMPLE_STMTW(hstmt1, L"insert into test_latin1 (c1, c2) values ('\xe6', '\xe6')");
    OK_SIMPLE_STMTW(hstmt1, L"insert into test_CentralEuro (c1, c2) values ('\x154', '\x154')");
    OK_SIMPLE_STMTW(hstmt1, L"insert into test_latin5 (c1, c2) values ('\x15f', '\x15f')");
    OK_SIMPLE_STMTW(hstmt1, L"insert into test_greek (c1, c2) values ('\x3b4', '\x3b4')");
    OK_SIMPLE_STMTW(hstmt1, L"insert into test_hebrew (c1, c2) values ('\x5d4', '\x5d4')");
    OK_SIMPLE_STMTW(hstmt1, L"insert into test_tis620 (c1, c2) values ('\xe12', '\xe12')");
    OK_SIMPLE_STMTW(hstmt1, L"insert into test_russian (c1, c2) values ('\x434', '\x434')");
    OK_SIMPLE_STMTW(hstmt1, L"insert into test_koi8u (c1, c2) values ('\x407', '\x407')");
    OK_SIMPLE_STMTW(hstmt1, L"insert into test_arabic (c1, c2) values ('\x643', '\x643')");

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    return OK;
}

ODBC_TEST(test_connectAscii)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "ascii", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "ascii", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_ascii");
    OK_SIMPLE_STMT(hstmt2, "select * from test_ascii");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\x4c", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x004c"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printHex(c3_buff, nlen2[0]);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\x4c", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x004c"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

ODBC_TEST(test_connectUTF8)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "utf8", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "utf8", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_kanji");
    OK_SIMPLE_STMT(hstmt2, "select * from test_kanji");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%s\n", c1_buff);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xe4\xb8\xad\xe6\x96\x87", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x4e2d\x6587"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printf("\n%s\n", c3_buff);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xe4\xb8\xad\xe6\x96\x87", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x4e2d\x6587"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

/*Emoji are beyond BMP, needs 4bytes for UTF8 - Test the character "Face With Tears of Joy"*/
ODBC_TEST(test_connectUTF8MB4)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "utf8", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "utf8", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_emoji");
    OK_SIMPLE_STMT(hstmt2, "select * from test_emoji");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%s\n", c1_buff);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xf0\x9f\x98\x82", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\xd83d\xde02"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printf("\n%s\n", c3_buff);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xf0\x9f\x98\x82", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\xd83d\xde02"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

ODBC_TEST(test_connectGBK)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = {0, 0};
    SQLLEN    nlen2[2] = {0, 0};

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);

    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "gbk", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "gbk", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_kanji");
    OK_SIMPLE_STMT(hstmt2, "select * from test_kanji");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1))&& SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%s\n", c1_buff);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "中文", nlen1[0]);
        printHex(c1_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x4e2d\x6587"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printf("\n%s\n", c3_buff);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "中文", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x4e2d\x6587"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

ODBC_TEST(test_connectGB2312)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);

    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "gb2312", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "gb2312", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_kanji");
    OK_SIMPLE_STMT(hstmt2, "select * from test_kanji");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%s\n", c1_buff);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xd6\xd0\xce\xc4", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x4e2d\x6587"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printf("\n%s\n", c3_buff);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xd6\xd0\xce\xc4", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x4e2d\x6587"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

ODBC_TEST(test_connectBIG5)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "big5", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "big5", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_kanji");
    OK_SIMPLE_STMT(hstmt2, "select * from test_kanji");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%s\n", c1_buff);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xa4\xa4\xa4\xe5", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x4e2d\x6587"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printf("\n%s\n", c3_buff);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xa4\xa4\xa4\xe5", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x4e2d\x6587"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

ODBC_TEST(test_connectSJIS)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "sjis", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "sjis", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_kanji");
    OK_SIMPLE_STMT(hstmt2, "select * from test_kanji");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%s\n", c1_buff);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\x92\x86\x95\xb6", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x4e2d\x6587"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printf("\n%s\n", c3_buff);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\x92\x86\x95\xb6", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x4e2d\x6587"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

ODBC_TEST(test_connectCP932)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "cp932", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "cp932", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_kanji");
    OK_SIMPLE_STMT(hstmt2, "select * from test_kanji");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%s\n", c1_buff);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\x92\x86\x95\xb6", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x4e2d\x6587"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printf("\n%s\n", c3_buff);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\x92\x86\x95\xb6", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x4e2d\x6587"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));


    return OK;
}

ODBC_TEST(test_connectLatin1)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "latin1", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "latin1", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_latin1");
    OK_SIMPLE_STMT(hstmt2, "select * from test_latin1");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xe6", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x00e6"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printHex(c3_buff, nlen2[0]);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xe6", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x00e6"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

/*CP850 - DOS Latin1*/
ODBC_TEST(test_connectCP850)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "cp850", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "cp850", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_latin1");
    OK_SIMPLE_STMT(hstmt2, "select * from test_latin1");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\x91", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x00e6"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printHex(c3_buff, nlen2[0]);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\x91", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x00e6"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

/*CP1257 - Windows Baltic*/
ODBC_TEST(test_connectCP1257)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "cp1257", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "cp1257", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_latin1");
    OK_SIMPLE_STMT(hstmt2, "select * from test_latin1");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xbf", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x00e6"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printHex(c3_buff, nlen2[0]);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xbf", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x00e6"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

/*CP852 - DOS Latin2 Central European*/
ODBC_TEST(test_connectCP852)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "cp852", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "cp852", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_CentralEuro");
    OK_SIMPLE_STMT(hstmt2, "select * from test_CentralEuro");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xe8", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x0154"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printHex(c3_buff, nlen2[0]);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xe8", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x0154"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

/*CP1250 - Windows Central and Eastern European*/
ODBC_TEST(test_connectCP1250)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "cp1250", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "cp1250", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_CentralEuro");
    OK_SIMPLE_STMT(hstmt2, "select * from test_CentralEuro");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xc0", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x0154"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printHex(c3_buff, nlen2[0]);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xc0", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x0154"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

/*Latin5 - Turkish*/
ODBC_TEST(test_connectLatin5)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "latin5", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "latin5", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_latin5");
    OK_SIMPLE_STMT(hstmt2, "select * from test_latin5");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xfe", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x015f"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printHex(c3_buff, nlen2[0]);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xfe", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x015f"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

ODBC_TEST(test_connectGreek)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "greek", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "greek", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_greek");
    OK_SIMPLE_STMT(hstmt2, "select * from test_greek");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xe4", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x03b4"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printHex(c3_buff, nlen2[0]);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xe4", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x03b4"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

ODBC_TEST(test_connectHebrew)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "hebrew", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "hebrew", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_hebrew");
    OK_SIMPLE_STMT(hstmt2, "select * from test_hebrew");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xe4", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x05d4"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printHex(c3_buff, nlen2[0]);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xe4", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x05d4"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

ODBC_TEST(test_connectTis620)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "tis620", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "tis620", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_tis620");
    OK_SIMPLE_STMT(hstmt2, "select * from test_tis620");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xb2", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x0e12"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printHex(c3_buff, nlen2[0]);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xb2", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x0e12"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

ODBC_TEST(test_connectKoi8r)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "koi8r", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "koi8r", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_russian");
    OK_SIMPLE_STMT(hstmt2, "select * from test_russian");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xc4", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x0434"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printHex(c3_buff, nlen2[0]);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xc4", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x0434"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

/*CP866 - DOS Cyrillic Russian*/
ODBC_TEST(test_connectCP866)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "cp866", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "cp866", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_russian");
    OK_SIMPLE_STMT(hstmt2, "select * from test_russian");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xa4", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x0434"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printHex(c3_buff, nlen2[0]);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xa4", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x0434"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

/*CP1251 - Windows Ruassian*/
ODBC_TEST(test_connectCP1251)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "cp1251", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "cp1251", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_russian");
    OK_SIMPLE_STMT(hstmt2, "select * from test_russian");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xe4", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x0434"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printHex(c3_buff, nlen2[0]);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xe4", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x0434"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

ODBC_TEST(test_connectKoi8u)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "koi8u", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "koi8u", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_koi8u");
    OK_SIMPLE_STMT(hstmt2, "select * from test_koi8u");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xb7", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x0407"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printHex(c3_buff, nlen2[0]);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xb7", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x0407"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    return OK;
}

ODBC_TEST(test_connectCP1256)
{
    HDBC hdbc1;
    HDBC hdbc2;
    HSTMT hstmt1;
    HSTMT hstmt2;

    SQLLEN    colnum1 = 0;
    SQLLEN    colnum2 = 0;
    SQLLEN    nlen1[2] = { 0, 0 };
    SQLLEN    nlen2[2] = { 0, 0 };

    SQLCHAR c1_buff[BUFF_LEN];
    SQLWCHAR c2_buff[BUFF_LEN];
    SQLCHAR c3_buff[BUFF_LEN];
    SQLWCHAR c4_buff[BUFF_LEN];
    memset(c1_buff, 0, BUFF_LEN);
    memset(c2_buff, 0, BUFF_LEN);


    AllocEnvConn(&Env, &hdbc1);
    AllocEnvConn(&Env, &hdbc2);

    hstmt1 = ConnectWithCharsetA(&hdbc1, "cp1256", NULL);
    hstmt2 = ConnectWithCharsetW(&hdbc2, "cp1256", NULL);

    OK_SIMPLE_STMT(hstmt1, "select * from test_arabic");
    OK_SIMPLE_STMT(hstmt2, "select * from test_arabic");

    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum1));
    CHECK_STMT_RC(hstmt2, SQLNumResultCols(hstmt1, &colnum2));

    SQLSMALLINT TargetType[2] = { SQL_C_CHAR, SQL_C_WCHAR };

    SQLPOINTER valptr1[2] = { &c1_buff, &c2_buff };
    SQLPOINTER valptr2[2] = { &c3_buff, &c4_buff };

    while (SQL_SUCCEEDED(SQLFetch(hstmt1)) && SQL_SUCCEEDED(SQLFetch(hstmt2)))
    {
        for (int i = 0; i < colnum1 && i < colnum2; i++) {
            CHECK_STMT_RC(hstmt1, SQLGetData(hstmt1, i + 1, TargetType[i], valptr1[i],
                BUFF_LEN, &nlen1[i]));
            CHECK_STMT_RC(hstmt2, SQLGetData(hstmt2, i + 1, TargetType[i], valptr2[i],
                BUFF_LEN, &nlen2[i]));
        }
        diag(my_dsn);
        printf("\n%lld\n", nlen1[0]);
        IS_STR(c1_buff, "\xdf", nlen1[0]);
        printHex(c2_buff, nlen1[1]);
        printf("\n%lld\n", nlen1[1]);
        IS_WSTR(c2_buff, W(L"\x0643"), 1 + nlen1[1] / sizeof(SQLWCHAR));

        printHex(c3_buff, nlen2[0]);
        printf("\n%lld\n", nlen2[0]);
        IS_STR(c3_buff, "\xdf", nlen2[0]);
        printHex(c4_buff, nlen2[1]);
        printf("\n%lld\n", nlen2[1]);
        IS_WSTR(c4_buff, W(L"\x0643"), 1 + nlen2[1] / sizeof(SQLWCHAR));

    }


    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc1));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc1));

    CHECK_DBC_RC(hdbc1, SQLDisconnect(hdbc2));
    CHECK_DBC_RC(hdbc1, SQLFreeConnect(hdbc2));

    cleanup();

    return OK;
}

MA_ODBC_TESTS my_tests[]=
{
  {test_ConnectA,               "test_ConnectA",                NORMAL},
  {test_ConnectW,               "test_ConnectW",                NORMAL},
  {test_connectUTF8,            "test_connectUTF8",             NORMAL},
  {test_connectAscii,           "test_connectAscii",            NORMAL},
  {test_connectUTF8MB4,         "test_connectUTF8MB4",          NORMAL},
  {test_connectGBK,             "test_connectGBK",              NORMAL},
  {test_connectGB2312,          "test_connectGB2312",           NORMAL},
  {test_connectBIG5,            "test_connectBIG5",             NORMAL},
  {test_connectSJIS,            "test_connectSJIS",             NORMAL},
  {test_connectCP932,           "test_connectCP932",            NORMAL},
  {test_connectLatin1,          "test_connectLatin1",           NORMAL},
  {test_connectCP1257,          "test_connectCP1257",           NORMAL},
  {test_connectCP850,           "test_connectCP850",            NORMAL},
  {test_connectCP852,           "test_connectCP852",            NORMAL},
  {test_connectCP1250,          "test_connectCP1250",           NORMAL},
  {test_connectLatin5,          "test_connectLatin5",           NORMAL},
  {test_connectGreek,           "test_connectGreek",            NORMAL},
  {test_connectHebrew,          "test_connectHebrew",           NORMAL},
  {test_connectTis620,          "test_connectTis620",           NORMAL},
  {test_connectKoi8r,           "test_connectKoi8r",            NORMAL},
  {test_connectCP866,           "test_connectCP866",            NORMAL},
  {test_connectCP1251,          "test_connectCP1251",           NORMAL},
  {test_connectKoi8u,           "test_connectKoi8u",            NORMAL},
  {test_connectCP1256,          "test_connectCP1256",           NORMAL},
  {NULL, NULL}
};


int main(int argc, char **argv)
{
  int tests= sizeof(my_tests)/sizeof(MA_ODBC_TESTS) - 1;
  get_options(argc, argv);
  plan(tests);
  return run_tests_ex(my_tests, TRUE);
}

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


ODBC_TEST(use_connstring_overridedsn)
{
    SQLHANDLE hdbc1;
    SQLHANDLE hdbc2;
    SQLHANDLE hstmt1;
    SQLCHAR connstr1[1024], connstr2[1024], conn_out[1024];
    SQLSMALLINT conn_out_len;
    char catbuff[128];
    int colnum = 0;

    SQLCHAR *catalog = (SQLCHAR *)"system";
    SQLCHAR *schema = (SQLCHAR *)"metadata";

    IS(AllocEnvConn(&Env, &hdbc1));
    IS(AllocEnvConn(&Env, &hdbc2));

    _snprintf((char *)connstr1, sizeof(connstr1), "DSN=%s", my_dsn);
    _snprintf((char *)connstr2, sizeof(connstr2), "DSN=%s;DATABASE=%s;SCHEMA=%s",
        my_dsn, catalog, schema);

    diag("Connect String: %s", connstr1);
    diag("Connect String: %s", connstr2);
    CHECK_DBC_RC(hdbc1, SQLDriverConnect(hdbc1, NULL, connstr1, sizeof(connstr1), conn_out,
        sizeof(conn_out), &conn_out_len,
        SQL_DRIVER_NOPROMPT));

    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_CURRENT_CATALOG,
        catbuff, sizeof(catbuff), NULL));
    diag("input DSN:%s, default catalog:%s", my_dsn, catbuff);

    CHECK_DBC_RC(hdbc2, SQLDriverConnect(hdbc2, NULL, connstr2, sizeof(connstr2), conn_out,
        sizeof(conn_out), &conn_out_len,
        SQL_DRIVER_NOPROMPT));

    CHECK_DBC_RC(hdbc2, SQLGetConnectAttr(hdbc2, SQL_ATTR_CURRENT_CATALOG,
        catbuff, sizeof(catbuff), NULL));
    IS_STR(catbuff, "system", sizeof(catbuff));

    CHECK_DBC_RC(hdbc2, SQLAllocHandle(SQL_HANDLE_STMT, hdbc2, &hstmt1));

    OK_SIMPLE_STMT(hstmt1, "SELECT * from schema_properties");
    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum));
    is_num(colnum, 5);

    SQLFreeHandle(SQL_HANDLE_DBC, hdbc1);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc2);
    return OK;
}

ODBC_TEST(use_connstring_nodsn)
{
    SQLHANDLE hdbc1;
    SQLHANDLE hstmt1;
    SQLCHAR url[30], connstr[1024], conn_out[1024];
    SQLSMALLINT conn_out_len;
    char catbuff[128];
    int colnum = 0;

    SQLCHAR *gatewayaddr = (SQLCHAR *)"localhost";
    SQLCHAR *gatewayport = (SQLCHAR *)"8066";
    SQLCHAR *schema = (SQLCHAR *)"information_schema";
   
    _snprintf((char *)url, sizeof(url), "%s:%u",
        my_servername, my_port);

    diag("CONNECTURL: %s", url);


    IS(AllocEnvConn(&Env, &hdbc1));

    /* Using options -S -P to receive the ipaddr and port for concating the connect url, and -s for catalog*/
    _snprintf((char *)connstr, sizeof(connstr), "DRIVER={%s};TCPIP=1;\
        UID=%s;SERVER=%s;PORT=%s;DATABASE=%s;SCHEMA=%s;CONNECTURL=%s",
        my_drivername, my_uid, gatewayaddr, gatewayport, my_schema, schema, url);


    diag("Connect String: %s", connstr);
    CHECK_DBC_RC(hdbc1, SQLDriverConnect(hdbc1, NULL, connstr, sizeof(connstr), conn_out,
        sizeof(conn_out), &conn_out_len,
        SQL_DRIVER_NOPROMPT));

    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_CURRENT_CATALOG,
        catbuff, sizeof(catbuff), NULL));
    IS_STR(catbuff, my_schema, sizeof(catbuff));

    CHECK_DBC_RC(hdbc1, SQLAllocHandle(SQL_HANDLE_STMT, hdbc1, &hstmt1));

    OK_SIMPLE_STMT(hstmt1, "SELECT * from tables");
    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum));
    is_num(colnum, 4);

    SQLFreeHandle(SQL_HANDLE_DBC, hdbc1);
    return OK;
}

MA_ODBC_TESTS my_tests[]=
{

    { use_connstring_overridedsn , "use_connstring_overridedsn" },
    { use_connstring_nodsn , "use_connstring_nodsn" },
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

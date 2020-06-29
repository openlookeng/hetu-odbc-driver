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


ODBC_TEST(getandset_attr_current_catalog)
{
    SQLHANDLE henv1;
    SQLHANDLE hdbc1;
    SQLHANDLE hstmt1;

    ODBC_Connect(&henv1, &hdbc1, &hstmt1);
    SQLUINTEGER infoval = 0;
    char catseparator[2] = "";
    char catbuff[128];
    int colnum = 0;

    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_CATALOG_LOCATION, (SQLPOINTER)&infoval, 0, NULL));
    is_num(infoval, SQL_CL_START);
    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_CATALOG_NAME_SEPARATOR, (SQLPOINTER)&catseparator, sizeof(catseparator), NULL));
    IS_STR(catseparator, ".", sizeof(catseparator));

    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_CURRENT_CATALOG,
        catbuff, sizeof(catbuff), NULL));
    printf("\nDSNcatalog:%s\n", catbuff);

    CHECK_DBC_RC(hdbc1, SQLSetConnectAttr(hdbc1, SQL_ATTR_CURRENT_CATALOG,
        "system", 7));

    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_CURRENT_CATALOG,
        catbuff, sizeof(catbuff), NULL));
    IS_STR(catbuff, "system", sizeof(catbuff));

    OK_SIMPLE_STMT(hstmt1, "SELECT * from metadata.schema_properties");
    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum));
    is_num(colnum, 5);

    ODBC_Disconnect(henv1, hdbc1, hstmt1);
    return OK;
}


ODBC_TEST(use_catalog_schema)
{
    SQLHANDLE henv1;
    SQLHANDLE hdbc1;
    SQLHANDLE hstmt1;

    ODBC_Connect(&henv1, &hdbc1, &hstmt1);
    SQLUINTEGER infoval = 0;
    char catseparator[2] = "";
    char catbuff[128];
    int colnum = 0;

    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_CATALOG_LOCATION, (SQLPOINTER)&infoval, 0, NULL));
    is_num(infoval, SQL_CL_START);
    CHECK_DBC_RC(hdbc1, SQLGetInfo(hdbc1, SQL_CATALOG_NAME_SEPARATOR, (SQLPOINTER)&catseparator, sizeof(catseparator), NULL));
    IS_STR(catseparator, ".", sizeof(catseparator));

    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_CURRENT_CATALOG,
        catbuff, sizeof(catbuff), NULL));
    printf("\nDSNcatalog:%s\n", catbuff);

    //Use Stmt changing catalog, should specified to schema level 
    OK_SIMPLE_STMT(hstmt1, "USE system.metadata");

    CHECK_DBC_RC(hdbc1, SQLGetConnectAttr(hdbc1, SQL_ATTR_CURRENT_CATALOG,
        catbuff, sizeof(catbuff), NULL));
    IS_STR(catbuff, "system", sizeof(catbuff));

    OK_SIMPLE_STMT(hstmt1, "SELECT * from schema_properties");
    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum));
    is_num(colnum, 5);

    //Use stmt changing schema
    OK_SIMPLE_STMT(hstmt1, "USE jdbc");
    OK_SIMPLE_STMT(hstmt1, "SELECT * from super_types");
    CHECK_STMT_RC(hstmt1, SQLNumResultCols(hstmt1, &colnum));
    is_num(colnum, 6);

    ODBC_Disconnect(henv1, hdbc1, hstmt1);
    return OK;
}

MA_ODBC_TESTS my_tests[]=
{
    { getandset_attr_current_catalog , "getandset_attr_current_catalog" },
    { use_catalog_schema , "use_catalog_schema" },
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

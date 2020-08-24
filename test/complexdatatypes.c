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

typedef struct
{
    const char* datatype;
    const char* create_params;
    SQLSMALLINT case_sensitive;
    SQLSMALLINT searchable;
} typeinfos;

typeinfos infoassert[] = {
       {"array", "max length", 0, SQL_UNSEARCHABLE},
       {"interval day to second", "(Null)", 0, SQL_ALL_EXCEPT_LIKE},
       {"interval year to month", "(Null)", 0, SQL_ALL_EXCEPT_LIKE},
       {"json", "max length", 0, SQL_UNSEARCHABLE},
       {"map", "max length", 0, SQL_UNSEARCHABLE},
       {"row", "max length", 0, SQL_UNSEARCHABLE},
       {"varchar", "max length", 0, SQL_SEARCHABLE}
};

ODBC_TEST(test_preparedata)
{    
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_complex_array "
        "as select ARRAY[1, 2, NULL] as col");
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_complex_map "
        "as select MAP(ARRAY['foo', 'bar'], ARRAY[1, NULL]) as col"); 
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_complex_row "
        "as select cast(ROW(1, 'foo') as ROW (x BIGINT, y VARCHAR(100))) as col");
    OK_SIMPLE_STMT(Stmt, "create table if not exists test_complex_json "
        "as select cast(ARRAY[ARRAY[1, 23], ARRAY[456]] as JSON) as col");
    return OK;
}

ODBC_TEST(test_cleanup)
{
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_complex_array");
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_complex_map");
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_complex_row");
    OK_SIMPLE_STMT(Stmt, "drop table if exists test_complex_json");
    return OK;
}

//=========================================================
//test for Complex DataType SQLGetTypeInfo
//1)SQLGetTypeInfo now has 20 DATA_TYPE
//2)SQLColumns   DATA_TYPE
//=========================================================
ODBC_TEST(test_sqlgettypeinfo_complexdata)
{
   SQLSMALLINT  dataTypeCols;
   SQLLEN       dataTypeRows;
   SQLCHAR      params[64];
   int counter = 0;

   //check  all 20 kinds  datatype
   CHECK_STMT_RC(Stmt, SQLGetTypeInfo(Stmt, SQL_ALL_TYPES));
   CHECK_STMT_RC(Stmt, SQLNumResultCols(Stmt, &dataTypeCols));
   CHECK_STMT_RC(Stmt, SQLRowCount(Stmt, &dataTypeRows));

   is_num(dataTypeCols, 19);
   is_num(dataTypeRows, 20);
   CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));
   
   //check  varchar data type
   CHECK_STMT_RC(Stmt, SQLGetTypeInfo(Stmt, SQL_VARCHAR));
   CHECK_STMT_RC(Stmt, SQLRowCount(Stmt, &dataTypeRows));
   while (SQL_SUCCEEDED(SQLFetch(Stmt))) {
       IS_STR(my_fetch_str(Stmt, params, 1), infoassert[counter].datatype, 
           sizeof(infoassert[counter].datatype));
       IS_STR(my_fetch_str(Stmt, params, 6), infoassert[counter].create_params, 
           sizeof(infoassert[counter].create_params));
       is_num(my_fetch_int(Stmt, 8), infoassert[counter].case_sensitive);
       is_num(my_fetch_int(Stmt, 9), infoassert[counter].searchable);
       counter++;
   }
   CHECK_STMT_RC(Stmt, SQLFreeStmt(Stmt, SQL_CLOSE));

    return OK;
}


ODBC_TEST(test_complexdata_sqlcolumns)
{
    SQLCHAR     TypeName[64];
    SQLSMALLINT DataType=0;
    SQLLEN      ColumnSize = 0;
    SQLLEN      BufferLength = 0;
    SQLLEN      OctetLength = 0;
    int count = 0;

    CHECK_STMT_RC(Stmt, SQLColumns(Stmt, NULL, SQL_NTS, NULL, SQL_NTS,
        (SQLCHAR *)"test_complex_%", SQL_NTS,
        NULL, SQL_NTS));

    while (SQL_SUCCEEDED(SQLFetch(Stmt)))
    {
        CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 6, SQL_C_CHAR, TypeName,
            sizeof(TypeName), NULL));
        if (sizeof(TypeName) != 0 && strstr(TypeName, "array(integer)")) {
            diag("ARRAY");
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 5, SQL_C_SSHORT, &DataType,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 7, SQL_C_ULONG, &ColumnSize,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 8, SQL_C_ULONG, &BufferLength,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 16, SQL_C_ULONG, &OctetLength,
                0, NULL));
            is_num(DataType, SQL_VARCHAR);
            is_num(ColumnSize, 2048);
            is_num(BufferLength, 2048);
            is_num(OctetLength, 2048);
            count++;
        }
        if (sizeof(TypeName) != 0 && strstr(TypeName, "map(varchar(3), integer)")) {
            diag("MAP");
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 5, SQL_C_SSHORT, &DataType,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 7, SQL_C_ULONG, &ColumnSize,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 8, SQL_C_ULONG, &BufferLength,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 16, SQL_C_ULONG, &OctetLength,
                0, NULL));
            is_num(DataType, SQL_VARCHAR);
            is_num(ColumnSize, 2048);
            is_num(BufferLength, 2048);
            is_num(OctetLength, 2048);
            count++;
        }
        if (sizeof(TypeName) != 0 && strstr(TypeName, "row(x bigint, y varchar(100))")) {
            diag("ROW");
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 5, SQL_C_SSHORT, &DataType,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 7, SQL_C_ULONG, &ColumnSize,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 8, SQL_C_ULONG, &BufferLength,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 16, SQL_C_ULONG, &OctetLength,
                0, NULL));
            is_num(DataType, SQL_VARCHAR);
            is_num(ColumnSize, 2048);
            is_num(BufferLength, 2048);
            is_num(OctetLength, 2048);
            count++;
        }
        if (sizeof(TypeName) != 0 && strstr(TypeName, "json")) {
            diag("JSON");
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 5, SQL_C_SSHORT, &DataType,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 7, SQL_C_ULONG, &ColumnSize,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 8, SQL_C_ULONG, &BufferLength,
                0, NULL));
            CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 16, SQL_C_ULONG, &OctetLength,
                0, NULL));
            is_num(DataType, SQL_VARCHAR);
            is_num(ColumnSize, 2048);
            is_num(BufferLength, 2048);
            is_num(OctetLength, 2048);
            count++;
        }
        DataType = 0;
        ColumnSize = 0;
        BufferLength = 0;
        OctetLength = 0;
    }
    is_num(count, 4);

    return OK;
}

/*ARRAY*/
ODBC_TEST(test_array_prepare)
{
    SQLCHAR     ColumnName[64];
    SQLSMALLINT dataType = 0;
    SQLSMALLINT NameLength = 0;
    SQLULEN     ColumnSize = 0;
    SQLSMALLINT DecimalDigits = 999;
    SQLSMALLINT Nullable = 0;

    CHECK_STMT_RC(Stmt, SQLPrepare(Stmt, 
        (SQLCHAR*)"select ARRAY[1.0, 2.0, 3.0] as col", SQL_NTS));

    //check metadata
    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, 1, ColumnName, 64, &NameLength, &dataType, &ColumnSize, &DecimalDigits, &Nullable));
    IS_STR(ColumnName, "col", NameLength);
    is_num(dataType, SQL_VARCHAR);
    is_num(ColumnSize, 2048);
    is_num(DecimalDigits, 0);
    is_num(Nullable, SQL_NULLABLE);

    SQLLEN attrDataType = 0;
    SQLLEN attrNullable = 0;
    SQLLEN DisplaySize = 0;
    SQLLEN OctetLength = 0;
    ColumnSize = 0;

    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, &attrDataType));
    is_num(attrDataType, SQL_VARCHAR);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, &DisplaySize));
    is_num(DisplaySize, 2048);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_OCTET_LENGTH, NULL, 0, NULL, &OctetLength));
    is_num(OctetLength, 2048);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_LENGTH, NULL, 0, NULL, &ColumnSize));
    is_num(ColumnSize, 2048);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_NULLABLE, NULL, 0, NULL, &attrNullable));
    is_num(attrNullable, 1);
    return OK;
}

ODBC_TEST(test_array_fetch)
{
    SQLCHAR     ColumnName[64];
    SQLSMALLINT dataType = 0;
    SQLSMALLINT NameLength = 0;
    SQLULEN     ColumnSize = 0;
    SQLSMALLINT DecimalDigits = 999;
    SQLSMALLINT Nullable = 0;
    SQLLEN      nlen = 0;

    OK_SIMPLE_STMT(Stmt, "select * from test_complex_array");

    //check metadata
    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, 1, ColumnName, 64, &NameLength, &dataType, &ColumnSize, &DecimalDigits, &Nullable));
    IS_STR(ColumnName, "col", NameLength);
    is_num(dataType, SQL_VARCHAR);
    is_num(ColumnSize, 2048);
    is_num(DecimalDigits, 0);
    is_num(Nullable, SQL_NULLABLE);

    SQLCHAR* Buffer = (SQLCHAR*)calloc(ColumnSize, sizeof(SQLCHAR));

    while (SQL_SUCCEEDED(SQLFetch(Stmt)))
    {
        CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 1, SQL_C_CHAR, Buffer,
            ColumnSize, &nlen));
        is_num(nlen, strlen("[1, 2, null]"));
        IS_STR(Buffer, "[1, 2, null]", nlen);
    }
    return OK;
}

/*MAP*/
ODBC_TEST(test_map_prepare)
{
    SQLCHAR     ColumnName[64];
    SQLSMALLINT dataType = 0;
    SQLSMALLINT NameLength = 0;
    SQLULEN     ColumnSize = 0;
    SQLSMALLINT DecimalDigits = 999;
    SQLSMALLINT Nullable = 0;

    CHECK_STMT_RC(Stmt, SQLPrepare(Stmt,
        (SQLCHAR*)"select MAP(ARRAY[0, 1], ARRAY[2, NULL]) as col", SQL_NTS));

    //check metadata
    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, 1, ColumnName, 64, &NameLength, &dataType, &ColumnSize, &DecimalDigits, &Nullable));
    IS_STR(ColumnName, "col", NameLength);
    is_num(dataType, SQL_VARCHAR);
    is_num(ColumnSize, 2048);
    is_num(DecimalDigits, 0);
    is_num(Nullable, SQL_NULLABLE);

    SQLLEN attrDataType = 0;
    SQLLEN attrNullable = 0;
    SQLLEN DisplaySize = 0;
    SQLLEN OctetLength = 0;
    ColumnSize = 0;

    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, &attrDataType));
    is_num(attrDataType, SQL_VARCHAR);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, &DisplaySize));
    is_num(DisplaySize, 2048);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_OCTET_LENGTH, NULL, 0, NULL, &OctetLength));
    is_num(OctetLength, 2048);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_LENGTH, NULL, 0, NULL, &ColumnSize));
    is_num(ColumnSize, 2048);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_NULLABLE, NULL, 0, NULL, &attrNullable));
    is_num(attrNullable, 1);
    return OK;
}

ODBC_TEST(test_map_fetch)
{
    SQLCHAR     ColumnName[64];
    SQLSMALLINT dataType = 0;
    SQLSMALLINT NameLength = 0;
    SQLULEN     ColumnSize = 0;
    SQLSMALLINT DecimalDigits = 999;
    SQLSMALLINT Nullable = 0;
    SQLLEN      nlen = 0;

    OK_SIMPLE_STMT(Stmt, "select * from test_complex_map");

    //check metadata
    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, 1, ColumnName, 64, &NameLength, &dataType, &ColumnSize, &DecimalDigits, &Nullable));
    IS_STR(ColumnName, "col", NameLength);
    is_num(dataType, SQL_VARCHAR);
    is_num(ColumnSize, 2048);
    is_num(DecimalDigits, 0);
    is_num(Nullable, SQL_NULLABLE);

    SQLCHAR* Buffer = (SQLCHAR*)calloc(ColumnSize, sizeof(SQLCHAR));

    while (SQL_SUCCEEDED(SQLFetch(Stmt)))
    {
        CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 1, SQL_C_CHAR, Buffer,
            ColumnSize, &nlen));
        is_num(nlen, strlen("{bar=null, foo=1}"));
        IS_STR(Buffer, "{bar=null, foo=1}", nlen);
    }
    return OK;
}

/*ROW*/
ODBC_TEST(test_row_prepare)
{
    SQLCHAR     ColumnName[64];
    SQLSMALLINT dataType = 0;
    SQLSMALLINT NameLength = 0;
    SQLULEN     ColumnSize = 0;
    SQLSMALLINT DecimalDigits = 999;
    SQLSMALLINT Nullable = 0;

    CHECK_STMT_RC(Stmt, SQLPrepare(Stmt,
        (SQLCHAR*)"select cast(ROW(1, NULL) as ROW(x BIGINT, y DOUBLE)) as col", SQL_NTS));

    //check metadata
    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, 1, ColumnName, 64, &NameLength, &dataType, &ColumnSize, &DecimalDigits, &Nullable));
    IS_STR(ColumnName, "col", NameLength);
    is_num(dataType, SQL_VARCHAR);
    is_num(ColumnSize, 2048);
    is_num(DecimalDigits, 0);
    is_num(Nullable, SQL_NULLABLE);

    SQLLEN attrDataType = 0;
    SQLLEN attrNullable = 0;
    SQLLEN DisplaySize = 0;
    SQLLEN OctetLength = 0;
    ColumnSize = 0;

    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, &attrDataType));
    is_num(attrDataType, SQL_VARCHAR);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, &DisplaySize));
    is_num(DisplaySize, 2048);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_OCTET_LENGTH, NULL, 0, NULL, &OctetLength));
    is_num(OctetLength, 2048);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_LENGTH, NULL, 0, NULL, &ColumnSize));
    is_num(ColumnSize, 2048);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_NULLABLE, NULL, 0, NULL, &attrNullable));
    is_num(attrNullable, 1);
    return OK;
}

ODBC_TEST(test_row_fetch)
{
    SQLCHAR     ColumnName[64];
    SQLSMALLINT dataType = 0;
    SQLSMALLINT NameLength = 0;
    SQLULEN     ColumnSize = 0;
    SQLSMALLINT DecimalDigits = 999;
    SQLSMALLINT Nullable = 0;
    SQLLEN      nlen = 0;

    OK_SIMPLE_STMT(Stmt, "select * from test_complex_row");

    //check metadata
    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, 1, ColumnName, 64, &NameLength, &dataType, &ColumnSize, &DecimalDigits, &Nullable));
    IS_STR(ColumnName, "col", NameLength);
    is_num(dataType, SQL_VARCHAR);
    is_num(ColumnSize, 2048);
    is_num(DecimalDigits, 0);
    is_num(Nullable, SQL_NULLABLE);

    SQLCHAR* Buffer = (SQLCHAR*)calloc(ColumnSize, sizeof(SQLCHAR));

    while (SQL_SUCCEEDED(SQLFetch(Stmt)))
    {
        CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 1, SQL_C_CHAR, Buffer,
            ColumnSize, &nlen));
        is_num(nlen, strlen("{x=1, y=foo}"));
        IS_STR(Buffer, "{x=1, y=foo}", nlen);
    }
    return OK;
}

ODBC_TEST(test_row_fetchname)
{
    SQLCHAR     ColumnName[64];
    SQLSMALLINT dataType = 0;
    SQLSMALLINT NameLength = 0;
    SQLULEN     ColumnSize = 0;
    SQLSMALLINT DecimalDigits = 999;
    SQLSMALLINT Nullable = 0;
    SQLLEN      nlen = 0;

    SQLCHAR Buffer[10];

    OK_SIMPLE_STMT(Stmt, "select col.x from test_complex_row");

    //check metadata
    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, 1, ColumnName, 64, &NameLength, &dataType, &ColumnSize, &DecimalDigits, &Nullable));
    IS_STR(ColumnName, "x", NameLength);
    is_num(dataType, SQL_BIGINT);
    is_num(ColumnSize, 20);
    is_num(DecimalDigits, 0);
    is_num(Nullable, SQL_NULLABLE);

    while (SQL_SUCCEEDED(SQLFetch(Stmt)))
    {
        CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 1, SQL_C_CHAR, Buffer,
            sizeof(Buffer), &nlen));
        is_num(nlen, strlen("1"));
        IS_STR(Buffer, "1", nlen);
    }
    return OK;
}

ODBC_TEST(test_row_fetchindex)
{
    SQLCHAR     ColumnName[64];
    SQLSMALLINT dataType = 0;
    SQLSMALLINT NameLength = 0;
    SQLULEN     ColumnSize = 0;
    SQLSMALLINT DecimalDigits = 999;
    SQLSMALLINT Nullable = 0;
    SQLLEN      nlen = 0;

    OK_SIMPLE_STMT(Stmt, "select col[2] from test_complex_row");

    //check metadata
    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, 1, ColumnName, 64, &NameLength, &dataType, &ColumnSize, &DecimalDigits, &Nullable));
    IS_STR(ColumnName, "_col0", NameLength);
    is_num(dataType, SQL_VARCHAR);
    is_num(ColumnSize, 100);
    is_num(DecimalDigits, 0);
    is_num(Nullable, SQL_NULLABLE);

    SQLCHAR* Buffer = (SQLCHAR*)calloc(ColumnSize, sizeof(SQLCHAR));

    while (SQL_SUCCEEDED(SQLFetch(Stmt)))
    {
        CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 1, SQL_C_CHAR, Buffer,
            ColumnSize, &nlen));
        is_num(nlen, strlen("foo"));
        IS_STR(Buffer, "foo", nlen);
    }
    return OK;
}

/*JSON*/
ODBC_TEST(test_json_prepare)
{
    SQLCHAR     ColumnName[64];
    SQLSMALLINT dataType = 0;
    SQLSMALLINT NameLength = 0;
    SQLULEN     ColumnSize = 0;
    SQLSMALLINT DecimalDigits = 999;
    SQLSMALLINT Nullable = 0;

    CHECK_STMT_RC(Stmt, SQLPrepare(Stmt,
        (SQLCHAR*)"select cast(ARRAY[ARRAY['foo', 'bar'], ARRAY['foobar']] as JSON) as col", SQL_NTS));

    //check metadata
    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, 1, ColumnName, 64, &NameLength, &dataType, &ColumnSize, &DecimalDigits, &Nullable));
    IS_STR(ColumnName, "col", NameLength);
    is_num(dataType, SQL_VARCHAR);
    is_num(ColumnSize, 2048);
    is_num(DecimalDigits, 0);
    is_num(Nullable, SQL_NULLABLE);

    SQLLEN attrDataType = 0;
    SQLLEN attrNullable = 0;
    SQLLEN DisplaySize = 0;
    SQLLEN OctetLength = 0;
    ColumnSize = 0;

    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, &attrDataType));
    is_num(attrDataType, SQL_VARCHAR);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, &DisplaySize));
    is_num(DisplaySize, 2048);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_OCTET_LENGTH, NULL, 0, NULL, &OctetLength));
    is_num(OctetLength, 2048);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_LENGTH, NULL, 0, NULL, &ColumnSize));
    is_num(ColumnSize, 2048);
    CHECK_STMT_RC(Stmt, SQLColAttribute(Stmt, 1, SQL_DESC_NULLABLE, NULL, 0, NULL, &attrNullable));
    is_num(attrNullable, 1);
    return OK;
}

ODBC_TEST(test_json_fetch)
{
    SQLCHAR     ColumnName[64];
    SQLSMALLINT dataType = 0;
    SQLSMALLINT NameLength = 0;
    SQLULEN     ColumnSize = 0;
    SQLSMALLINT DecimalDigits = 999;
    SQLSMALLINT Nullable = 0;
    SQLLEN      nlen = 0;

    OK_SIMPLE_STMT(Stmt, "select * from test_complex_json");

    //check metadata
    CHECK_STMT_RC(Stmt, SQLDescribeCol(Stmt, 1, ColumnName, 64, &NameLength, &dataType, &ColumnSize, &DecimalDigits, &Nullable));
    IS_STR(ColumnName, "col", NameLength);
    is_num(dataType, SQL_VARCHAR);
    is_num(ColumnSize, 2048);
    is_num(DecimalDigits, 0);
    is_num(Nullable, SQL_NULLABLE);

    SQLCHAR* Buffer = (SQLCHAR*)calloc(ColumnSize, sizeof(SQLCHAR));

    while (SQL_SUCCEEDED(SQLFetch(Stmt)))
    {
        CHECK_STMT_RC(Stmt, SQLGetData(Stmt, 1, SQL_C_CHAR, Buffer,
            ColumnSize, &nlen));
        is_num(nlen, strlen("[[1,23],[456]]"));
        IS_STR(Buffer, "[[1,23],[456]]", nlen);
    }
    return OK;
}

MA_ODBC_TESTS my_tests[]=
{
    {test_preparedata,                      "test_preparedata"},
    {test_sqlgettypeinfo_complexdata,       "test_sqlgettypeinfo_complexdata"},
    {test_complexdata_sqlcolumns,           "test_complexdata_sqlcolumns"},
    {test_array_prepare,                    "test_array_prepare"},
    {test_array_fetch,                      "test_array_fetch"},
    {test_map_prepare,                      "test_map_prepare"},
    {test_map_fetch,                        "test_map_fetch"},
    {test_row_prepare,                      "test_row_prepare"},
    {test_row_fetch,                        "test_row_fetch"},
    {test_row_fetchname,                    "test_row_fetchname"},
    {test_row_fetchindex,                   "test_row_fetchindex"},
    {test_json_prepare,                     "test_json_prepare"},
    {test_json_fetch,                       "test_json_fetch"},
    {test_cleanup,                          "test_cleanup"},
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

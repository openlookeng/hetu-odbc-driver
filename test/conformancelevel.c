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


ODBC_TEST(tests_capabilitiesFromSQLGetFunctions)
{
     SQLUSMALLINT funcs[SQL_API_ODBC3_ALL_FUNCTIONS_SIZE] = {SQL_FALSE};

     CHECK_DBC_RC(Connection, SQLGetFunctions(Connection, SQL_API_ODBC3_ALL_FUNCTIONS, funcs));

     //unsupport function lists
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLBROWSECONNECT), SQL_FALSE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLDESCRIBEPARAM), SQL_FALSE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLCANCEL), SQL_FALSE);

     //support part feature, but set to unsupport
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLBULKOPERATIONS), SQL_FALSE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLSETCURSORNAME), SQL_FALSE);

     //support function
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLSETPOS), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLGETCONNECTATTR), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLGETCURSORNAME), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLGETDATA), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLGETDESCFIELD), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLGETDESCREC), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLGETDIAGFIELD), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLGETDIAGREC), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLGETENVATTR), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLGETFUNCTIONS), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLGETINFO), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLGETSTMTATTR), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLGETTYPEINFO), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLMORERESULTS), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLNATIVESQL), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLNUMPARAMS), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLNUMRESULTCOLS), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLPARAMDATA), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLPREPARE), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLPRIMARYKEYS), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLPROCEDURECOLUMNS), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLPROCEDURES), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLPUTDATA), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLROWCOUNT), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLSETCONNECTATTR), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLSETDESCFIELD), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLSETDESCREC), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLSETENVATTR), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLSETSTMTATTR), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLSPECIALCOLUMNS), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLSTATISTICS), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLTABLEPRIVILEGES), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLTABLES), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLDATASOURCES), SQL_TRUE);
     is_num(SQL_FUNC_EXISTS(funcs, SQL_API_SQLDRIVERS), SQL_TRUE);

     return OK;
}

#define  MAX_STR_LENG  4096

ODBC_TEST(tests_capabilitiesFromSQLGetInfo)
{
    SQLSMALLINT sintValue4InfoValue;
    SQLUINTEGER unitBitmask4InfoValue;
    SQLUINTEGER unitValue4InfoValue;
    SQLUINTEGER unitBinaryValue4InfoValue;
    SQLCHAR   stringValue4InfoValue[MAX_STR_LENG];
    SQLSMALLINT length;

    //for SQLUINTEGER bitmask
    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_FUNCTIONS, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_FN_CVT_CAST | SQL_FN_CVT_CONVERT);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_BIGINT, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_DOUBLE | SQL_CVT_REAL |
                                  SQL_CVT_BIGINT | SQL_CVT_VARCHAR | SQL_CVT_TINYINT);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_BIT, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, 0);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_CHAR, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CVT_CHAR | SQL_CVT_VARCHAR);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_WCHAR, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, 0);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_DATE, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CVT_DATE | SQL_CVT_TIMESTAMP | SQL_CVT_VARCHAR);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_DECIMAL, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CVT_REAL | SQL_CVT_INTEGER | SQL_CVT_SMALLINT |
                                  SQL_CVT_DECIMAL | SQL_CVT_TINYINT | SQL_CVT_BIGINT |
                                  SQL_CVT_SMALLINT | SQL_CVT_VARCHAR);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_DOUBLE, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CVT_REAL | SQL_CVT_INTEGER | SQL_CVT_SMALLINT |
                                  SQL_CVT_DOUBLE | SQL_CVT_TINYINT | SQL_CVT_BIGINT |
                                  SQL_CVT_SMALLINT | SQL_CVT_VARCHAR);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_FLOAT, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, 0);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_INTEGER, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CVT_INTEGER | SQL_CVT_SMALLINT | SQL_CVT_REAL |
                                  SQL_CVT_DOUBLE | SQL_CVT_TINYINT | SQL_CVT_BIGINT |
                                  SQL_CVT_SMALLINT | SQL_CVT_VARCHAR);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_LONGVARCHAR, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, 0);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_WLONGVARCHAR, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, 0);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_NUMERIC, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, 0);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_REAL, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CVT_REAL | SQL_CVT_INTEGER | SQL_CVT_SMALLINT |
                                  SQL_CVT_DOUBLE | SQL_CVT_TINYINT | SQL_CVT_BIGINT |
                                  SQL_CVT_SMALLINT | SQL_CVT_VARCHAR);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_SMALLINT, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CVT_INTEGER | SQL_CVT_SMALLINT | SQL_CVT_REAL |
                                  SQL_CVT_DOUBLE | SQL_CVT_TINYINT | SQL_CVT_BIGINT |
                                  SQL_CVT_SMALLINT | SQL_CVT_VARCHAR);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_TIME, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_VARCHAR);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_TIMESTAMP, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CVT_DATE | SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_VARCHAR);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_TINYINT, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CVT_REAL | SQL_CVT_INTEGER | SQL_CVT_SMALLINT |
                                  SQL_CVT_DOUBLE | SQL_CVT_TINYINT | SQL_CVT_BIGINT |
                                  SQL_CVT_SMALLINT | SQL_CVT_VARCHAR);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_VARBINARY, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CVT_VARBINARY);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_VARCHAR, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CVT_VARCHAR | SQL_CVT_VARBINARY | SQL_CVT_CHAR | SQL_CVT_SMALLINT | 
                                  SQL_CVT_INTEGER | SQL_CVT_DOUBLE | SQL_CVT_REAL |SQL_CVT_BIGINT | SQL_CVT_TINYINT);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_WVARCHAR, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, 0);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CONVERT_BIGINT, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CVT_SMALLINT | SQL_CVT_INTEGER | SQL_CVT_DOUBLE | SQL_CVT_REAL |
                                  SQL_CVT_BIGINT | SQL_CVT_VARCHAR | SQL_CVT_TINYINT);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_STRING_FUNCTIONS, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_FN_STR_ASCII | SQL_FN_STR_CHAR | SQL_FN_STR_CONCAT |
                                  SQL_FN_STR_LCASE |SQL_FN_STR_LEFT | SQL_FN_STR_LENGTH | SQL_FN_STR_LOCATE |
                                  SQL_FN_STR_LTRIM | SQL_FN_STR_POSITION | SQL_FN_STR_REPEAT |
                                  SQL_FN_STR_REPLACE | SQL_FN_STR_RIGHT | SQL_FN_STR_RTRIM |
                                  SQL_FN_STR_SPACE | SQL_FN_STR_SUBSTRING | SQL_FN_STR_UCASE);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_SYSTEM_FUNCTIONS, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_FN_SYS_IFNULL | SQL_FN_SYS_USERNAME);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_TIMEDATE_FUNCTIONS, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_FN_TD_CURRENT_DATE | SQL_FN_TD_CURRENT_TIME | SQL_FN_TD_CURRENT_TIMESTAMP |
                                                SQL_FN_TD_CURDATE | SQL_FN_TD_CURTIME | SQL_FN_TD_DAYOFMONTH |
                                                SQL_FN_TD_DAYOFWEEK | SQL_FN_TD_DAYOFYEAR | SQL_FN_TD_EXTRACT | SQL_FN_TD_HOUR |
                                                SQL_FN_TD_MINUTE | SQL_FN_TD_MONTH | SQL_FN_TD_NOW | SQL_FN_TD_QUARTER |
                                                SQL_FN_TD_SECOND | SQL_FN_TD_TIMESTAMPADD | SQL_FN_TD_TIMESTAMPDIFF |
                                                SQL_FN_TD_WEEK | SQL_FN_TD_YEAR);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NUMERIC_FUNCTIONS, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_FN_NUM_ABS | SQL_FN_NUM_ACOS | SQL_FN_NUM_ASIN | SQL_FN_NUM_ATAN |
                                                SQL_FN_NUM_ATAN2 | SQL_FN_NUM_CEILING | SQL_FN_NUM_COS | SQL_FN_NUM_DEGREES |
                                                SQL_FN_NUM_EXP | SQL_FN_NUM_FLOOR | SQL_FN_NUM_LOG | SQL_FN_NUM_LOG10 | SQL_FN_NUM_MOD |
                                                SQL_FN_NUM_PI | SQL_FN_NUM_POWER | SQL_FN_NUM_RADIANS | SQL_FN_NUM_RAND | SQL_FN_NUM_ROUND |
                                                SQL_FN_NUM_SIGN | SQL_FN_NUM_SIN | SQL_FN_NUM_SQRT | SQL_FN_NUM_TAN);
    IS(length == sizeof(SQLUINTEGER));


    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CREATE_SCHEMA, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CS_CREATE_SCHEMA | SQL_CS_AUTHORIZATION);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_DROP_SCHEMA, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_DS_DROP_SCHEMA);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CREATE_TABLE, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CT_CREATE_TABLE);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_DROP_TABLE, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_DT_DROP_TABLE);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_CREATE_VIEW, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_CV_CREATE_VIEW);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_DROP_VIEW, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_DV_DROP_VIEW);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_DEFAULT_TXN_ISOLATION, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_TXN_READ_UNCOMMITTED);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_INDEX_KEYWORDS, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, 0);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_INFO_SCHEMA_VIEWS, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, 0);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_OJ_CAPABILITIES, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_OJ_LEFT | SQL_OJ_RIGHT |
                                                SQL_OJ_FULL | SQL_OJ_NESTED |
                                                SQL_OJ_NOT_ORDERED | SQL_OJ_INNER |
                                                SQL_OJ_ALL_COMPARISON_OPS );
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_BATCH_ROW_COUNT, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, 0);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_BATCH_SUPPORT, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, 0);
    IS(length == sizeof(SQLUINTEGER));

    unitBitmask4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_SQL92_DATETIME_FUNCTIONS, &unitBitmask4InfoValue,
                            sizeof(unitBitmask4InfoValue), &length));
    is_num(unitBitmask4InfoValue, SQL_SDF_CURRENT_DATE | SQL_SDF_CURRENT_TIME | SQL_SDF_CURRENT_TIMESTAMP);
    IS(length == sizeof(SQLUINTEGER));


    //for SQLUINTEGER value
    unitValue4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_MAX_INDEX_SIZE, &unitValue4InfoValue,
                            sizeof(unitValue4InfoValue), &length));
    is_num(unitValue4InfoValue, 0);
    IS(length == sizeof(SQLUINTEGER));

    unitValue4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_ODBC_INTERFACE_CONFORMANCE, &unitValue4InfoValue,
                            sizeof(unitValue4InfoValue), &length));
    is_num(unitValue4InfoValue, SQL_OIC_CORE);
    IS(length == sizeof(SQLUINTEGER));

    unitValue4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_OJ_CAPABILITIES, &unitValue4InfoValue,
                            sizeof(unitValue4InfoValue), &length));
    is_num(unitValue4InfoValue, SQL_OJ_LEFT | SQL_OJ_RIGHT |
                                                SQL_OJ_FULL | SQL_OJ_NESTED |
                                                SQL_OJ_NOT_ORDERED | SQL_OJ_INNER |
                                                SQL_OJ_ALL_COMPARISON_OPS);
    IS(length == sizeof(SQLUINTEGER));


    //for SQLUSMALLINT value
    sintValue4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_GROUP_BY, &sintValue4InfoValue,
                            sizeof(sintValue4InfoValue), &length));
    is_num(sintValue4InfoValue, SQL_GB_GROUP_BY_CONTAINS_SELECT);
    IS(length == sizeof(SQLSMALLINT));

    sintValue4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_MAX_COLUMNS_IN_INDEX, &sintValue4InfoValue,
                            sizeof(sintValue4InfoValue), &length));
    is_num(sintValue4InfoValue, 0);
    IS(length == sizeof(SQLSMALLINT));

    sintValue4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_MAX_TABLES_IN_SELECT, &sintValue4InfoValue,
                            sizeof(sintValue4InfoValue), &length));
    is_num(sintValue4InfoValue, 0);
    IS(length == sizeof(SQLSMALLINT));

    sintValue4InfoValue = 1;
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_NON_NULLABLE_COLUMNS, &sintValue4InfoValue,
                            sizeof(sintValue4InfoValue), &length));
    is_num(sintValue4InfoValue, SQL_NNC_NULL);
    IS(length == sizeof(SQLSMALLINT));

    //for character string value
    memset (stringValue4InfoValue, '\0', MAX_STR_LENG);
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_DESCRIBE_PARAMETER, stringValue4InfoValue,
                            sizeof(stringValue4InfoValue), &length));
    IS_STR(stringValue4InfoValue, "Y", 1);
    IS(length == 1);

    memset (stringValue4InfoValue, '\0', MAX_STR_LENG);
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_KEYWORDS, stringValue4InfoValue,
                            sizeof(stringValue4InfoValue), &length));
    IS_STR(stringValue4InfoValue,  "CUBE,CURRENT_PATH,CURRENT_ROLE,GROUPING,LOCALTIME,LOCALTIMESTAMP,"
                                     "NORMALIZE,RECURSIVE,ROLLUP,UESCAPE,UNNEST", 106);
    IS(length == 106);

    memset (stringValue4InfoValue, '\0', MAX_STR_LENG);
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_MULT_RESULT_SETS, stringValue4InfoValue,
                            sizeof(stringValue4InfoValue), &length));
    IS_STR(stringValue4InfoValue,  "N", 1);
    IS(length == 1);

    memset (stringValue4InfoValue, '\0', MAX_STR_LENG);
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_PROCEDURE_TERM, stringValue4InfoValue,
                            sizeof(stringValue4InfoValue), &length));
    IS_STR(stringValue4InfoValue, "procedure", 9);
    IS(length == 9);

    memset (stringValue4InfoValue, '\0', MAX_STR_LENG);
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_PROCEDURES, stringValue4InfoValue,
                            sizeof(stringValue4InfoValue), &length));
    IS_STR(stringValue4InfoValue, "N", 1);
    IS(length == 1);

    memset (stringValue4InfoValue, '\0', MAX_STR_LENG);
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_SEARCH_PATTERN_ESCAPE, stringValue4InfoValue,
                            sizeof(stringValue4InfoValue), &length));
    IS_STR(stringValue4InfoValue, "\\", 1);
    IS(length == 1);

    memset (stringValue4InfoValue, '\0', MAX_STR_LENG);
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_SPECIAL_CHARACTERS, stringValue4InfoValue,
                            sizeof(stringValue4InfoValue), &length));
    IS_STR(stringValue4InfoValue, "", 0);
    IS(length == 0);

    memset (stringValue4InfoValue, '\0', MAX_STR_LENG);
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_ACCESSIBLE_PROCEDURES, stringValue4InfoValue,
                            sizeof(stringValue4InfoValue), &length));
    IS_STR(stringValue4InfoValue, "Y", 1);
    IS(length == 1);

    memset (stringValue4InfoValue, '\0', MAX_STR_LENG);
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_ACCESSIBLE_TABLES, stringValue4InfoValue,
                            sizeof(stringValue4InfoValue), &length));
    IS_STR(stringValue4InfoValue, "Y", 1);
    IS(length == 1);

    memset (stringValue4InfoValue, '\0', MAX_STR_LENG);
    CHECK_DBC_RC(Connection, SQLGetInfo(Connection, SQL_COLLATION_SEQ, stringValue4InfoValue,
                            sizeof(stringValue4InfoValue), &length));
    IS_STR(stringValue4InfoValue, "", 0);
    IS(length == 0);

    return OK;
}








/*****************************************************************/
//1)Interface Conformance Levels
// 1.1)SQLGetInfo SQL_ODBC_INTERFACE_CONFORMANCE  => 1
//  1.2)Function Conformance：    SQLGetFunctions 
//  1.3)Attribute Conformance：   SQLSetEnvAttr/SQLGetEnvAttr SQLSetConnectAttr/SQLGetConnectAttr  SQLSetStmtAttr/SQLGetStmtAttr
//  1.4)Descriptor Field Conformance
//      SQLGetDescField/SQLSetDescField
//      SQLGetDescRec/SQLSetDescRec
//
//2)SQL Conformance Levels 
//SQLGetInfo

//FOR 1.2,1.3,1.4 no capabilities detect method
/*****************************************************************/

MA_ODBC_TESTS my_tests[]=
{
    {tests_capabilitiesFromSQLGetFunctions,  "tests_capabilitiesFromSQLGetFunctions"},
    {tests_capabilitiesFromSQLGetInfo,       "tests_capabilitiesFromSQLGetInfo"},
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

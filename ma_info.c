/************************************************************************************
   Copyright (C) 2013, 2017 MariaDB Corporation AB
   Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
   
   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.
   
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.
   
   You should have received a copy of the GNU Library General Public
   License along with this library; if not see <http://www.gnu.org/licenses>
   or write to the Free Software Foundation, Inc., 
   51 Franklin St., Fifth Floor, Boston, MA 02110, USA
*************************************************************************************/
#include <ma_odbc.h>

MADB_TypeInfo TypeInfoV3[]=
{
    //TypeName,DataType,ColumnSize,LiteralPrefix,LiteralSuffix,CreateParams,Nullable,CaseSensitive,Searchable,_Unsigned,Unsigned,FixedPrecScale,_AutoUniqueValue,AutoUniqueValue,
    //LocalTypeName,_MinimumScale,MinimumScale,_MaximumScale,MaximumScale,SqlDataType1,_SqlDateTimeSub,SqlDateTimeSub,_NumPrecRadix,NumPrecRadix,SQLDataType
    
    //TN,DT,CoSi,LP,LS,CP,Na,CaSen,Sa,_Us,Us,FPS,_AUV,AUV,LTN,_MinS,MinS,_MaxS,MaxS,SDT1,_STsub,STsub,_NPR,NPR,SDT
    { "boolean",SQL_BIT,1,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"NULL","0","NULL","boolean","NULL","NULL","NULL","NULL",SQL_BIT },
    { "bigint",SQL_BIGINT,19,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"0","0","NULL","bigint","NULL","NULL","NULL","10",SQL_BIGINT },
    { "double",SQL_DOUBLE,53,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"0","0","NULL","double","0","0","NULL","2",SQL_DOUBLE }, //IntervalPrecision are all NULL, NO NEED to initialize explicitly(0)
    { "decimal",SQL_DECIMAL,38,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"0","0","NULL","decimal","0","38","NULL","10",SQL_DECIMAL },
    { "tinyint",SQL_TINYINT,3,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"0","0","NULL","tinyint","NULL","NULL","NULL","10",SQL_TINYINT },
    { "smallint",SQL_SMALLINT,5,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"0","0","NULL","smallint","NULL","NULL","NULL","10",SQL_SMALLINT },
    { "integer",SQL_INTEGER,10,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"0","0","NULL","integer","NULL","NULL","NULL","10",SQL_INTEGER },
    { "real",SQL_REAL,24,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"0","0","NULL","real","0","0","NULL","2",SQL_REAL},
    { "varchar",SQL_VARCHAR,2048,"''''","''''","'max length'",1,0,SQL_SEARCHABLE,"NULL","0","NULL","varchar","NULL","NULL","NULL","NULL",SQL_VARCHAR },
    { "array",SQL_VARCHAR,2048,"''''","''''","'max length'",1,0,SQL_UNSEARCHABLE,"NULL","0","NULL","array","NULL","NULL","NULL","NULL",SQL_VARCHAR },
    { "map",SQL_VARCHAR,2048,"''''","''''","'max length'",1,0,SQL_UNSEARCHABLE,"NULL","0","NULL","map","NULL","NULL","NULL","NULL",SQL_VARCHAR },
    { "row",SQL_VARCHAR,2048,"''''","''''","'max length'",1,0,SQL_UNSEARCHABLE,"NULL","0","NULL","row","NULL","NULL","NULL","NULL",SQL_VARCHAR },
    { "json",SQL_VARCHAR,2048,"''''","''''","'max length'",1,0,SQL_UNSEARCHABLE,"NULL","0","NULL","json","NULL","NULL","NULL","NULL",SQL_VARCHAR },
    { "char",SQL_CHAR,255,"''''","''''","'LENGTH'",1,0,SQL_SEARCHABLE,"NULL","0","NULL","char","NULL","NULL","NULL","NULL",SQL_CHAR },
    { "varbinary",SQL_VARBINARY,2048,"'0x'''","NULL","'max length'",1,0,SQL_ALL_EXCEPT_LIKE,"NULL","0","NULL","varbinary","NULL","NULL","NULL","NULL",SQL_VARBINARY },
    { "date",SQL_TYPE_DATE,10,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"NULL","0","NULL","date","NULL","NULL","1","NULL",SQL_DATE },
    { "time",SQL_TYPE_TIME,12,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"NULL","0","NULL","time","0","3","2","NULL",SQL_DATE },
    { "timestamp",SQL_TYPE_TIMESTAMP,23,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"NULL","0","NULL","timestamp","0","3","3","NULL",SQL_DATE },
    { "interval year to month",SQL_VARCHAR,2048,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"NULL","0","NULL","interval year to month","NULL","NULL","NULL","NULL",SQL_VARCHAR },
    { "interval day to second",SQL_VARCHAR,2048,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"NULL","0","NULL","interval day to second","NULL","NULL","NULL","NULL",SQL_VARCHAR },
    { NULL,0,0,NULL,NULL,NULL,0,0,0,0,0,0,NULL,0,0,0,0,0,0 }


};
MADB_TypeInfo TypeInfoV2[]=
{
    { "boolean",SQL_BIT,1,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"NULL","0","NULL","boolean","NULL","NULL","NULL","NULL",SQL_BIT },
    { "bigint",SQL_BIGINT,19,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"0","0","NULL","bigint","NULL","NULL","NULL","10",SQL_BIGINT },
    { "double",SQL_DOUBLE,53,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"0","0","NULL","double","0","0","NULL","2",SQL_DOUBLE }, //IntervalPrecision are all NULL£¬NO NEED to initialize explicitly£¨0£©
    { "decimal",SQL_DECIMAL,38,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"0","0","NULL","decimal","0","38","NULL","10",SQL_DECIMAL },
    { "tinyint",SQL_TINYINT,3,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"0","0","NULL","tinyint","NULL","NULL","NULL","10",SQL_TINYINT },
    { "smallint",SQL_SMALLINT,5,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"0","0","NULL","smallint","NULL","NULL","NULL","10",SQL_SMALLINT },
    { "integer",SQL_INTEGER,10,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"0","0","NULL","integer","NULL","NULL","NULL","10",SQL_INTEGER },
    { "real",SQL_REAL,24,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"0","0","NULL","real","0","0","NULL","2",SQL_REAL },
    { "varchar",SQL_VARCHAR,2048,"''''","''''","'max length'",1,0,SQL_SEARCHABLE,"NULL","0","NULL","varchar","NULL","NULL","NULL","NULL",SQL_VARCHAR },
    { "array",SQL_VARCHAR,2048,"''''","''''","'max length'",1,0,SQL_UNSEARCHABLE,"NULL","0","NULL","array","NULL","NULL","NULL","NULL",SQL_VARCHAR },
    { "map",SQL_VARCHAR,2048,"''''","''''","'max length'",1,0,SQL_UNSEARCHABLE,"NULL","0","NULL","map","NULL","NULL","NULL","NULL",SQL_VARCHAR },
    { "row",SQL_VARCHAR,2048,"''''","''''","'max length'",1,0,SQL_UNSEARCHABLE,"NULL","0","NULL","row","NULL","NULL","NULL","NULL",SQL_VARCHAR },
    { "json",SQL_VARCHAR,2048,"''''","''''","'max length'",1,0,SQL_UNSEARCHABLE,"NULL","0","NULL","json","NULL","NULL","NULL","NULL",SQL_VARCHAR },
    { "char",SQL_CHAR,255,"''''","''''","'LENGTH'",1,0,SQL_SEARCHABLE,"NULL","0","NULL","char","NULL","NULL","NULL","NULL",SQL_CHAR },
    { "varbinary",SQL_VARBINARY,2048,"'0x'''","NULL","'max length'",1,0,SQL_ALL_EXCEPT_LIKE,"NULL","0","NULL","varbinary","NULL","NULL","NULL","NULL",SQL_VARBINARY }, //todo 0x
    { "date",SQL_TYPE_DATE,10,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"NULL","0","NULL","date","NULL","NULL","1","NULL",SQL_DATE },
    { "time",SQL_TYPE_TIME,12,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"NULL","0","NULL","time","0","3","2","NULL",SQL_DATE },
    { "timestamp",SQL_TYPE_TIMESTAMP,23,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"NULL","0","NULL","timestamp","0","3","3","NULL",SQL_DATE },
    { "interval year to month",SQL_VARCHAR,2048,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"NULL","0","NULL","interval year to month","NULL","NULL","NULL","NULL",SQL_VARCHAR },
    { "interval day to second",SQL_VARCHAR,2048,"NULL","NULL","NULL",1,0,SQL_ALL_EXCEPT_LIKE,"NULL","0","NULL","interval day to second","NULL","NULL","NULL","NULL",SQL_VARCHAR },
    { NULL,0,0,NULL,NULL,NULL,0,0,0,0,0,0,NULL,0,0,0,0,0,0 }
};

static MADB_ShortTypeInfo gtiDefType[19]= {{0, 0, 0, 0}, {SQL_SMALLINT, 0, 0, 0}, {SQL_INTEGER, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0},
                                 /*7*/     {SQL_SMALLINT, 0, 0, 0}, {SQL_SMALLINT, 0, 0, 0}, {SQL_SMALLINT, 0, 0, 0}, {SQL_SMALLINT, 0, 0, 0},
                                 /*11*/    {SQL_SMALLINT, 0, 0, 0}, {SQL_SMALLINT, 0, 0, 0}, {0, 0, 0, 0}, {SQL_SMALLINT, 0, 0, 0}, {SQL_SMALLINT, 0, 0, 0},
                                 /*16*/    {SQL_SMALLINT, 0, 0, 0}, {SQL_SMALLINT, 0, 0, 0}, {SQL_INTEGER, 0, 0, 0}, {SQL_SMALLINT, 0, 0, 0} };
/* {{{ MADB_GetTypeInfo */
SQLRETURN MADB_GetTypeInfo(SQLHSTMT StatementHandle,
                           SQLSMALLINT DataType)
{
  MADB_Stmt *Stmt= (MADB_Stmt *)StatementHandle;
  SQLRETURN ret;
  my_bool   isFirst= TRUE;
  my_bool   hasResult = FALSE;
  char      StmtStr[5120];
  char      *p= StmtStr;
  int       length;
  int       i;
  MADB_TypeInfo *TypeInfo= TypeInfoV3;

  if (Stmt->Connection->Environment->OdbcVersion == SQL_OV_ODBC2)
  {
      //TODO: in odbc 2.0 COLUMN_SIZE-->PRECISION, FIXED_PREC_SCALE-->MONEY, AUTO_UNIQUE_VALUE-->AUTO_INCREMENT;
    TypeInfo= TypeInfoV2;
    /* We need to map time types */
    switch(DataType) {
      case SQL_TYPE_TIMESTAMP:
        DataType=SQL_TIMESTAMP;
        break;
      case SQL_TYPE_DATE:
        DataType= SQL_DATE;
        break;
      case SQL_TYPE_TIME:
        DataType= SQL_TIME;
        break;
      default:
      break;
    }
  }

  StmtStr[0]= 0;
  for (i=0;TypeInfo[i].TypeName; i++)
  {
    if (DataType == SQL_ALL_TYPES ||
       TypeInfo[i].DataType == DataType)
    {
      if(isFirst)
      {
        isFirst= FALSE;
        hasResult = TRUE;

        length = _snprintf_s(p, sizeof(StmtStr) - strlen(StmtStr), sizeof(StmtStr) - strlen(StmtStr) - 1,
            "SELECT '%s' AS TYPE_NAME, %d AS DATA_TYPE, %lu AS COLUMN_SIZE, %s AS LITERAL_PREFIX, "
            "%s AS LITERAL_SUFFIX, %s AS CREATE_PARAMS, %d AS NULLABLE, %d AS CASE_SENSITIVE, " //'%s' makes its diff from simba <NULL>
            "%d AS SEARCHABLE, %s AS UNSIGNED_ATTRIBUTE, %s AS FIXED_PREC_SCALE, %s AS AUTO_UNIQUE_VALUE, "
            "'%s' AS LOCAL_TYPE_NAME, %s AS MINIMUM_SCALE, %s AS MAXIMUM_SCALE, %d AS SQL_DATA_TYPE, "
            "%s AS SQL_DATETIME_SUB, %s AS NUM_PREC_RADIX, NULL AS INTERVAL_PRECISION ",
            TypeInfo[i].TypeName, TypeInfo[i].DataType, TypeInfo[i].ColumnSize, TypeInfo[i].LiteralPrefix,
            TypeInfo[i].LiteralSuffix, TypeInfo[i].CreateParams, TypeInfo[i].Nullable, TypeInfo[i].CaseSensitive,
            TypeInfo[i].Searchable, TypeInfo[i].Unsigned, TypeInfo[i].FixedPrecScale, TypeInfo[i].AutoUniqueValue,
            TypeInfo[i].LocalTypeName, TypeInfo[i].MinimumScale, TypeInfo[i].MaximumScale, TypeInfo[i].SqlDataType,
            TypeInfo[i].SqlDateTimeSub, TypeInfo[i].NumPrecRadix);
        p = (length >= 0) ? (p + length) : (&StmtStr[sizeof(StmtStr) - 1]);
      }
      else
      {
          length = _snprintf_s(p, sizeof(StmtStr) - strlen(StmtStr), sizeof(StmtStr) - strlen(StmtStr) - 1,
              "UNION SELECT '%s', %d, %lu , %s, "
              "%s, %s, %d, %d, %d, %s, %s, %s, "
              "'%s', %s, %s, %d, "
              "%s, %s, NULL ",
              TypeInfo[i].TypeName, TypeInfo[i].DataType, TypeInfo[i].ColumnSize, TypeInfo[i].LiteralPrefix,
              TypeInfo[i].LiteralSuffix, TypeInfo[i].CreateParams, TypeInfo[i].Nullable, TypeInfo[i].CaseSensitive,
              TypeInfo[i].Searchable, TypeInfo[i].Unsigned, TypeInfo[i].FixedPrecScale, TypeInfo[i].AutoUniqueValue,
              TypeInfo[i].LocalTypeName, TypeInfo[i].MinimumScale, TypeInfo[i].MaximumScale, TypeInfo[i].SqlDataType,
              TypeInfo[i].SqlDateTimeSub, TypeInfo[i].NumPrecRadix);
          p = (length >= 0) ? (p + length) : (&StmtStr[sizeof(StmtStr) - 1]);
      }
    }
  }
  
  if (hasResult)
  {
      length = _snprintf_s(p, sizeof(StmtStr) - strlen(StmtStr), sizeof(StmtStr) - strlen(StmtStr) - 1, "ORDER BY TYPE_NAME");
  }
  else
  {
      length = _snprintf_s(p, sizeof(StmtStr) - strlen(StmtStr), sizeof(StmtStr) - strlen(StmtStr) - 1,
          "SELECT 'EMPTY RESULT' AS TYPE_NAME, -7 AS DATA_TYPE, 1 AS COLUMN_SIZE, NULL AS LITERAL_PREFIX, "
          "NULL AS LITERAL_SUFFIX, NULL AS CREATE_PARAMS, 1 AS NULLABLE, 0 AS CASE_SENSITIVE, "
          "2 AS SEARCHABLE, NULL AS UNSIGNED_ATTRIBUTE, 0 AS FIXED_PREC_SCALE, NULL AS AUTO_UNIQUE_VALUE, "
          "'EMPTY RESULT' AS LOCAL_TYPE_NAME, NULL AS MINIMUM_SCALE, NULL AS MAXIMUM_SCALE, -7 AS SQL_DATA_TYPE, "
          "NULL AS SQL_DATETIME_SUB, NULL AS NUM_PREC_RADIX, NULL AS INTERVAL_PRECISION "
          "WHERE 1=0"
      );
  }

  p = (length >= 0) ? (p + length) : (&StmtStr[sizeof(StmtStr) - 1]);
  
  ret= Stmt->Methods->ExecDirect(Stmt, StmtStr, SQL_NTS);
  if (SQL_SUCCEEDED(ret))
  {
    MADB_FixColumnDataTypes(Stmt, gtiDefType);
  }
  return ret;
}
/* }}} */

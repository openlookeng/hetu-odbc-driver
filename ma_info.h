/************************************************************************************
   Copyright (C) 2013, 2016 MariaDB Corporation AB
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
#ifndef _ma_info_h_
#define _ma_info_h_


//struct modified. 
//--First reason is when initializing this, original mariadb seems only send in 19 values,
//this means SqlDataType will always be 0 in resultset, it's against ODBC API and MySQL practice.
//--Second is to treat 5 columns that defined as smallint but will accpet null as result.
//--Third is reuse SqlDataType1 as USER_DATA_TYPE


typedef struct
{
  char *TypeName;
  SQLSMALLINT DataType;
  SQLINTEGER ColumnSize;
  char *LiteralPrefix;
  char *LiteralSuffix;
  char *CreateParams;
  SQLSMALLINT Nullable;
  SQLSMALLINT CaseSensitive;
  SQLSMALLINT Searchable;
  char *Unsigned;
  char *FixedPrecScale;
  char *AutoUniqueValue;
  char *LocalTypeName;
  char *MinimumScale;
  char *MaximumScale;
  char *SqlDateTimeSub;
  char *NumPrecRadix;
  SQLSMALLINT SqlDataType;
  SQLSMALLINT IntervalPrecision;

} MADB_TypeInfo;


SQLRETURN MADB_GetTypeInfo(SQLHSTMT StatementHandle,
                           SQLSMALLINT DataType);

#endif /* _ma_info_h_ */

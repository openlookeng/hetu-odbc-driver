# ODBC Protocol Implementation 

## Protocol Implementation 
1.  Supports 16 types of openLooKeng data type (test case: test/datatypes.c). 
2.  Complies with the ODBC 3.5 protocol and supports core-level APIs and some L1 and L2 APIs. (Test case: test/conformancelevel.c) 
3.  Supports the scalar function of the ODBC and SQL 92 style (test case: test/sqlgrammar.c). 
4.  Catalog functions (test case: test/catalog1.c) 
5.  Prepared execution and Direct Execution (test case: test/datahandle.c) 
6.  Test on PowerBI Desktop and Tableau Desktop 

Note: For details about the protocol implementation, see the end-to-end test cases in the test directory. 

## Restrictions
1.  Only the 64-bit Windows operating system is released. Currently, the following operating systems have been verified: Windows 10 and Windows Server 2016. 
2.  The metadata API may be slow because the Presto Server queries the information schema slowly. 

Note: Other Windows systems may also run properly, but we have not verified the system. You are welcome to provide the test results and problems of other systems. If you have any feedback, submit the issue. 


# Basic principles

## Solution Introduction 
The openLooKeng ODBC Driver solution reuses existing components to the maximum extent and uses the bridging solution (the front end uses the ODBC protocol and the back end uses the JDBC protocol for interconnection). The open-source software MariaDB ODBC driver and MyCat Server are selected, the openLooKeng ODBC Driver is implemented based on the two software. 

![image](odbc-driver-components.png) 

  Major modifications at the front end: 
1.  Data type adaptation 
2.  Catalog function adaptation 
3.  Consistency level and setting adaptation 
4.  DNS parameter adaptation and simplification 
5.  Connection and authentication parameters can be configured for different JDBC drivers. 
6.  Integrate the gateway component. 
7.  Installation program and DSN configuration interface OEM

  Major changes at the backend: 
1.  Backend data source connection and authentication passthrough 
2.  Presto data type adaptation and conversion 
3.  MySQL C/S Command Input and Output Parameter Data Types 
4.  Scalar function support in ODBC and SQL 92 styles 
5.  JVM memory optimization 
6.  E2E transaction support 
7.  OpenJDK integration 


## Compatibility with the Presto Ecosystem 
OpenLooKeng leverages the well-known open-source SQL engine - Presto,  and add a lot of enhancements over Presto. Therefore, the openLooKeng ODBC driver is also considered for the Presto ecosystem. We are compatible with the Presto ecosystem based on the following points: 
1.  Presto data type, SQL syntax    
2.  Presto data dictionary (system table)     
3.  Standard JDBC protocol   
4.  Replaceable JDBC Driver interconnected with the server 

The connection and authentication functions with the server are offloaded to the JDBC driver layer. In the branch version of the Presto ecosystem, a large number of customized implementation solutions are provided to implement the functions. Each version uses its own JDBC driver to implement the interconnection of these functions. In terms of JDBC configuration parameters, we have streamlined the configuration parameters between the version and the DSN, the DSN allows you to flexibly configure JDBC parameters. For details, see JDBC Driver Adaptation. 


# Development 
## Setting Up the Environment

### 1.  Prepare

The following software has been installed: 
* git https://git-scm.com/downloads 
* Wix https://github.com/wixtoolset/wix3/releases/tag/wix3104rtm    
* cmake https://github.com/Kitware/CMake/releases?after=v3.15.0-rc1
* Visual Studio 2017 or 2019 
* maven https://maven.apache.org/download.cgi 

### 2.  Build 
  ```shell 
    git clone https://gitee.com/openlookeng/hetu-odbc-driver.git
  ```
   or
  ```shell 
   git clone https://github.com/openlookeng/hetu-odbc-driver       
  ```

    Go to the code directory and configure and compile the code.   

  ```shell 
   cmake -G "Visual Studio 16 2019" -A x64 -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCONC_WITH_UNIT_TESTS=Off -DCONC_WITH_MSI=OFF -DWITH_SSL=SCHANNEL. 
   cmake --build . --config RelWithDebInfo
  ```

    Note: 1. During the build, two pieces of code that depend on the code repository are downloaded. Ensure that the download channel is available. For details about the two pieces of code, see the .gitmodules file. 
          2. If the build version needs to be specified, you can pass in the following four variables through the -D parameter:
    MARIADB_ODBC_VERSION_MAJOR, MARIADB_ODBC_VERSION_MINOR, MARIADB_ODBC_VERSION_PATCH and GATEWAY_TAG. The first three Variables MAJOR, MINOR, and PATCH represent the three digits of the version number, and GATEWAY_TAG represents the release tag of the gateway code.
    	For example, to build version 1.0.1:
    	cmake -G "Visual Studio 16 2019" -A x64 -DMARIADB_ODBC_VERSION_MAJOR=1 -DMARIADB_ODBC_VERSION_MINOR=0 -DMARIADB_ODBC_VERSION_PATCH=1 -DGATEWAY_TAG="1.0.1" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCONC_WITH_UNIT_TESTS=Off -DCONC_WITH_MSI=OFF -DWITH_SSL=SCHANNEL .
    	cmake --build . --config RelWithDebInfo
          3. For the -G parameter, if you use Visual Studio 2017, set to "Visual Studio 15 2017".For more options, see the CMake manual.

## Development
### Program Starting and Stopping 
#### driver 
NA 

#### gateway 
  ```shell
 <Installation directory>\odbc_gateway\mycat\mycat.bat {start | stop | restart | status} 
  ```

## Program Log
### Trace Log Function of Microsoft ODBC Driver Manager 
   For details, see https://docs.microsoft.com/zh-cn/sql/odbc/admin/setting-tracing-options?view=sql-server-ver15 or https://docs.microsoft.com/EN-US/sql/odbc/admin/setting-tracing-options?view=sql-server-ver15 

### ODBC driver

On the DSN configuration page, select DEBUG. The Driver records debug logs in the %TMP%\MAODBC.LOG file. 

### ODBC gateway

 < Installation directory >\odbc_gateway\mycat\logs\mycat.log     
Historical logs are archived to subdirectories. 

## Program Test
### Microsoft ODBC Test 

The ODBC API test tool developed by Microsoft can test the ODBC API without writing a line of code. 
This tool is a part of the MDAC tool package. You can download the tool from https://www.microsoft.com/en-us/download/details.aspx?id=21995 

The simba blog provides a more detailed description of how to use it. 
https://www.simba.com/blog/testing-driver-odbc-test/ 

### Running Test Cases 
The test directory contains a large number of test cases. You can write your own test cases to test or debug code. 


# JDBC Driver Adaptation 
This function is compatible with PrestoSQL, PrestoDB and other presto forks with special connection and authentication implementation. It is connected to and authenticated by the server and has been uninstalled to the JDBC driver layer. You can perform secondary adaptation to the JDBC driver implementation layer to support the ODBC of the server. 

## 1.  Remove the driver of openLooKeng and change it to your JDBC driver. 

  ```xml 
      <dependency> 
        <groupId>io.hetu.core</groupId> 
        <artifactId>hetu-jdbc</artifactId> 
        <version>010</version> 
      </dependency> 
  ```
  Node: The version may be upgraded, here just an example.
## 2.  Change the protocol prefix of the JDBC driver. 
  server.xml 

```xml 
  <! -- 
    the jdbc url prefix, openLooKeng-> jdbc:lk://, presto-> jdbc:presto:// 
  --> 
  <property name="jdbcUrlPrefix">jdbc:lk://</property> 
```

## 3.  JVM customized parameters in the JDBC running environment 
```xml 
    wrapper.conf 
    wrapper.java.additional.XX= 
```
Add new configuration parameters to the file. 

Note: Before modifying the configuration items in the mycat directory, stop the mycat service. 

## 4.  Configuring the URL Parameters and Attribute File for the Connection         
    Set the customized connection parameters in Connect URL and Connect Config on the DSN configuration page.
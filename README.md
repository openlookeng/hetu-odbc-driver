# openLooKeng ODBC Driver

The ODBC driver of openLooKeng provides the ODBC interconnection capability for openLooKeng. In addition, this driver can be connected to other versions (such as prestosql and prestodb) based on presto evolution through configuration.


## Introduction

The ODBC driver consists of two parts: driver and gateway. The driver provides the capability of interconnecting with the ODBC protocol, and the gateway provides the capability of interconnecting with openLooKeng server.

The [driver](https://gitee.com/openlookeng/hetu-odbc-driver) is extended based on [mariadb-connector-odbc](https://github.com/mariadb-corporation/mariadb-connector-odbc). To adapt to openLooKeng, the following modifications are made:
1) Data type adaptation
2) Catalog function adaptation
3) Conformance Levels adaptation
4) DSN configuration adaptation and parameter simplification
5) Connection and authentication adaptation
6) Integrates the gateway component (build and installer).
7) Customization of the installation interface and DSN configuration interface

The [gateway](https://gitee.com/openlookeng/hetu-odbc-gateway) is extended based on [Mycat-Server](https://github.com/MyCATApache/Mycat-Server). To adapt to openLooKeng, the following modifications are made:
1) Directly connect to the backend data source (authentication and configuration).
2) Data type conversion and adaptation  
3) Input and output metadata processing of MySQL CS protocol preprocessing commands
4) Add the scalar functions for the ODBC and SQL92 specifications.
5) JVM memory optimization
6) Transaction
7) OpenJDK integration


## How to use:
  
   see openLookeng [user guide(english)](docs/userguide_en.md)  [user guide(chinese)](docs/userguide_zh.md)
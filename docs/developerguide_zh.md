# ODBC协议实现说明

## 协议实现
1. 支持16种openLooKeng数据类型(测试用例: test/datatypes.c)
2. 遵从ODBC 3.5协议，完成core级别和部分L1和L2的API的支持(测试用例: test/conformancelevel.c)
3. 支持ODBC和SQL 92风格的scalar function(测试用例: test/sqlgrammar.c)
4. 完整支持catalog functions(测试用例: catalog functions)(test/catalog1.c)
5. 支持预处理命令和直接执行命令
6. 经过PowerBI Desktop和Teabeau Desktop的测试

备注： 具体协议实现情况，可以参看test目录下的端到端用例，及时了解当前对协议实现状态。

## 约束和限制
1. 发布只提供64位window系统版本，当前验证过的OS有：win10,  win server 2016
2. 元数据接口API可能会比较慢，这个主要由于Presto Server本身的information schema查询较慢

备注：其他windows系统可能也是可以运行，只是我们没有进行验证，欢迎大家反馈其他系统的测试结果和问题，反馈请提交issue.


# 基本原理

## 方案介绍
​     openLooKeng ODBC Driver的方案采用最大化复用已有组件，整体采用桥接方案(前端采用ODBC协议，后端采用JDBC协议对接)，选取了mariadb ODBC driver和myCat Server二个开源软件, 在二个软件的基础上实现openLooKeng ODBC Driver的目标。

![image](odbc-driver-components.png) 

  前端的主要修改点：
1. 数据类型适配
2. Catalog function适配
3. 一致性级别和设置适配
4. DNS参数适配及简化
5. 支持不同JDBC Driver连接和认证参数可配置
6. 集成gateway组件
7. 安装程序和DSN配置界面OEM

  后端的主要修改点：
1. 后端数据源连接和认证直通
2. Presto数据类型适配和转换
3. MySQL C/S 命令输入和输出参数数据类型
4. ODBC和SQL 92风格的标量函数支持
5. JVM内存优化
6. 端到端事务支持
7. 集成openJDK


## 与Presto生态兼容问题
​     openLooKeng基于知名的开源数据库引擎Presto，并对Presto做了大量的功能增强。openLooKeng ODBC driver对Presto生态也是我们考虑的点，我们基于以下点，与Presto生态系统兼容:
1. Presto的数据类型，SQL语法   
2. Presto的数据字典(系统表)    
3. 标准JDBC协议  
4. 与server对接的JDBC Driver可替换

​     我们将与server的连接和认证功能都卸载到JDBC Driver层，也是考虑到Presto生态的分支版本在这一功能点上会大量定制化的实现方案，各自使用自己的JDBC Driver来实现这些功能的对接, 在JDBC配置参数上，我们已经将其与DSN的配置参数打通，DSN可以灵活配置JDBC参数，具体参考 JDBC Driver适配 章节。


# 开发
## 环境搭建

###  1. 准备

预先安装以下软件：
*  git     https://git-scm.com/downloads
*  Wix     https://github.com/wixtoolset/wix3/releases/tag/wix3104rtm   
*  cmake   https://github.com/Kitware/CMake/releases?after=v3.15.0-rc1 
*  Visual studio 2017或2019
*  maven https://maven.apache.org/download.cgi

###  2. 构建

  ```shell
    git clone https://gitee.com/openlookeng/hetu-odbc-driver.git
  ```
   或
  ```shell
   git clone https://github.com/openlookeng/hetu-odbc-driver
  ```
    进入代码目录后，配置和编译代码:

  ```shell
     cmake -G "Visual Studio 16 2019" -A x64 -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCONC_WITH_UNIT_TESTS=Off -DCONC_WITH_MSI=OFF -DWITH_SSL=SCHANNEL .
     cmake --build . --config RelWithDebInfo
  ```

    注意: 1.构建中会下载二个依赖代码仓的代码，请确保下载通道可用，二个依赖仓的信息参看.gitmodules文件。
         2.对-G参数，如使用Visual Studio 2017请使用“Visual Studio 15 2017"，更多选项请参看cmake手册。

## 开发调测
###   程序启动和停止
####  driver
NA

#### gateway
 <安装目录>\odbc_gateway\mycat\mycat.bat { start | stop | restart | status }

## 日志
### 微软ODBC驱动管理器的跟踪日志功能
   参看: https://docs.microsoft.com/zh-cn/sql/odbc/admin/setting-tracing-options?view=sql-server-ver15 或https://docs.microsoft.com/EN-US/sql/odbc/admin/setting-tracing-options?view=sql-server-ver15

### ODBC driver

在DSN配置界面，勾选DEBUG,  Driver会在%TMP%\MAODBC.LOG文件中记录debug日志

### ODBC gateway

<安装目录>\odbc_gateway\mycat\logs\mycat.log
历史日志会归档到子目录下

##  测试
### Microsoft ODBC Test

微软开发的ODBC API测试工具，可以在不写一行代码的情况下，对ODBC API进行测试。
属于MDAC工具包的一部分，工具下载地址： https://www.microsoft.com/en-us/download/details.aspx?id=21995

simba的blog提供了一份比较详细的使用说明
https://www.simba.com/blog/testing-driver-odbc-test/

### 自己编写测试用例
test目录下，有我们写的大量测试用例，可以参照编写自己的测试用例，来测试或调测代码。


# JDBC Driver适配
  此功能针对有特殊连接和认证实现的presto分支版本(如PrestoSQL, PrestoDB等)，与server端连接和认证，已经卸载到JDBC驱动层，可以通过二次适配JDBC驱动实现层来完成server的ODBC支持。

## 1. 修改openLooKeng的driver为自己的JDBC driver

  ```xml
      <dependency>
        <groupId>io.hetu.core</groupId>
        <artifactId>hetu-jdbc</artifactId>
        <version>010</version>
      </dependency>
  ```
  注意:  version可能升级，这里仅示例。
## 2. 修改jdbc driver的协议前缀
  server.xml

```xml
  <!--
    the jdbc url prefix, openLooKeng-> jdbc:lk://, presto-> jdbc:presto://
  -->
  <property name="jdbcUrlPrefix">jdbc:lk://</property>
```

## 3. JDBC运行环境JVM自定义参数设置
```xml
    wrapper.conf
    wrapper.java.additional.XX=
```
在其中添加新的配置参数

注意: 修改mycat目录下的配置项，需要先停mycat服务

## 4. 配置连接的URL参数和属性文件       
    将自定义的连接参数配置到 DSN配置界面中的Connect URL和Connect Config二个配置项中。    


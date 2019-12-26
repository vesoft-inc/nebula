# 欢迎使用 Nebula Graph 手册

**Nebula Graph** 是一个分布式的可扩展的高性能的图数据库。

**Nebula Graph** 可以容纳百亿节点和万亿条边，并达到毫秒级的时延.

## 前言

* [关于本手册](0.about-this-manual.md)
* [变更历史](CHANGELOG.md)

## 概览 (入门用户)

* [简介](1.overview/0.introduction.md)
* 基本概念
  * [数据模型](1.overview/1.concepts/1.data-model.md)
  * [查询语言概览](1.overview/1.concepts/2.nGQL-overview.md)
* 快速开始和常用链接
  * [开始试用](1.overview/2.quick-start/1.get-started.md)
  * [常见问题](1.overview/2.quick-start/2.trouble-shooting.md)
  * [编译源代码](3.build-develop-and-administration/1.build/1.build-source-code.md)
  * [部署集群](3.build-develop-and-administration/3.deploy-and-administrations/deployment/deploy-cluster.md)
  * [导入 .csv 文件](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/data-import/import-csv-file.md)
  * [加载 .sst 文件](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/data-import/download-and-ingest-sst-file.md)
  * [Nebula Graph 客户端](1.overview/2.quick-start/3.supported-clients.md)
  * [常见问题 FAQ](1.overview/2.quick-start/2.FAQ.md)
* 系统设计与架构
  * [设计总览](1.overview/3.design-and-architecture/1.design-and-architecture.md)
  * [存储层架构](1.overview/3.design-and-architecture/2.storage-design.md)
  * [查询引擎架构](1.overview/3.design-and-architecture/3.query-engine.md)

## 查询语言 (所有用户)

* 数据类型
  * [基本数据类型](2.query-language/1.data-types/data-types.md)
  * [类型转换](2.query-language/1.data-types/type-conversion.md)
* 函数与操作符
  * [位运算](2.query-language/2.functions-and-operators/bitwise-operators.md)
  * [内置函数](2.query-language/2.functions-and-operators/built-in-functions.md)
  * [比较运算](2.query-language/2.functions-and-operators/comparison-functions-and-operators.md)
  * [聚合运算](2.query-language/2.functions-and-operators/group-by-function.md)
  * [分页 (Limit)](2.query-language/2.functions-and-operators/limit-syntax.md)
  * [逻辑运算](2.query-language/2.functions-and-operators/logical-operators.md)
  * [排序 (Order By)](2.query-language/2.functions-and-operators/order-by-function.md)
  * [集合运算](2.query-language/2.functions-and-operators/set-operations.md)
  * [uuid 函数](2.query-language/2.functions-and-operators/uuid.md)
* 语言结构
  * 字面值常量
    * [布尔类型](2.query-language/3.language-structure/literal-values/boolean-literals.md)
    * [数值类型](2.query-language/3.language-structure/literal-values/numeric-literals.md)
    * [字符串类型](2.query-language/3.language-structure/literal-values/string-literals.md)
  * [注释语法](2.query-language/3.language-structure/comment-syntax.md)
  * [标识符大小写](2.query-language/3.language-structure/identifier-case-sensitivity.md)
  * [管道](2.query-language/3.language-structure/pipe-syntax.md)
  * [属性引用](2.query-language/3.language-structure/property-reference.md)
  * [标识符命名规则](2.query-language/3.language-structure/schema-object-names.md)
  * [语句组合](2.query-language/3.language-structure/statement-composition.md)
* 语句语法
  * 数据定义语句 (DDL)
    * [新建图空间](2.query-language/4.statement-syntax/1.data-definition-statements/create-space-syntax.md)
    * [新建 Tag 和 Edge](2.query-language/4.statement-syntax/1.data-definition-statements/create-tag-edge-syntax.md)
    * [更改 Tag 和 Edge](2.query-language/4.statement-syntax/1.data-definition-statements/alter-tag-edge-syntax.md)
    * [Drop Tag](2.query-language/4.statement-syntax/1.data-definition-statements/drop-tag-syntax.md)
    * [Drop Edge](2.query-language/4.statement-syntax/1.data-definition-statements/drop-edge-syntax.md)
    * [Drop Space](2.query-language/4.statement-syntax/1.data-definition-statements/drop-space-syntax.md)
  * 数据查询与操作语句 (DQL 和 DML)
    * [删除边](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/delete-edge-syntax.md)
    * [删除顶点](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/delete-vertex-syntax.md)
    * [获取点和边属性 (Fetch)](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/fetch-syntax.md)
    * [图遍历 (Go)](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/go-syntax.md)
    * [插入边](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/insert-edge-syntax.md)
    * [插入顶点](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/insert-vertex-syntax.md)
    * [更新点和边](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/update-vertex-edge-syntax.md)
    * [条件语句 (Where)](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/where-syntax.md)
    * [返回结果语句 (Yield)](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/yield-syntax.md)
    * [返回满足条件的语句 (Return)](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/return-syntax.md)
  * 辅助功能语句
    * [Describe](2.query-language/4.statement-syntax/3.utility-statements/describe-syntax.md)
    * [Use](2.query-language/4.statement-syntax/3.utility-statements/use-syntax.md)
    * [Show](2.query-language/4.statement-syntax/3.utility-statements/show-syntax.md)
  * 图算法
    * [查找路径](2.query-language/4.statement-syntax/4.graph-algorithms/find-path-syntax.md)

## 编译、部署与运维 (程序员和 DBA )

* 编译
  * [编译源代码](3.build-develop-and-administration/1.build/1.build-source-code.md)
  * [使用 Docker 编译](3.build-develop-and-administration/1.build/2.build-by-docker.md)
* 源码开发和 API
  * [Key Value 接口](3.build-develop-and-administration/2.develop-and-interface/kv-interfaces.md)
  * [Nebula Graph 客户端](1.overview/2.quick-start/3.supported-clients.md)

* 部署与运维
  * 部署
    * [配置文件说明](3.build-develop-and-administration/3.deploy-and-administrations/deployment/configuration-description.md)
    * [用 Docker 部署](3.build-develop-and-administration/3.deploy-and-administrations/deployment/deploy-cluster-on-docker.md)
    * [部署集群](3.build-develop-and-administration/3.deploy-and-administrations/deployment/deploy-cluster.md)
    * [接入 Prometheus](3.build-develop-and-administration/3.deploy-and-administrations/deployment/connect-prometheus.md)
  * 服务器管理操作
    * 账号管理
      * [Drop User](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/account-management-statements/drop-user-syntax.md)
    * 服务器配置
      * [服务器配置](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/configuration-statements/configs-syntax.md)
      * [RocksDB Compaction 和 Flush](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/configuration-statements/rocksdb-compaction-flush.md)
      * [日志](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/configuration-statements/log.md)
    * 计算服务相关运维
      * [计算层运行统计 (metrics)](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/graph-service-administration/graph-metrics.md)
    * meta 服务相关运维
      * [meta 层运行统计 (metrics)](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/meta-service-administration/meta-metrics.md)
    * 存储服务相关运维
      * 离线数据加载
        * [加载 .sst 文件](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/data-import/download-and-ingest-sst-file.md)
        * [读取 .csv 文件](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/data-import/import-csv-file.md)
        * [Spark 导入工具](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/data-import/spark-writer.md)
      * [负载均衡和数据迁移](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/storage-balance.md)
      * [存储层运行统计 (metrics)](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/storage-metrics.md)
      * [集群快照](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/cluster-snapshot.md)

## 社区贡献 (开源社区爱好者)

* [贡献文档](4.contributions/contribute-to-documentation.md)
* [C++ 编程风格](4.contributions/cpp-coding-style.md)
* [开发者文档风格](4.contributions/developer-documentation-style-guide.md)
* [如何贡献](4.contributions/how-to-contribute.md)

## 其他

### 视频

* [YouTube](https://www.youtube.com/channel/UC73V8q795eSEMxDX4Pvdwmw/)
* [Bilibili](https://space.bilibili.com/472621355)

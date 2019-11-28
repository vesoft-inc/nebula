# Spark Writer

## 概述

Spark Writer 是 Nebula Graph 基于 Spark 的分布式数据导入工具，能够将多种数据仓库中的数据转化为图的点和边，并批量导入到图数据库中。目前支持的数据仓库有：

* HDFS，包括 Parquet、JSON、ORC 和 CSV 格式的文件
* HIVE

Spark Writer 支持并发导入多个 tag、edge，支持不同 tag/edge 配置不同的数据仓库。

## 获取 Spark Writer

### 编译源码

```bash
git clone https://github.com/vesoft-inc/nebula.git
cd nebula/src/tools/spark-sstfile-generator
mvn compile package
```

或者直接下载

### 从云存储 OSS 下载

```bash
wget https://nebula-graph.oss-accelerate.aliyuncs.com/jar-packages/sst.generator-1.0.0-beta.jar
```

## 使用流程

基本流程分为以下几步：

1. 在 Nebula Graph 中创建图模型，构图
1. 编写数据文件
1. 编写输入源映射文件
1. 导入数据

### 构图

构图请参考[快速试用](../../../../../1.overview/2.quick-start/1.get-started.md)中的示例构图。

注意：请先在 Nebula Graph 中完成构图（创建图空间和定义图数据 Schema），再通过本工具向 Nebula Graph 中写入数据。

### 数据示例

#### 点

顶点数据文件由一行一行的数据组成，文件中每一行表示一个点和它的属性。一般来说，第一列为点的 ID ——此列的名称将在后文的映射文件中指定，其他列为点的属性。

* **player** 顶点数据

```text
{"id":100,"name":"Tim Duncan","age":42}
{"id":101,"name":"Tony Parker","age":36}
{"id":102,"name":"LaMarcus Aldridge","age":33}
```

#### 边

边数据文件由一行一行的数据组成，文件中每一行表示一条边和它的属性。一般来说，第一列为起点 ID，第二列为终点 ID，起点 ID 列及终点 ID 列会在映射文件中指定。其他列为边属性。下面以 JSON 格式为例进行说明。

以边 边 _**follow**_ 的数据为例：

* 无 rank 的边

```text
{"source":100,"target":101,"likeness":95}
{"source":101,"target":100,"likeness":95}
{"source":101,"target":102,"likeness":90}
```

* 有 rank 的边

```text
{"source":100,"target":101,"likeness":95,"ranking":2}
{"source":101,"target":100,"likeness":95,"ranking":1}
{"source":101,"target":102,"likeness":90,"ranking":3}
```

#### 含有地理位置 Geo 的数据

Spark Writer 支持 Geo 数据导入，Geo 数据用 **latitude** 与 **longitude** 字段描述经纬度，数据类型为 double。

```text
{"latitude":30.2822095,"longitude":120.0298785,"target":0,"dp_poi_name":"0"}
{"latitude":30.2813834,"longitude":120.0208692,"target":1,"dp_poi_name":"1"}
{"latitude":30.2807347,"longitude":120.0181162,"target":2,"dp_poi_name":"2"}
{"latitude":30.2812694,"longitude":120.0164896,"target":3,"dp_poi_name":"3"}
```

#### 数据源文件

目前 Spark Writer 支持的数据源有：

* HDFS
* HIVE

##### HDFS 文件

支持的文件格式包括：

* Parquet
* JSON
* CSV
* ORC

Player 的 Parquet 示例如下：

```text
+---+---+------------+
|age| id|        name|
+---+---+------------+
| 42|100| Tim Duncan |
| 36|101| Tony Parker|
+---+---+------------+
```

JSON 示例如下：

```json
{"id":100,"name":"Tim Duncan","age":42}
{"id":101,"name":"Tony Parker","age":36}
```

CSV 示例如下：

```csv
age,id,name
42,100,Tim Duncan
36,101,Tony Parker
```

##### 数据库

Spark Writer 支持以数据库作为数据源，目前支持 HIVE。

Player 表结构如下：

|col_name |  data_type  | comment |
|---------|-------------|---------|
| id      |     int     |         |
| name    |     string  |         |
| age     |     int     |         |

### 编写配置文件

配置文件由 Spark 相关信息，Nebula 相关信息，以及 tags 映射 和 edges 映射块组成。Spark 信息配置了 Spark 运行的相关参数，Nebula 相关信息配置了连接 Nebula Graph 的用户名和密码等信息。 tags 映射和 edges 映射分别对应多个 tag/edge 的输入源映射，描述每个 tag/edge 的数据源等基本信息，不同 tag/edge 可以来自不同数据源。

输入源的映射文件示例：

```text
{
  # Spark 相关信息配置
  # 参见： http://spark.apache.org/docs/latest/configuration.html
  spark: {
    app: {
      name: Spark Writer
    }

    driver: {
      cores: 1
      maxResultSize: 1G
    }

    cores {
      max: 16
    }
  }

  # Nebula Graph 相关信息配置
  nebula: {
    # 查询引擎 IP 列表
    addresses: ["127.0.0.1:3699"]

    # 连接 Nebula Graph 服务的用户名和密码
    user: user
    pswd: password

    # Nebula Graph 图空间名称
    space: test

    # thrift 超时时长及重试次数
    # 如未设置，则默认值分别为 3000 和 3
    connection {
      timeout: 3000
      retry: 3
    }

  # nGQL 查询重试次数
  # 如未设置，则默认值为 3
    execution {
      retry: 3
    }
  }

  # 标签处理
  tags: {

    # 从 HDFS 文件加载数据， 此处数据类型为 Parquet
    # tag 名称为 tag name 0
    #  HDFS Parquet 文件的中的 field 0、field 1、field 2 将写入 tag name 0
    # 节点列为 vertex key field
    tag name 0: {
      type: parquet
      path: hdfs path
      fields: {
        field 0: nebula field 0,
        field 1: nebula field 1,
        field 2: nebula field 2
      }
      vertex: vertex key field
      batch : 16
    }

    # 与上述类似
    # 从 Hive 加载将执行命令 $ {exec} 作为数据集
    tag name 1: {
      type: hive
      exec: "select hive field 0, hive field 1, hive field 2 from database.table"
      fields: {
        hive field 0: nebula field 0,
        hive field 1: nebula field 1,
        hive field 2: nebula field 2
      }
      vertex: vertex id field
    }
  }

  # 边处理
  edges: {
    # 从 HDFS 加载数据，数据类型为 JSON
    # 边名称为 edge name 0
    # HDFS JSON 文件中的 field 0、field 1、field 2 将被写入 edge name 0
    # 起始列为 source field
    edge name 0: {
      type: json
      path: hdfs path
      fields: {
        field 0: nebula field 0,
        field 1: nebula field 1,
        field 2: nebula field 2
      }
      source:  source field
      target:  target field
      ranking: ranking field
    }


   # 从 Hive 加载将执行命令 $ {exec} 作为数据集
   # 边权重为可选
   edge name 1: {
    type: hive
    exec: "select hive field 0, hive field 1, hive field 2 from database.table"
    fields: {
      hive field 0: nebula field 0,
      hive field 1: nebula field 1,
      hive field 2: nebula field 2
     }
    source:  source id field
    target:  target id field
   }
  }
}
```

#### Spark 配置信息

下表给出了一些示例，所有可配置项请见 [Spark Available Properties](http://spark.apache.org/docs/latest/configuration.html#available-properties)。

| 字段 | 默认值 | 是否必须 | 说明 |
|  --- | ---  |  --- | ---  |
| spark.app.name | Spark Writer | 否 | app 名称 |
| spark.driver.cores | 1 | 否 | 驱动程序进程的核数，仅适用于群集模式|
| spark.driver.maxResultSize | 1G | 否 | 每个 Spark 操作（例如收集）中所有分区的序列化结果的上限（以字节为单位）。至少应为 1M，否则应为 0（无限制）|
| spark.cores.max | (not set) | 否 | 当以“粗粒度”共享模式在独立部署群集或 Mesos 群集上运行时，跨群集（而非从每台计算机）请求应用程序的最大 CPU 核数。如果未设置，则默认值为 Spark 的独立集群管理器上的 `spark.deploy.defaultCores` 或 Mesos 上的 infinite（所有可用的内核）|

#### Nebula Graph 配置信息

| 字段 | 默认值 | 是否必须 | 说明 |
|  --- | ---  |  --- | ---  |
| nebula.addresses | 无 | 是 | 查询引擎的地址列表，逗号分隔 |
| nebula.user | 无 | 是 | 数据库用户名，默认为 user |
| nebula.pswd | 无 | 是 | 数据库用户名对应密码，默认 user 密码为 password |
| nebula.space | 无 | 是 | 导入数据对应的 space，本例中为 test |
| nebula.connection.timeout | 3000 | 否 | Thrift 连接超时时间 |
| nebula.connection.retry | 3 | 否 | Thrift 连接重试次数 |
| nebula.execution.retry | 3 | 否 | nGQL 语句执行重试次数 |

#### tags 和 edges 映射信息

tag 和 edge 映射的选项比较类似。下面先介绍相同的选项，再分别介绍 `tag 映射`和 `edge 映射`的特有选项。

* **相同的选项**
  * `type` 指定上文中提到的数据类型，目前支持 “Parquet”、"JSON"、"ORC" 和 “CSV”，大小写不敏感，必填
  * `path` 适用于 HDFS 数据源，指定HDFS 文件或目录的绝对路径，type 为 HDFS 时，必填
  * `exec` 适用于 Hive 数据源， 当执行查询语句 type 为 HIVE 时，必填
  * `fields` 将输入源列的列名映射为 tag / edge 的属性名，必填

* **tag 映射的特有选项**
  * `vertex` 指定某一列作为点的 ID 列，必填

* **edge 映射的特有选项**
  * `source` 指定输入源某一列作为**源点**的 ID 列，必填
  * `target` 指定某一列作为**目标点**的 ID 列，必填
  * 当插入边有 ranking 值， `ranking` 指定某一列作为边 ranking 列，选填

#### 数据源映射

* **HDFS Parquet 文件**
  * `type` 指定输入源类型，当为 parquet 时大小写不敏感，必填
  * `path` 指定 HDFS 文件或目录的路径，必须是 HDFS 的绝对路径，必填
* **HDFS JSON 文件**
  * `type` 指定输入源类型，当为 JSON 时大小写不敏感，必填
  * `path` 指定 HDFS 文件或目录的路径，必须是 HDFS 的绝对路径，必填
* **HIVE ORC 文件**
  * `type` 指定输入源类型，当为 ORC时大小写不敏感，必填
  * `path` 指定 HDFS 文件或目录的路径，必须是 HDFS 的绝对路径，必填
* **HIVE CSV 文件**
  * `type` 指定输入源类型，当为 CSV 时大小写不敏感，必填
  * `path` 指定 HDFS 文件或目录的路径，必须是 HDFS 的绝对路径，必填
* **HIVE**
  * `type` 指定输入源类型，当为 HIVE 时大小写不敏感，必填
  * `exec` 指定 HIVE 执行查询的语句，必填

### 执行命令导入数据

导入数据命令：

```bash
bin/spark-submit \
 --class com.vesoft.nebula.tools.generator.v2.SparkClientGenerator \
 --master ${MASTER-URL} \
 ${SPARK_WRITER_JAR_PACKAGE} -c conf/test.conf -h -d
```

参数说明：

| Abbreviation | Required | Default | Description | 示例 |
| --- | --- | --- | --- | --- |
| --class | yes |  | 指定程序主类 |  |
| --master | yes |  | 指定spark cluster master url，请参见 [master-urls](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls) | e.g. `spark://23.195.26.187:7077` |
| -c / --config  | yes |  | 上文所编写的配置文件路径 |  |
| -h / --hive | no | false | 用于指定是否支持 Hive |  |
| -d / --directly | no | false | true 为客户端方式插入；<br>false 为 sst 方式导入 (TODO) |  |
| -D / --dry | no | false | 检查配置文件是否正确 |  |

## 性能测试结果

三台物理机 (56 核，250G 内存，万兆网，SSD），写 1 亿条数据（每条数据三个字段，每个 batch 64 条记录），用时 4 分钟（40万条/秒）。

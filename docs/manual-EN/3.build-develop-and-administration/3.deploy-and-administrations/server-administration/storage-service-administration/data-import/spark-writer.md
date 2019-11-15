# Spark Writer

## 1. Overview

Spark Writer is Nebula Graph's Spark-based distributed data import tool that converts data from multiple data sources into vertices and edges of graphs and batch imports data into the graph database. Currently supported data sources are:

* HDFS files, including Parquet, JSON, ORC and CSV
* HIVE

Spark Writer supports importing multiple tags and edges at one time, and configuring different data sources on different tags and edges.

## 2. Get Spark Writer

### 2.1 From Source Code

```bash
git clone xxxxx
mvn compile package
```

### 2.2 oss Download

```bash
wget xxx
```

## 3. User Guide

User guide includes the following steps:

* Create a graph space and its schema in Nebula
* Write data files
* Write input source mapping file
* Import data

### 3.1 Create Graph Space

Please refer to the demo graph in [Quick Start](../../../../../1.overview/2.quick-start/1.get-started.md)。

### 3.2 Demo Data

#### 3.2.1 Vertices

A vertex data file consists of multiple rows. Generally one row represents one vertex, and one of the columns is the ID of the vertex. This ID column is specified in the mapping file. Other columns are the properties of the vertex. Consider the following example in JSON format.

* **Player** data

```text
{"id":100,"name":"Tim Duncan","age":42}
{"id":101,"name":"Tony Parker","age":36}
{"id":102,"name":"LaMarcus Aldridge","age":33}
```

#### 3.2.2 Edges

An edge data file consists of multiple rows. Generally one row represents one edge, and one of the columns is the ID of the source vertex, the other column is the ID of the dest vertex. These ID columns are specified in the mapping file. Other columns are the properties of the edge. Consider the following example in JSON format.

* Edge without rank, take edge _**follow**_ as example

```text
{"source":100,"target":101,"likeness":95}
{"source":101,"target":100,"likeness":95}
{"source":101,"target":102,"likeness":90}
```

* Edge with rank, take edge _**follow**_ as example

```text
{"source":100,"target":101,"likeness":95,"ranking":2}
{"source":101,"target":100,"likeness":95,"ranking":1}
{"source":101,"target":102,"likeness":90,"ranking":3}
```

#### 3.2.3 Supporting for Geo Data

Spark Writer supports importing Geo data. Geo data contains latitude and longitude, and the data type is double.

```text
{"latitude":30.2822095,"longitude":120.0298785,"target":0,"dp_poi_name":"0"}
{"latitude":30.2813834,"longitude":120.0208692,"target":1,"dp_poi_name":"1"}
{"latitude":30.2807347,"longitude":120.0181162,"target":2,"dp_poi_name":"2"}
{"latitude":30.2812694,"longitude":120.0164896,"target":3,"dp_poi_name":"3"}
```

#### 3.2.4 Data Source Files

The currently supported data sources by Spark Writer are:

* HDFS files, including the following file formats:
  * Parquet
  * JSON
  * CSV
  * ORC
* HIVE

Player data in Parquet format:

```text
+-------+---+---------+
|age| id|        name|
+-------+---+---------+
| 42|100| Tim Duncan |
| 36|101| Tony Parker|
+-------+---+---------+
```

In JSON:

```text
{"id":100,"name":"Tim Duncan","age":42}
{"id":101,"name":"Tony Parker","age":36}
```

In CSV:

```text
age,id,name
42,100,Tim Duncan
36,101,Tony Parker
```

### 3.3 Write Configuration Files

The configuration files consists of Spark related information, Nebula related information, and tags mapping and edges mapping blocks. Spark information is configured with the associated parameters running Spark. Nebula information is configured with information such as user name and password to connect Nebula. Tags mapping and edges mapping correspond to the input source mapping of multiple tag/edges respectively, describing the basic information like each tag/edge's data source. It's possible that different tag/edge come from different daa sources.

Example of a mapping file for the input source:

```text
{
  # Spark related configurations.
  # See also: http://spark.apache.org/docs/latest/configuration.html
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

  # Nebula related configurations.
  nebula: {
    # Query engine address list.
    addresses: ["127.0.0.1:3699"]

    # Nebula access user name and password.
    user: user
    pswd: password

    # Nebula space's name.
    space: test

    # The thrift connection timeout and retry times.
    # If no configurations are set, the default values are 3000 and 3 respectively.
    connection {
      timeout: 3000
      retry: 3
    }

  # The nGQL execution retry times.
  # If no configuration is set, the default value is 3.
    execution {
      retry: 3
    }
  }

  # Processing tags
  tags: {

    # Loading tag from HDFS and the data type is parquet.
    # The tag's name is tag-name-0.
    # field-0, field-1 and field-2 from HDFS's Parquet file are written into tag-name-0
    # and the vertex column is vertex-key-field.
    tag-name-0: {
      type: parquet
      path: hdfs path
      fields: {
        field-0: nebula-field-0,
        field-1: nebula-field-1,
        field-2: nebula-field-2
      }
      vertex: vertex-key-field
      batch : 16
    }

    # Similar to the above.
    # Loading from Hive will execute command ${exec} as data set.
    tag-name-1: {
      type: hive
      exec: "select hive-field-0, hive-field-1, hive-field-2 from database.table"
      fields: {
        hive-field-0: nebula-field-0,
        hive-field-1: nebula-field-1,
        hive-field-2: nebula-field-2
      }
      vertex: vertex-id-field
    }
  }

  # Processing edges
  edges: {
    # Loading edge from HDFS and data type is JSON.
    # The edge's name is edge-name-0.
    # field-0, field-1 and field-2 from HDFS's JSON file are written into edge-name-0
    # The source column is source-field, target column is target-field and ranking column is ranking-field.
    edge-name-0: {
      type: json
      path: hdfs path
      fields: {
        field-0: nebula-field-0,
        field-1: nebula-field-1,
        field-2: nebula-field-2
      }
      source:  source-field
      target:  target-field
      ranking: ranking-field
    }


   # Loading from Hive will execute command ${exec} as data set.
   # Ranking is optional.
   edge-name-1: {
    type: hive
    exec: "select hive-field-0, hive-field-1, hive-field-2 from database.table"
    fields: {
      hive-field-0: nebula-field-0,
      hive-field-1: nebula-field-1,
      hive-field-2: nebula-field-2
     }
    source:  source-id-field
    target:  target-id-field
   }
  }
}
```

#### 3.3.1 Spark Properties

The following table gives some example properties, all the which can be found in [Spark Available Properties](http://spark.apache.org/docs/latest/configuration.html#available-properties).

| Field | Default | Compulsory | Description |
| --- | --- | --- | --- |
| spark.app.name | Spark Writer | No | The name of your application |
| spark.driver.cores | 1 | No | Number of cores to use for the driver process, only in cluster mode. |
| spark.driver.maxResultSize | 1G | No | Limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or 0 for unlimited. |
| spark.cores.max | (not set) | No | When running on a standalone deploy cluster or a Mesos cluster in "coarse-grained" sharing mode, the maximum amount of CPU cores to request for the application from across the cluster (not from each machine). If not set, the default will be `spark.deploy.defaultCores` on Spark's standalone cluster manager, or infinite (all available cores) on Mesos. |

#### 3.3.2 Nebula Configuration

| Field | Default Value | Compulsory | Description |
| --- | --- | --- | --- |
| nebula.addresses | / | yes | query engine IP list, separated with comma |
| nebula.user | / | yes | user name |
| nebula.pswd | / | yes | password |
| nebula.space | / | yes | space to import data |
| nebula.connection.timeout | 3000 | no | Thrift timeout |
| nebula.connection.retry | 3 | no | Thrift retry times |
| nebula.execution.retry | 3 | no | nGQL execution retry times |

#### 3.3.3 Mapping of Tags and Edges

#### 3.3.4 Data Source Mapping

### 3.4 Import Data

## 4. Performance

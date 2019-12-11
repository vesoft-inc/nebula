### Introduction

`SparkClientGenerator` is a simple spark job used to write data into `Nebula Graph` parallel.

The insert statement is generated according to the configuration file and executed through thrift client.

Data Source could be `HDFS` or `Hive`.

In the configuration file, the tag section is similar to the edge section, which is used to specify the generation rules.

The `type` specified the data source type, and it should be `parquet`, `json`, `ORC`, `csv` or `hive`.

If the `type` is a file, the `path` is use to point out the `HDFS` path, otherwise it use `exec` to execute SQL in `hive`.

`fields` describe the mapping between the data field and the Nebula field.

In the tag section, use `vertex` to describe the vertex key, and in the edge section, `source` is the starting point, `target` is the ending point and `ranking` is the weight which is optional.

***
### Build

Build command:

```
mvn compile package
```


### Setup

To setup the Spark Writer, you should specified some spark arguments.

As following:

```
bin/spark-submit \
	--class com.vesoft.nebula.tools.generator.v2.SparkClientGenerator
	--master ${SPARK_MODE}
	sst.generator-1.0.0-beta.jar -c conf/test.conf -h -d
```

The `--class` describe the main class for you application. The `--master` is the master URL for the cluster.

*Command Line Argument*

|Abbreviation  | Name               | Description    | Required       | Default        |
|--------------|--------------------|----------------|----------------|----------------|
|c             | config             | config file    | Yes            |                |
|h             | hive               | hive supported | No             | false          |
|d             | directly           | directly mode  | No             | false          |
|D             | dry                | dry run        | No             | false          |


### Data example

Inserted data sample for `Get Start` is under `src/main/resources/data/json`.

The **student** tag's JSON data example:

```JSON
{"id":200,"name":"Monica","age":16,"gender":"female"}
{"id":201,"name":"Mike","age":18,"gender":"male"}
{"id":202,"name":"Jane","age":17,"gender":"female"}
```

The like edge's JSON date example:

```JSON
{"source":200,"target":201,"likeness":92.5}
{"source":201,"target":200,"likeness":85.6}
{"source":201,"target":202,"likeness":93.2}
```

The Geo JSON data example, `latitude` and `longitude` use to  describe coordinate.

```JSON
{"latitude":30.2822095,"longitude":120.0298785,"target":0,"dp_poi_name":"0"}
{"latitude":30.2813834,"longitude":120.0208692,"target":1,"dp_poi_name":"1"}
{"latitude":30.2807347,"longitude":120.0181162,"target":2,"dp_poi_name":"2"}
{"latitude":30.2812694,"longitude":120.0164896,"target":3,"dp_poi_name":"3"}
```


### Configuration

```
{
  # Spark relation config.
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

  # Nebula Graph relation config.
  nebula: {
    # Query engine address list.
    addresses: ["127.0.0.1:3699"]

    # Nebula access user name and password.
    user: user
    pswd: password

    # Nebula space's name.
    space: test

    # The thrift connection timeout and retry times.
    # If the timeout and retry no settings displayed,
    # the default value is 3000 and 3.
    connection {
      timeout: 3000
      retry: 3
    }

	 # The NGQL execution retry times
	 # If the execution retry no settings displayed,
    # the default value is 3.
    execution {
      retry: 3
    }
  }

  # Processing tags
  tags: {

    # Loading tag from HDFS and the data type is parquet.
    # The tag's name is tag-name-0.
    # Will write field-0, field-1 and field-2 from HDFS's Parquet file into tag-name-0
    # and the vertex column is vertex-field.
    tag-name-0: {
      type: parquet
      path: HDFS path
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
      vertex: vertex-key-field
    }
  }

  # Processing edges
  edges: {
    # Loading edge from HDFS and data type is json.
    # The edge's name is edge-name-0.
    # Will write field-0, field-1 and field-2 from HDFS's JSON file into edge-name-0
    # The source column is source-field, target column is target-field and ranking column is ranking-field.
    edge-name-0: {
      type: json
      path: HDFS path
      fields: {
        field-0: nebula-field-0,
        field-1: nebula-field-1,
        field-2: nebula-field-2
      }
      source:  source-field
      target:  target-field
      ranking: ranking-field
    }
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
    source:  source-field
    target:  target-field
  }

  # edge with GEO info, latitude and longitude are the coordinate
  edge-locate: {
      type: JSON
      path: HDFS path
      fields: {
        field-0: nebula-field-0,
        field-1: nebula-field-1,
        field-2: nebula-field-2
      }
      latitude:  latitude-field
      longitude: longitude-field
      target: target
    }
}
```


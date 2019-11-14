# Nebula-importer

## Introduction

Nebula-importer is a tool developed by [Nebula Graph](https://github.com/vesoft-inc/nebula-docker-compose) to import csv files. This tool reads local csv files and writes the data into Nebula Graph. Nebula-importer supports importing CSV files with `go` or with `docker`.

## Prerequisites
Before importing CSV files into **Nebula Graph**, you must ensure the following prerequisites are met:

1. Nebula Graph is installed by [`docker-compose`](https://github.com/vesoft-inc/nebula-docker-compose "nebula-docker-compose") or [rpm installation](https://github.com/vesoft-inc/nebula/tree/master/docs/manual-EN/3.build-develop-and-administration/3.deploy-and-administrations/deployment).
2. At least one space is created and used in Nebula Graph. 
3. Tags are created in **Nebula Graph**.
4. Edge types are created in **Nebula Graph**.
5. All services in **Nebula Graph** are up and running.

**Note**: The space selected in Nebula Graph is the place where your CSV files are imported. Tags are used to categorize your vertexes or nodes. Edge types are used to categorize your edges. If you do not know how to create spaces, tags or edges, you can refer to the **Build Your Own Graph** section in [`Quick Start`](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/1.overview/2.quick-start/1.get-started.md).


## Preparing the Configuration File

Nebula-importer reads a configuration file in `yaml` format to get all the required information about the connection to the Nebula Graph server, and the schema for tags and edges, etc.

For details about the `.yaml` file, you can refer to [`Example yaml`](example/example.yaml).

The following code block shows all the parameters that you might modify for your `.yaml` configuration file.


```yaml
version: v1rc1
description: example
clientSettings:
  concurrency: 4 # number of graph clients
  channelBufferSize: 128
  connection:
    user: user
    password: password
    address: 127.0.0.1:3699
logPath: ./err/test.log
files:
  - path: ./edge.csv
    failDataPath: ./err/edge.csv
    batchSize: 100
    type: csv
    csv:
      withHeader: false
      withLabel: false
    schema:
      space: test
      type: edge
      edge:
        name: edge_name
        withRanking: true
        props:
          - name: prop_name
            type: string
  - path: ./vertex.csv
    failDataPath: ./err/vertex.csv
    batchSize: 100
    type: csv
    csv:
      withHeader: false
      withLabel: false
    schema:
      space: test
      type: vertex
      vertex:
        tags:
          - name: tag1
            props:
              - name: prop1
                type: int
              - name: prop2
                type: timestamp
          - name: tag2
            props:
              - name: prop3
                type: double
              - name: prop4
                type: string
```

**Note**: In the above example, Nebula-importer imports two **csv** data files, `edge.csv` and `vertex.csv` in turn.

### Configuration Properties
The following table describes all the parameters in the `.yaml` configuration file.

| Parameters                                      | Description                                                               | Default        |
| :--                                          | :--                                                                       | :--            |
| version                                      | The version number of Nebula-importer                                                | v1rc1          |
| description                                  | Describe what you will do with this configuration file                                       | ""             |
| clientSettings                               | The graph client settings                                                     | -              |
| clientSettings.concurrency                   | The number of graph clients                                                   | 4              |
| clientSettings.channelBufferSize             | The buffer size of client channels                                            | 128            |
| clientSettings.connection                    | The connection options of graph clients                                        | -              |
| clientSettings.connection.user               | The username used to login Nebula Graph                                                                  | user           |
| clientSettings.connection.password           | The password used to login Nebula Graph                                                                  | password       |
| clientSettings.connection.address            | The address of the graph client that is composed of ip and port                                                   | 127.0.0.1:3699 |
| logPath                                      | The path used to record log files                                                          | ""             |
| files                                        | A list of files to be imported                                                  | -              |
| files[0].path                                | The path of the file to be imported                                                                | ""             |
| files[0].type                                | The path of the file to be imported                                                                 | csv            |
| files[0].csv                                 | The options for CSV files                                                          | -              |
| files[0].csv.withHeader                      | It indicates whether the csv file has header                                               | false          |
| files[0].csv.withLabel                       | It indicates whether csv file has `+/-` label to represent **delete/insert** operation | false          |
| files[0].schema                              | The schema definition for the file data                                      | -              |
| files[0].schema.space                        | The space name created in Nebula Graph                                              | ""             |
| files[0].schema.type                         | The schema type: vertex or edge                                               | vertex         |
| files[0].schema.edge                         | The options for edges                                                              | -              |
| files[0].schema.edge.name                    | The edge name in the selected space                                                  | ""             |
| files[0].schema.edge.withRanking             | It indicates whether this edge has ranking                                             | false          |
| files[0].schema.edge.props                   | The properties of the edge                                                    | -              |
| files[0].schema.edge.props[0].name           | The property name                                                             | ""             |
| files[0].schema.edge.props[0].type           | The property type                                                             | ""             |
| files[0].schema.vertex                       | The options for vertex                                                            | -              |
| files[0].schema.vertex.tags                  | The options for vertex tags                                                 | -              |
| files[0].schema.vertex.tags[0].name          | The vertex tag name                                                           | ""             |
| files[0].schema.vertex.tags[0].props         | The vertex tag's properties                                                   | -              |
| files[0].schema.vertex.tags[0].props[0].name | The vertex tag's property name                                                | ""             |
| files[0].schema.vertex.tags[0].props[0].type | The vertex tag's property type                                                | ""             |
| files[0].failDataPath                        | It indicates the file path for failed data                                                   | ""             |

## Preparing CSV Data

After you configure the `.yaml` configuration file, you have to prepare all the data required in the `.yaml` configuration file.

## Importing CSV Data
After your `.yaml` configuration file and all the required CSV data files are ready, you can import your data by [Importing CSV Data with Go](#importing-csv-data-with-go), or [Importing CSV Data with Docker](#importing-csv-data-with-docker).



### Importing CSV Data with Go

Nebula-importer depends on *golang 1.13*, so make sure you have installed `go` first. Before import data with `go` you have to set the environment variable for `go` as follows:
```
export PATH=$PATH:/usr/local/go/bin
export GOROOT=/usr/local/go
export GOPATH=/home/nebula/go:$HOME/nebula-importer
export GOPROXY=https://goproxy.cn
```
**Note**: You must set your environment for `go` according to your specific installation circumstances.

Now, you can import CSV files according to the following steps:

1. Clone the nebula-importer project to your local directory.

```
git clone https://github.com/vesoft-inc/nebula-importer.git
```

2. Change the directory to the nebula-importer file directory.

```
cd nebula-importer/cmd
```

3. Run nebula-importer with the following command.

```
go run importer.go --config /path/to/yaml/config/file
```

**Note**: If your `.yaml` configuration file is located at /home/nebula/config.yaml directory, you must change the /path/to/yaml/config/file directory to this directory.

### Importing CSV Data with Docker

With `docker`, You can import your local data to **Nebula Graph** with the following command:

```
    docker run --rm -ti \
    --network=host \
    -v {your-config-file}:/root/{your-config-file} \
    -v {your-csv-data-dir}:/root/{your-csv-data-dir} \
    vesoft/nebula-importer
    --config /root/{your-config-file}
```
**Note**: You have to change your direcotry to the directory where your `.yaml`configuration is stored. For example, if your `config.yaml` file is in the /home/nebula/ directory, it means {your-config-file} = /home/nebula/config.yaml; if your `csv` file is in the /home/nebula/ directory, it means {your-csv-data-dir} = /home/nebula/.


## CSV Data Example

Now we only support csv files without header. We are going to support csv files with header in the near future. So, in this we only import csv files without header.

### Without Header Line

#### Vertex

In a vertex csv data file, the first column can be a label(+/-) or the vid(vertex id). Vertex VID column must be the first column if the label option `csv.withLabel` is configured to `false`.
Then property values and their order must be in consistence with the `props` in configuration.

```csv
1,2,this is a property string
2,4,yet another property string
```

With label:

- `+`: Insert
- `-`: Delete

In labeled `-` row, only need the vid which you want to delete.

```csv
+,1,2,this is a property string
-,1
+,2,4,yet author property string
```

#### Edge

Edge csv data file format is like the vertex description. But the difference with above vertex vid is source vertex vid, destination vertex vid and edge ranking.

Without label column, `src_vid`, `dst_vid` and `ranking` always are first three columns in csv data file.

```csv
1,2,0,first property value
1,3,2,prop value
```

Ranking column is not required, you must not give it if you do not need it.

```csv
1,2,first property value
1,3,prop value
```

With label:

```csv
+,1,2,0,first property value
+,1,3,2,prop value
```

### With Header Line

This feature has not been supported now. Please remove the header from your csv data file at present.

#### Edge

```csv
_src,_dst,_ranking,prop1,prop2
...
```

`_src` and `_dst` represent edge source and destination vertex id. `_ranking` column is value of edge ranking.

#### Vertex

```csv
_vid,tag1.prop1,tag2.prop2,tag1.prop3,tag2.prop4
...
```

`_vid` column represent the global unique vertex id.


### Log

All logs info will output to your `logPath` file in configuration.

## Example

The following example shows you how to import CSV data to **Nebula Graph** with Nebula-importer. In this example, Nebula Graph is installed with `docker` and `docker compose`. You can walk through the example by the following steps:

1. [Start your Nebula Graph services](#starting-nebula-graph-services).
2. [Create the schema for tags and edges](#creating-the-schema-for-tags-and-edges).
3. [Prepare the configuration file](#preparing-the-configuration-file).
4. [Prepare the CSV data](#preparing-the-csv-data).
5. [Import the CSV data](#importing-the-csv-data).

### Starting Nebula Graph Services

You can start your Nebula Graph services by the following steps:

1. On a command line interface, go to the nebula-docker-compose directory.
2. Execute the following command to start nebula services:

```
sudo docker-compose up -d
```

3. Execute the following command to get the port for graphd of **Nebula Graph**:

```
sudo docker-compose ps
```

4. Execute the following command to pull the Nebula Graph image:

```
sudo docker pull vesoft/nebula-console:nightly
```

5. Execute the following command to connect to your Nebula Graph server:

```
sudo docker run --rm -ti --network=host vesoft/nebula-console:nightly --addr=127.0.0.1 --port=32868
```

**Note**: You must make sure the ip and port are correct.

### Creating the Schema for Tags and Edges
Before you can input your schema, you must create a space and use it.
In this example, we create two tags and one edge type with the following commands:

```
CREATE TAG directors (name string, gender string, age int)

CREATE TAG stars (name string, gender string, age int)

CREATE EDGE chose (rating int)
```

### Preparing the Configuration File
You must configure the `.yaml` configuration file, which regulates how data is organized in the CSV files.

In this example, we configure the `.yaml` configuration file as follows:
```
version: v1rc1
description: example
clientSettings:
  concurrency: 4 # number of graph clients
  channelBufferSize: 128
  space: importdata
  connection:
    user: user
    password: password
    address: 127.0.0.1:32778
logPath: ./err/test.log
files:
  - path: /home/nebula/edge.csv
    failDataPath: ./err/edge.csv
    batchSize: 100
    type: csv
    csv:
      withHeader: false
      withLabel: false
    schema:
      type: edge
      edge:
        name: chose
        withRanking: true
        props:
          - name: rating
            type: string
  - path: /home/nebula/directors.csv
    failDataPath: ./err/vertex.csv
    batchSize: 100
    type: csv
    csv:
      withHeader: false
      withLabel: false
    schema:
      type: vertex
      vertex:
        tags:
          - name: directors
            props:
              - name: name
                type: string
                ignore: true
              - name: gender
                type: string
              - name: age
                type: int  
    - path: /home/nebula/stars.csv
    failDataPath: ./err/vertex.csv
    batchSize: 100
    type: csv
    csv:
      withHeader: false
      withLabel: false
    schema:
      type: vertex
      vertex:
        tags:
          - name: directors
            props:
              - name: name
                type: string
                ignore: true
              - name: gender
                type: string
              - name: age
                type: int      

```
**Note**: In the above configuration file, you must change the ip and port to yours.


### Preparing the CSV Data
In this example we prepare three CSV data files, one for the edge and two for vertexes.

The data in the directors CSV file is as follows:
``` 
100,Hanks,male,55
101,Tom,male,81
102,Jim,male,42
103,Tan,male,45
104,Powell,male,64
105,Aval,female,49
106,Joe,female,60
107,Juli,female,43
108,Eve,female,29
``` 
The data in the stars CSV file is as follows:
```
200,Jack,male,17
201,Mike,male,18
202,Michael,male,16
203,Nelson,male,19
204,Kally,female,18
205,Jane,female,17
206,Joy,female,21
207,Tina,female,19
208,Lily,female,20
```
The data in the edge CSV file is as follows:
```
100,200,good
100,201,good
100,202,wonderful
101,203,excellent
101,200,excellent
102,201,good
103,204,fantastic
104,205,good
104,202,wonderful

```
**Note**: In the above files, the first column is the vertex id and the other fields are in consistent with the `.yaml` configuration file.

### Importing the CSV Data
After all the previous four steps are complete, you can import the CSV data by the following command:

```
sudo docker run --rm -ti --network=host -v /home/nebula/config.yaml:/home/nebula/config.yaml -v /home/nebula/:/home/nebula/ vesoft/nebula-importer --config /home/nebula/config.yaml
```
**Note**: You must change the directory for the `.yaml` file to yours, otherwise this command cannot be executed successfully.

## TODO

- [X] Summary statistics of response
- [X] Write error log and data
- [X] Configure file
- [X] Concurrent request to Graph server
- [ ] Create space and tag/edge automatically
- [ ] Configure retry option for Nebula client
- [X] Support edge rank
- [X] Support label for add/delete(+/-) in first column
- [ ] Support column header in first line
- [X] Support vid partition
- [X] Support multi-tags insertion in vertex
- [X] Provide docker image and usage
- [ ] Make header adapt to props order defined in schema of configure file
- [X] Handle string column in nice way
- [ ] Update concurrency and batch size online
- [ ] Count duplicate vids
- [ ] Support VID generation automatically
- [ ] Output logs to file

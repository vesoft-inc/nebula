# Importer

`Importer` is a single-threaded tool used to load small data set from a CSV file into **Nebula Graph**.
As for large data set, generating SST files and then ingesting them may be a better way. For details, please refer to [Spark SST File Generator](../spark-sstfile-generator/README.md) and HDFS Importer. <!--to be done-->

## Download JARs

<!-- to be replaced after Nexus MVN repo being set -->
Our maven repository is under construction. At this moment, get fbthrift and graph-client packages from the OSS Cloud.

```bash
wget https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/jar-packages/thrift-1.0-SNAPSHOT.jar

wget https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/jar-packages/graph-client-1.0.0-beta.jar
```

Or build it by yourself.

```bash
> cd ${YOUR_NEBULA_DIR}
> cd src/client
> mvn clean package
```

`graph-client` jar package is under the target directory.

### Install JARs

```bash
 > mvn install:install-file -Dfile=${YOUR_GRAPH_CLIENT_FILEPATH} -DgroupId=nebula-graph -DartifactId=graph-client -Dversion=${GRAPH_CLIENT_VERSION} -Dpackaging=jar

 > mvn install:install-file -Dfile=${YOUR_FBTHRIFT_FILEPATH} -DgroupId=com.facebook -DartifactId=thrift -Dversion=${FBTHRIFT_VERSION} -Dpackaging=jar
```

Replace "${YOUR_GRAPH_CLIENT_FILEPATH}" and "${YOUR_FBTHRIFT_FILEPATH}", "${GRAPH_CLIENT_VERSION}" and "${FBTHRIFT_VERSION}" with your own values.

### Get Importer

Get the `Importer` jar package from the OSS Cloud:

```bash
wget https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/jar-packages/importer-1.0.0-beta.jar
```

Or build it by yourself.

```bash
> cd ${YOUR_NEBULA_DIR}
> cd src/tools/importer
> mvn clean package
```

The `Importer` jar package is under the target directory.

### Data File

#### For Tags

The `Importer` can only import one tag or edge type each time. For tags, the first column is `vid` and then the property value list, separated by a comma.

```bash
vid,prop_name_1,prop_name_2,...
```

#### For Edge Types

For edge type, the file structure is as following:

```bash
sourceID, destinationID, [ranking], prop_name_1, prop_name_2, ...
```

> The "ranking" column is optional. It works with "--ranking" option, which will be described below.

### Import Data

To import data, run:

```bash
> java -jar importer-1.0.0-beta.jar <OPTIONS>
```

|Property Name  | Abbreviation |  Description| Example |
|:----|:----:|:----:|:----:|
|--address        | -a            | graphd service address| 127.0.0.1:3699 |
|--batch          | -b            | batch insert size|2|
|--name           | -n            | specify the space name| myspace_test2 |
|--schema         | -m            | specify the tag or edge type name| student |
|--column         | -c            | properties of tag or edge to be inserted, separated by a comma | name,age |
|--ranking        | -k            | whether the edge has ranking value. Default is false| true/false|
|--file           | -f            | data file| ./tmp/data.txt |
|--errorPath    | -d            | error log file | ./tmp/error.log |
|--help           | -h            | list help|
|--timeout        | -o            | specify connection timeout, in the units of millisecond| 3000 |
|--pswd           | -p            | graphd service password| password |
|--connectionRetry       | -r            | connection retry times| 3 |
|--executionRetry       | -e           | thrift execution retry times| 3|
|--stat           | -s            | print statistics info| true/false |
|--type           | -t            | indicate to insert vertex properties or edge properties| vertex/edge|
|--user           | -u            | graphd service username| user |

> For edge type, if 'ranking' column is specified, '--ranking' option should be set to `true`.

### Example

The examples are based on [Insert Data Section of get-started.md](../../../docs/manual-EN/1.overview/2.quick-start/1.get-started.md#insert-data).

E1. Insert vertices 200, 201, 202.

data.csv

```csv
200,"Monica",16,"female"
201,"Mike",18,"male"
202,"Jane",17,"female"
```

Example command:

```bash
> java -jar importer-${VERSION}.jar --address 127.0.0.1:3699 --name myspace_test2 --schema student -u user -p password -t vertex --file data.txt --column name,age,gender --batch 2
```

E2. Insert `select` edges.

**Without Ranking**

data.csv

```csv
200,101,5
200,102,5
201,102,3
202,102,3
```

Example command:

```bash
> java -jar importer-${VERSION}.jar --address 127.0.0.1:3699 --name myspace_test2 --schema select -u user -p password -t edge --file data.txt --column grade --batch 2
```

**With Ranking**

data.csv

```csv
200,101,0,5
200,102,0,5
201,102,0,3
202,102,0,3
```

Example command:

```bash
> java -jar importer-${VERSION}.jar --address 127.0.0.1:3699 --name myspace_test2 --schema select -u user -p password -t edge --file data.txt --column grade --batch 2 -k true
```

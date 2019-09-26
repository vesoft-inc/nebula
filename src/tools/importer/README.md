
#### Importer

`Importer` is a tool used to load a small amount of data from a `CSV` file into `Nebula`. It is single-threaded.

### Download JARs

<!-- to be replaced after Nexus MVN repo being set -->
Our maven repository is under construction. At this moment, please contact us to get fbthrift and graph-client package.

Install JARs

```
 > mvn install:install-file -Dfile=$YOUR_GRAPH_CLIENT_FILEPATH -DgroupId=nebula-graph -DartifactId=graph-client -Dversion=$GRAPH_CLIENT_VERSION -Dpackaging=jar

 > mvn install:install-file -Dfile=$YOUR_FBTHRIFT_FILEPATH -DgroupId=com.facebook -DartifactId=thrift -Dversion=$FBTHRIFT_VERSION -Dpackaging=jar
```

Replace `$YOUR_GRAPH_CLIENT_FILEPATH` and `$YOUR_FBTHRIFT_FILEPATH` with your own values. Also `$GRAPH_CLIENT_VERSION` and `$FBTHRIFT_VERSION`.

### Get Importer

At this moment, please contact us to get the importer jar package.

### Data File

#### For Tags

The Impoter can only import one tag or edgetype per time. For tags, the first column is `vid` and then the property value list, separated by a comma. 

```
vid,prop_name_1,prop_name_2,...
```

#### For Edge Types

For edge type, the file structure is as following:

```
sourceID, destinationID, [ranking], prop_name_1, prop_name_2, ...
```

> The `ranking` column is optional. It works with --ranking optional, which will be described below.

### Import Data

To import data, run:

```
> java -jar importer-1.0.0-beta.jar <OPTIONS>
```

|Property Name  | Abbreviation |  Description| Example |
|:----|:----:|:----:|:----:|
|--address        | -a            | graphd service address.| 127.0.0.1:3699 |
|--batch          | -b            | batch insert size.|2|
|--name           | -n            | specify the space name.| myspace_test2 |
|--schema         | -m            | specify the tag or edgetype name.| student |
|--column         | -c            | properties of tag or edge to be inserted, separated by a comma | name,age |
|--ranking        |               | the edge have ranking data. Default is false| true/false|
|--file           | -f            | data file| ./tmp/data.txt |
|--help           | -h            | list help||
|--timeout        | -o            | specify connection timeout, in millisecond| 3000 |
|--pswd           | -p            | graphd service password||
|--retry          | -r            | connection retry times||
|--stat           | -s            | print statistics info| true/false |
|--type           | -t            | indicate to insert vertex properties or edge properties| vertext/edge|
|--user           | -u            | graphd service username||

> For edge type, if 'ranking' column is specified, --ranking options should be set to `true`.

### Example

The examples are based on the [Insert Data Section of get-started.md](../../../docs/get-started.md#insert-data).

E1. Insert vertices 200, 201, 202.

datafile.txt

```
200,"Monica",16,"female"
201,"Mike",18,"male"
202,"Jane",17,"female"
```

under your importer dir run:
```
> java -jar importer-1.0.0-beta.jar "--address 127.0.0.1:3699 --name myspace_test2 --schema student -u user -p password -t vertex --file data.txt --column name,age,gender --batch 2"
```

E2. Insert `select` edges.

**Without Ranking**

datafile.txt

```
200,101,5
200,102,5
201,102,3
202,102,3
```

under your importer dir run:

```
> java -jar importer-1.0.0-beta.jar "--address 127.0.0.1:3699 --name myspace_test2 --schema select -u user -p password -t edge --file data.txt --column grade --batch 2"
```

**With Ranking**

datafile.txt

```
200,101,0,5
200,102,0,5
201,102,0,3
202,102,0,3
```


under your importer dir run:

```
> java -jar importer-1.0.0-beta.jar "--address 127.0.0.1:3699 --name myspace_test2 --schema select -u user -p password -t edge --file data.txt --column grade --batch 2 -k true"
```
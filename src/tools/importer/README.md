
## Importer

`Importer` is a tool used to load `CSV` files into `Nebula`.

### Download JARs

<!-- to be replaced after Nexus MVN repo being set -->
Download fbthrift and graph-client

Install JARs

```
 > mvn install:install-file -Dfile=$your_graph_client_file_path -DgroupId=nebula-graph -DartifactId=graph-client -Dversion=$graph-client-version -Dpackaging=jar

 > mvn install:install-file -Dfile=$your_fbthrift_file_path -DgroupId=com.facebook -DartifactId=thrift -Dversion=$fbthrift-version -Dpackaging=jar
```

Replace `$your_graph_client_file_path` and `$your_fbthrift_file_path` with your own values. Also `$fbthrift-version` and `$graph-client-version`.

### Get Importer

```
git clone https://github.com/vesoft-inc/nebula.git

cd nebula/src/tools/importer
```

To import data, run the command:

```
> ./run.sh ${CONFIG_PARAMETER}
```

When loading a vertex CSV file, the first column is vid and then following the value list, separated by a comma.

The edge csv file's structure look like: 

```
sourceID, destinationID, [ranking], value list.
```

#### Config reference:

|Property Name  | Abbreviation |  Description| Example |
|:----|:----:|:----:|:----:|
|--address        | -a            | graphd service address.| 127.0.0.1:3699 |
|--batch          | -b            | batch insert size.|2|
|--name           | -n            | specify the space name.| myspace_test2 |
|--schema         | -m            | specify the tag or edgetype name.| student |
|--column         | -c            | properties of tag or edge to be inserted, separated by a comma | name,age |
|--ranking        |             | the edge have ranking data. Default is false| true/false|
|--file           | -f            | data file| ./tmp/data.txt |
|--help           | -h            | list help||
|--timeout        | -o            | specify connection timeout, in millisecond| 3000 |
|--pswd           | -p            | graphd service password||
|--retry          | -r            | connection retry times||
|--stat           | -s            | print statistics info| true/false |
|--type           | -t            | indicate to insert vertex properties or edge properties| vertext/edge|
|--user           | -u            | graphd service username||

### Example

The examples are based on [Insert Data Section of get-started.md](../../../docs/get-started.md#insert-data).

E1. Insert vertices 200, 201, 202.

datafile.txt

```
200,"Monica",16,"female"
201,"Mike",18,"male"
202,"Jane",17,"female"
```

under your importer dir run:
```
> ./run.sh "--address 127.0.0.1:3699 --name myspace_test2 --schema student -u user -p password -t vertex --file data.txt --column name,age,gender --batch 2"
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
> ./run.sh "--address 127.0.0.1:3699 --name myspace_test2 --schema select -u user -p password -t edge --file data.txt --column grade --batch 2"
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
> ./run.sh "--address 127.0.0.1:3699 --name myspace_test2 --schema select -u user -p password -t edge --file data.txt --column grade --batch 2 -k true"
```
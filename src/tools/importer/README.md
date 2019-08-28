
#### Importer

`Importer` is a tool use to load `CSV` file into `Nebula`.

You can use `./run.sh ${CONFIG_PARAMETER} ${JAVA_FBTHRIFT_JAR}` to startup it.

When loading vertex csv file, the first field is vid and then following the value list.

The edge csv file's structure look like: 

```
sourceID, destinationID, [ranking], value list.
```

#### Config reference:

Property Name  | Abbreviation |  Description
-------------- | ------------ | -------------
help           | h            | help message.
file           | f            | data file path.
batch          | b            | batch insert size.
type           | t            | data type. vertex or edge.
name           | n            | specify the space name.
schema         | m            | specify the schema name.
column         | c            | vertex and edge's column.
stat           | s            | print statistics info.
address        | a            | thrift service address.
retry          | r            | thrift connection retry number.
timeout        | o            | thrift connection timeout.
user           | u            | thrift service username.
pswd           | p            | thrift service password.
ranking        | k            | the edge have ranking data.


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
address        | a            | thrift service address.
batch          | b            | batch insert size.
column         | c            | vertex and edge's column.
file           | f            | data file path.
help           | h            | help message.
ranking        | k            | the edge have ranking data.
schema         | m            | specify the schema name.
name           | n            | specify the space name.
timeout        | o            | thrift connection timeout.
pswd           | p            | thrift service password.
retry          | r            | thrift connection retry number.
stat           | s            | print statistics info.
type           | t            | data type. vertex or edge.
user           | u            | thrift service username.

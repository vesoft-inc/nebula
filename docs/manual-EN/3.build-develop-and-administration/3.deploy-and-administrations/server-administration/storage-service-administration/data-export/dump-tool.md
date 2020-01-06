# Dump Tool

Dump Tool is an off-line data dumping tool that can be used to dump or count data with specified conditions.

## How to Get

The source code of the dump tool is under `nebula/src/tools/db_dump`. You can use command `make db_dump` to compile it. Since the tool dumps data by opening the RockDB, you need to use it on the machines that have the storage service deployed and make sure the meta_server is started. Please refer to the following section on detailed usage.

## How to Use

The `db_dump` command displays information about how to use the dump tool. Parameter `space` is required. Parameters `db_path` and `meta_server` both have default values and you can configure them based on your actual situation. You can combine parameters `vids`, `parts`, `tags` and `edges` arbitrarily to dump the data you want.

```bash
  ./db_dump --space=<space name>

required:
       --space=<space name>
        # A space name must be given.

optional:
       --db_path=<path to rocksdb>
         # Path to the RocksDB data directory. If nebula was installed in `/usr/local/nebula`,
         # the db_path would be /usr/local/nebula/data/storage/nebula/
         # Default: ./

       --meta_server=<ip:port,...>
         # A list of meta severs' ip:port separated by comma.
         # Default: 127.0.0.1:45500

       --mode= scan | stat
         # scan: print to screen when records meet the condition, and also print statistics to screen in final.
         # stat: print statistics to screen.
         # Default: scan

       --vids=<list of vid>
         # A list of vids separated by comma. This parameter means vertex_id/edge_src_id
         # Would scan the whole space's records if it is not given.

       --parts=<list of partition id>
         # A list of partition ids separated by comma.
         # Would output all partitions if it is not given.

       --tags=<list of tag name>
         # A list of tag name separated by comma.

       --edges=<list of edge name>
         # A list of edge name separated by comma.

       --limit=<N>
         # A positive number that limits the output.
         # Would output all if set to 0 or negative.
         # Default: 1000
```

Following is an example:

```bash
// Specify a space to dump data
./db_dump --space=space_name

// Specify space, db_path, meta_server
./db_dump --space=space_name --db_path=/usr/local/nebula/data/storage/nebula/ --meta_server=127.0.0.1:45513

// Set mode to stat, only stats are returned, no data is printed
./db_dump --space=space_name --mode=stat --db_path=/usr/local/nebula/data/storage/nebula/ --meta_server=127.0.0.1:45513

// Specify vid to dump the vertex and the edges sourcing from it
./db_dump --space=space_name --mode=stat --db_path=/usr/local/nebula/data/storage/nebula/ --meta_server=127.0.0.1:45513 --vids=123,456

// Specify tag and dump vertices with the tag 
./db_dump --space=space_name --mode=stat --db_path=/usr/local/nebula/data/storage/nebula/ --meta_server=127.0.0.1:45513 --tags=tag1,tag2

```

The returned data format:

```bash
// vertices, key: part_id, vertex_id, tag_name, value: <prop_list>
[vertex] key: 1, 0, poi value:mid:0,8191765721868409651,8025713627522363385,1993089399535188613,3926276052777355165,5123607763506443893,2990089379644866415,poi_name_0,上海,华东,30.2824,120.016,poi_stat_0,poi_fc_0,poi_sc_0,0,poi_star_0,

// edges, key: part_id, src_id, edge_name, ranking, dst_id, value: <prop_list>
[edge] key: 1, 0, consume_poi_reverse, 0, 656384 value:mid:656384,mid:0,7.19312,mid:656384,3897457441682646732,mun:656384,4038264117233984707,dun:656384,empe:656384,mobile:656384,gender:656384,age:656384,rs:656384,fpd:656384,0.75313,1.34433,fpd:656384,0.03567,7.56212,

// stats
=======================================================
COUNT: 10           # total number of data dumped
VERTEX COUNT: 1     # number of vertices dumped
EDGE COUNT: 9       # number of edges dumped
TAG STATISTICS:     # number of tags dumped
        poi : 1
EDGE STATISTICS:    # number of edge types dumped
        consume_poi_reverse : 9
=======================================================
```

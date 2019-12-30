# Dump Tool

Dump Tool 是一个离线数据导出工具，可以用于导出或统计指定条件的数据。

## 如何获得

Dump Tool 源码位于 `nebula/src/tools/db_dump` 下，用户可以执行 `make db_dump` 命令来编译生成该工具。该工具通过直接打开 RocksDB 转储数据，因此需要在部署 storage 服务的机器上使用，同时需要保证 meta_server 已启动。具体用法请参考下方说明。

## 如何使用

具体用法如下所示，用户可以通过执行不带参数的 `db_dump` 命令获得帮助。其中 `space` 参数是必须的，而 `db_pat`h 以及 `meta_server` 具有默认值，用户可以按照实际配置。`vids`、`parts`、`tags`、`edges` 可以任意组合，导出你需要的数据。

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

下面是一些示例：

```bash
// 指定 space 导出数据
./db_dump --space=space_name

// 指定space, db_path, meta_server
./db_dump --space=space_name --db_path=/usr/local/nebula/data/storage/nebula/ --meta_server=127.0.0.1:45513

// 指定 mode=stat(统计模式)，此时只返回统计信息，不打印数据
./db_dump --space=space_name --mode=stat --db_path=/usr/local/nebula/data/storage/nebula/ --meta_server=127.0.0.1:45513

// 指定 vid 导出该点以及以该点为起始点的边
./db_dump --space=space_name --mode=stat --db_path=/usr/local/nebula/data/storage/nebula/ --meta_server=127.0.0.1:45513 --vids=123,456

// 指定 tag 类型，导出具有该 tag 的点
./db_dump --space=space_name --mode=stat --db_path=/usr/local/nebula/data/storage/nebula/ --meta_server=127.0.0.1:45513 --tags=tag1,tag2

```

返回的数据格式：

```bash
// 点，key: part_id, vertex_id, tag_name, value: <prop_list>
[vertex] key: 1, 0, poi value:mid:0,8191765721868409651,8025713627522363385,1993089399535188613,3926276052777355165,5123607763506443893,2990089379644866415,poi_name_0,上海,华东,30.2824,120.016,poi_stat_0,poi_fc_0,poi_sc_0,0,poi_star_0,

// 边, key: part_id, src_id, edge_name, ranking, dst_id, value: <prop_list>
[edge] key: 1, 0, consume_poi_reverse, 0, 656384 value:mid:656384,mid:0,7.19312,mid:656384,3897457441682646732,mun:656384,4038264117233984707,dun:656384,empe:656384,mobile:656384,gender:656384,age:656384,rs:656384,fpd:656384,0.75313,1.34433,fpd:656384,0.03567,7.56212,

// 统计
=======================================================
COUNT: 10           #本次导出记录数量
VERTEX COUNT: 1     #本次导出点数量
EDGE COUNT: 9       #本次导出边数量
TAG STATISTICS:     #本次导出tag统计
        poi : 1
EDGE STATISTICS:    #本次导出edge统计
        consume_poi_reverse : 9
=======================================================
```

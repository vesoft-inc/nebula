/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"

void printHelp() {
    fprintf(stderr,
           R"(  db_dump --db=<path to rocksdb> --meta_seve=<ip:port,...>

       --db=<path to rocksdb>
       Path to the rocksdb data directory. Default: ./

       --meta_server=<ip:port,...>
         A list of meta severs' ip:port seperated by comma, if there exists.
         Default: 127.0.0.1:45500

       optional:
       --part=<list of partition id>
         A list of partition id seperated by comma.
         Would output all patitions if it is not given,

       --scan=vertex | edge | all
         Vertex or edge would output if set. Default: all.

       --tag=<list of tags>
         Filtering with given tags.

       --edge=<list of edges>
         Filtering with given edges.

       --limit=<N>
         Limit to output.

       --output=console | csv
         Output to the concole, default: console.
         csv is not supported yet.
)");
}

int main(int argc, char *argv[]) {
    UNUSED(argc);
    UNUSED(argv);
    printHelp();
}


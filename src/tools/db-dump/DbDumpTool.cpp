/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "tools/db-dump/DbDumper.h"

void printHelp() {
    fprintf(stderr,
           R"(  ./db_dump --space_name=<space name>

required:
       --space_name=<space name>
         A space name must be given.

optional:
       --db_path=<path to rocksdb>
         Path to the rocksdb data directory. If nebula was installed in /usr/local/nebula,
         the db_path would be /usr/local/nebula/data/storage/nebula/
         Default: ./

       --meta_server=<ip:port,...>
         A list of meta severs' ip:port seperated by comma.
         Default: 127.0.0.1:45500

       --mode= scan | stat
         scan: print to screen when records meet the condition, and also print statistics
               to screen in final.
         stat: print statistics to screen.
         Defualt: scan

       --vids=<list of vid>
         A list of vid seperated by comma. This parameter means vertex_id/edge_src_id
         Would scan the whole space's records if it is not given.

       --parts=<list of partition id>
         A list of partition id seperated by comma.
         Would output all patitions if it is not given.

       --tags=<list of tag name>
         A list of tag name seperated by comma.

       --edges=<list of edge name>
         A list of edge name seperated by comma.

       --limit=<N>
         A positive number that limits the output.
         Would output all if set 0 or negative number.
         Default: 1000


)");
}

void printParams() {
    std::cout << "===========================PARAMS============================\n";
    std::cout << "mode: " << FLAGS_mode << "\n";
    std::cout << "meta server: " << FLAGS_meta_server << "\n";
    std::cout << "space name: " << FLAGS_space_name << "\n";
    std::cout << "path: " << FLAGS_db_path << "\n";
    std::cout << "parts: " << FLAGS_parts << "\n";
    std::cout << "vids: " << FLAGS_vids << "\n";
    std::cout << "tags: " << FLAGS_tags << "\n";
    std::cout << "edges: " << FLAGS_edges << "\n";
    std::cout << "limit: " << FLAGS_limit << "\n";
    std::cout << "===========================PARAMS============================\n\n";
}

int main(int argc, char *argv[]) {
    if (argc == 1) {
        printHelp();
        return EXIT_FAILURE;
    } else {
        folly::init(&argc, &argv, true);
    }

    google::SetStderrLogging(google::FATAL);

    printParams();

    nebula::storage::DbDumper dumper;
    auto status = dumper.init();
    if (!status.ok()) {
      std::cerr << "Error: " << status << "\n\n";
      return EXIT_FAILURE;
    }
    dumper.run();
}


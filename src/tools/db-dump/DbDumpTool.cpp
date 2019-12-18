/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "DbDumper.h"

void printHelp() {
    fprintf(stderr,
           R"(  db_dump --db_path=<path to rocksdb> --meta_seve=<ip:port,...>

       --db_path=<path to rocksdb>
        Path to the rocksdb data directory. Default: ./

       --meta_server=<ip:port,...>
         A list of meta severs' ip:port seperated by comma, if there exists.
         Default: 127.0.0.1:45500

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
)");
}

int main(int argc, char *argv[]) {
    if (argc == 1) {
        printHelp();
        return EXIT_FAILURE;
    } else {
        folly::init(&argc, &argv, true);
    }

    google::SetStderrLogging(google::FATAL);

    nebula::storage::DbDumper dumper;
    auto status = dumper.init();
    if (!status.ok()) {
      std::cerr << status << "\n\n";
      return EXIT_FAILURE;
    }
    dumper.run();
}


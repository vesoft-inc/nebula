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

void printParams() {
    std::cout << "=======================================================\n";
    std::cout << "mode: " << FLAGS_mode << "\n";
    std::cout << "seta server: " << FLAGS_meta_server << "\n";
    std::cout << "space: " << FLAGS_space << "\n";
    std::cout << "path: " << FLAGS_db_path << "\n";
    std::cout << "parts: " << FLAGS_parts << "\n";
    std::cout << "vids: " << FLAGS_vids << "\n";
    std::cout << "tags: " << FLAGS_tags << "\n";
    std::cout << "edges: " << FLAGS_edges << "\n";
    std::cout << "limit: " << FLAGS_limit << "\n";
    std::cout << "=======================================================\n";
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


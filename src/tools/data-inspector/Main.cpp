/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/base/Base.h"
#include "tools/data-inspector/Inspector.h"

void printHelp() {
  fprintf(stderr, R"(
  ./data-inspector [info|stats|dump] options

Required options:
        --db_path=<Path to RocksDB instance>
          Which database to inspect

Optional options:
        Please use --help to see all possible options

Prefix
        Prefix is used in the "dump" command. It is an optional string specified by
        --prefix="". The valus is a set of key/value pairs, separated by semicolon.
        A colon is used to separate the key and value. i.e
          --prefix="k1:v1;k2:v2;k3:v3;.."

        The following are supported keys:

        type  (int)     -- Nebula key types. In version 2.6, it can only be one
                           of [1, 6]. Based on the type, not all keys will be used
        part  (int)     -- Partition number
        idlen (int)     -- The length of the vertex id
        vid   (str/hex) -- vid is a string, which could start with "\\x". If it
                           starts with "\x", all following string will be treated
                           as a HEX string. Otherwise, it will be treated as a
                           normal string
        dst             -- Same as vid
        tag   (int)     -- Nebula Tag id
        edge  (int)     -- Edge type
        rank  (int)     -- Rank on the edge
)");
}


int main(int argc, char *argv[]) {
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::FATAL);

  if (argc != 2) {
    printHelp();
    return 0;
  }

  auto inspector = nebula::Inspector::getInspector();
  if (!inspector) {
    return -1;
  }

  if (strcmp(argv[1], "info") == 0) {
    // Info
    inspector->info();
  } else if (strcmp(argv[1], "stats") == 0) {
    // stats
    inspector->stats();
  } else if (strcmp(argv[1], "dump") == 0) {
    // dump
    inspector->dump();
  } else {
    LOG(ERROR) << "Unknown command: " << argv[1];
  }

  return 0;
}

/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "DbDumper.h"

DEFINE_string(db, "./", "Path to rocksdb.");
DEFINE_string(meta_server, "127.0.0.1:45500", "Meta servers' address.");
DEFINE_string(part, "", "A list of partition id seperated by comma.");
DEFINE_string(scan, "all", "Scan all/vertex/edge.");
DEFINE_string(tag, "", "Filtering with given tags.");
DEFINE_string(edge, "", "Filtering with given edges.");
DEFINE_int64(limit, -1, "Limit to output.");
DEFINE_string(output, "console", "Output to the console or csv.");

namespace nebula {
namespace storage {

}  // namespace storage
}  // namespace nebula

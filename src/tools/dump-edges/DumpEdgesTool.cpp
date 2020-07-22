/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "utils/NebulaKeyUtils.h"
#include <rocksdb/db.h>

DEFINE_string(path, "", "rocksdb instance path");
DEFINE_int64(vertex_id, 0, "Specify the vertex id");
DEFINE_int64(parts_num, 100, "Specify the parts number");

namespace nebula {

class DumpEdges {
public:
    static void scan(const char* path) {
        LOG(INFO) << "open rocksdb on " << path;
        rocksdb::DB* db = nullptr;
        rocksdb::Options options;
        auto status = rocksdb::DB::OpenForReadOnly(options, path, &db);
        CHECK(status.ok()) << status.ToString();
        rocksdb::ReadOptions roptions;
        rocksdb::Iterator* iter = db->NewIterator(roptions);
        if (!iter) {
            LOG(FATAL) << "null iterator!";
        }
        if (FLAGS_vertex_id != 0) {
            auto partId = FLAGS_vertex_id % FLAGS_parts_num + 1;
            auto prefix = NebulaKeyUtils::edgePrefix(partId, FLAGS_vertex_id);
            iter->Seek(prefix);
        } else {
            iter->SeekToFirst();
        }
        size_t count = 0;
        while (iter->Valid()) {
            auto key = folly::StringPiece(iter->key().data(), iter->key().size());
            if (NebulaKeyUtils::isEdge(key)) {
                LOG(INFO) << NebulaKeyUtils::getSrcId(key) << "," << NebulaKeyUtils::getDstId(key);
                count++;
            }
            iter->Next();
        }
        LOG(INFO) << "Total edges:" << count;
        if (iter) {
            delete iter;
        }

        if (db) {
            db->Close();
            delete db;
        }
    }
};

}  // namespace nebula

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    LOG(INFO) << "path:" << FLAGS_path
              << ", vertex_id:" << FLAGS_vertex_id
              << ", parts_num:" << FLAGS_parts_num;
    if (FLAGS_path.empty()) {
        LOG(INFO) << "Specify the path!";
        return -1;
    }
    nebula::DumpEdges::scan(FLAGS_path.c_str());
    return 0;
}

/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/clients/meta/MetaClient.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "utils/NebulaKeyUtils.h"
#include <rocksdb/db.h>

DEFINE_string(meta_server, "127.0.0.1:45500", "Meta servers' address.");
DEFINE_string(path, "", "rocksdb instance path");
DEFINE_string(space_name, "", "The space name.");
DEFINE_string(vertex_id, "", "Specify the vertex id");
DEFINE_bool(print_hex_edge_id, false, "Print edge src & dst in hex");

namespace nebula {
namespace storage {

std::string hexEdgeId(size_t vIdLen, folly::StringPiece key) {
    return folly::hexlify(NebulaKeyUtils::getSrcId(vIdLen, key)) +
           folly::hexlify(NebulaKeyUtils::getDstId(vIdLen, key));
}

class DumpEdges {
public:
    Status init() {
        auto status = initMeta();
        if (!status.ok()) {
            return status;
        }

        status = initSpace();
        if (!status.ok()) {
            return status;
        }
        return Status::OK();
    }

    Status initMeta() {
        auto addrs = network::NetworkUtils::toHosts(FLAGS_meta_server);
        if (!addrs.ok()) {
            return addrs.status();
        }

        auto ioExecutor = std::make_shared<folly::IOThreadPoolExecutor>(1);
        meta::MetaClientOptions options;
        options.skipConfig_ = true;
        metaClient_ = std::make_unique<meta::MetaClient>(ioExecutor,
                                                         std::move(addrs.value()),
                                                         options);
        if (!metaClient_->waitForMetadReady(1)) {
            return Status::Error("Meta is not ready: '%s'.", FLAGS_meta_server.c_str());
        }
        schemaMng_ = std::make_unique<meta::ServerBasedSchemaManager>();
        schemaMng_->init(metaClient_.get());
        return Status::OK();
    }

    Status initSpace() {
        if (FLAGS_space_name.empty()) {
            return Status::Error("Space name is not given.");
        }
        auto space = schemaMng_->toGraphSpaceID(FLAGS_space_name);
        if (!space.ok()) {
            return Status::Error("Space '%s' not found in meta server.", FLAGS_space_name.c_str());
        }
        spaceId_ = space.value();

        auto spaceVidLen = metaClient_->getSpaceVidLen(spaceId_);
        if (!spaceVidLen.ok()) {
            return spaceVidLen.status();
        }
        spaceVidLen_ = spaceVidLen.value();

        auto partNum = metaClient_->partsNum(spaceId_);
        if (!partNum.ok()) {
            return Status::Error("Get partition number from '%s' failed.",
                                 FLAGS_space_name.c_str());
        }
        partNum_ = partNum.value();
        return Status::OK();
    }

    void scan(const char* path) {
        LOG(INFO) << "open rocksdb on " << path;
        rocksdb::DB* db = nullptr;
        rocksdb::Options options;
        auto status = rocksdb::DB::OpenForReadOnly(options, path, &db);
        CHECK(status.ok()) << status.ToString();
        rocksdb::ReadOptions roptions;
        rocksdb::Iterator* iter = db->NewIterator(roptions);
        if (!iter) {
            LOG(FATAL) << "Null iterator!";
        }
        if (!FLAGS_vertex_id.empty()) {
            if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, FLAGS_vertex_id)) {
                LOG(FATAL) << "Vertex id length is illegal.";
            }
            auto partId = std::hash<VertexID>()(FLAGS_vertex_id) % partNum_  + 1;
            auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen_, partId, FLAGS_vertex_id);
            iter->Seek(prefix);
        } else {
            iter->SeekToFirst();
        }
        size_t count = 0;
        while (iter->Valid()) {
            auto key = folly::StringPiece(iter->key().data(), iter->key().size());
            if (NebulaKeyUtils::isEdge(spaceVidLen_, key)) {
                LOG(INFO) << NebulaKeyUtils::getSrcId(spaceVidLen_, key) << ","
                          << NebulaKeyUtils::getDstId(spaceVidLen_, key);
                LOG_IF(INFO, FLAGS_print_hex_edge_id) << hexEdgeId(spaceVidLen_, key);
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

private:
    std::unique_ptr<meta::MetaClient>                              metaClient_;
    std::unique_ptr<meta::ServerBasedSchemaManager>                schemaMng_;
    GraphSpaceID                                                   spaceId_;
    int32_t                                                        spaceVidLen_;
    int32_t                                                        partNum_;
};

}  // namespace storage
}  // namespace nebula

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    LOG(INFO) << "path: " << FLAGS_path
              << "space name: " << FLAGS_space_name
              << ", vertex_id: " << FLAGS_vertex_id;
    if (FLAGS_path.empty() || FLAGS_space_name.empty()) {
        LOG(INFO) << "Specify the path, space name!";
        return -1;
    }
    nebula::storage::DumpEdges dumper;
    auto status = dumper.init();
    if (!status.ok()) {
      std::cerr << "Error: " << status << "\n\n";
      return EXIT_FAILURE;
    }
    dumper.scan(FLAGS_path.c_str());
    return 0;
}

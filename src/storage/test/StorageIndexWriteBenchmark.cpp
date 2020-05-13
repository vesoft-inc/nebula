/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/KVStore.h"
#include "kvstore/NebulaStore.h"
#include "fs/FileUtils.h"
#include "storage/client/StorageClient.h"
#include "meta/ServerBasedSchemaManager.h"
#include "dataman/ResultSchemaProvider.h"
#include "storage/test/AdHocSchemaManager.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "dataman/RowWriter.h"
#include <folly/Benchmark.h>
#include "storage/test/TestUtils.h"
#include "storage/StorageFlags.h"


DEFINE_int32(bulk_insert_size, 1000, "The number of vertices by bulk insert");
DEFINE_int32(total_vertices_size, 1000, "The number of vertices");
DEFINE_string(root_data_path, "/tmp/IndexWritePref", "Engine data path");

namespace nebula {
namespace storage {

std::vector<storage::cpp2::Vertex> genVertices(VertexID &vId, TagID tagId) {
    std::vector<storage::cpp2::Vertex> vertices;
    for (auto i = 0; i < FLAGS_bulk_insert_size; i++) {
        storage::cpp2::Vertex v;
        v.set_id(++vId);
        decltype(v.tags) tags;
        storage::cpp2::Tag tag;
        tag.set_tag_id(tagId);
        auto props = TestUtils::setupEncode();
        tag.set_props(std::move(props));
        tags.emplace_back(std::move(tag));
        v.set_tags(std::move(tags));
        vertices.emplace_back(std::move(v));
    }
    return vertices;
}

bool processVertices(kvstore::KVStore* kv,
                     meta::SchemaManager* schemaMan,
                     meta::IndexManager* indexMan,
                     VertexID &vId) {
    cpp2::AddVerticesRequest req;
    BENCHMARK_SUSPEND {
        req.space_id = 0;
        req.overwritable = true;
        std::vector<cpp2::Vertex> vertices;
        auto v = genVertices(vId, 3001);
        vertices.insert(vertices.end(), v.begin(), v.end());
        req.parts.emplace(0, std::move(vertices));
    };

    auto* processor = AddVerticesProcessor::instance(kv, schemaMan, indexMan, nullptr);
    auto fut = processor->getFuture();
    processor->process(std::move(req));
    auto resp = std::move(fut).get();
    BENCHMARK_SUSPEND {
        if (resp.result.failed_codes.size() > 0) {
            return false;
        }
    };
    return true;
}

void insertVertices(bool withoutIndex) {
    std::unique_ptr<kvstore::KVStore> kv;
    std::unique_ptr<meta::SchemaManager> schemaMan;
    std::unique_ptr<meta::IndexManager> indexMan;
    VertexID vId = 0;
    BENCHMARK_SUSPEND {
        std::string rootPath;
        if (withoutIndex) {
            rootPath = folly::stringPrintf("%s/%s",
                                           FLAGS_root_data_path.c_str(),
                                           "withoutIndex");
        } else {
            rootPath = folly::stringPrintf("%s/%s",
                                           FLAGS_root_data_path.c_str(),
                                           "attachIndex");
        }
        kv = TestUtils::initKV(std::move(rootPath).c_str());
        if (withoutIndex) {
            indexMan = std::make_unique<nebula::storage::AdHocIndexManager>();
            schemaMan = TestUtils::mockSchemaMan();
        } else {
            indexMan = TestUtils::mockIndexMan(0, 3001, 3002, 0, 0);
            schemaMan = TestUtils::mockSchemaMan();
        }
    };
    while (vId < FLAGS_total_vertices_size) {
        if (!processVertices(kv.get(), schemaMan.get(), indexMan.get(), vId)) {
            LOG(ERROR) << "Vertices bulk insert error";
            return;
        }
    }
    BENCHMARK_SUSPEND {
        if (withoutIndex) {
            auto prefix = NebulaKeyUtils::prefix(0);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto status = kv->prefix(0, 0, prefix, &iter);
            if (status != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Index scan error";
                return;
            }
            int32_t cnt = 0;
            while (iter->valid()) {
                cnt++;
                iter->next();
            }
            if (cnt != FLAGS_total_vertices_size) {
                LOG(ERROR) << "Vertices insert error , expected : "
                           << FLAGS_total_vertices_size
                           << "actual : " << cnt;
            }
        } else {
            {
                auto prefix = NebulaKeyUtils::prefix(0);
                std::unique_ptr<kvstore::KVIterator> iter;
                auto status = kv->prefix(0, 0, prefix, &iter);
                if (status != kvstore::ResultCode::SUCCEEDED) {
                    LOG(ERROR) << "Index scan error";
                    return;
                }
                int32_t cnt = 0;
                while (iter->valid()) {
                    cnt++;
                    iter->next();
                }
                if (cnt != FLAGS_total_vertices_size) {
                    LOG(ERROR) << "Vertices insert error , expected : "
                               << FLAGS_total_vertices_size
                               << "actual : " << cnt;
                    return;
                }
            }
            {
                auto prefix = NebulaKeyUtils::indexPrefix(0, 4001);
                std::unique_ptr<kvstore::KVIterator> iter;
                auto status = kv->prefix(0, 0, prefix, &iter);
                if (status != kvstore::ResultCode::SUCCEEDED) {
                    LOG(ERROR) << "Index scan error";
                    return;
                }
                int32_t cnt = 0;
                while (iter->valid()) {
                    cnt++;
                    iter->next();
                }
                if (cnt != FLAGS_total_vertices_size) {
                    LOG(ERROR) << "Indexes number error , expected : "
                               << FLAGS_total_vertices_size
                               << "actual : " << cnt;
                    return;
                }
            }
        }
    };
    BENCHMARK_SUSPEND {
        schemaMan.release();
        kv.release();
        fs::FileUtils::remove(FLAGS_root_data_path.c_str(), true);
    };
}

void insertUnmatchIndex() {
    std::unique_ptr<kvstore::KVStore> kv;
    std::unique_ptr<meta::SchemaManager> schemaMan;
    std::unique_ptr<meta::IndexManager> indexMan;
    VertexID vId = 0;
    BENCHMARK_SUSPEND {
        auto rootPath = folly::stringPrintf("%s/%s", FLAGS_root_data_path.c_str(), "unmatchIndex");
        kv = TestUtils::initKV(std::move(rootPath).c_str());
        indexMan = TestUtils::mockIndexMan(0, 2001, 2002);
        schemaMan = TestUtils::mockSchemaMan(0);
    };
    while (vId < FLAGS_total_vertices_size) {
        if (!processVertices(kv.get(), schemaMan.get(), indexMan.get(), vId)) {
            LOG(ERROR) << "Vertices bulk insert error";
            return;
        }
    }
    BENCHMARK_SUSPEND {
        {
            auto prefix = NebulaKeyUtils::prefix(0);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto status = kv->prefix(0, 0, prefix, &iter);
            if (status != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Index scan error";
                return;
            }
            int32_t cnt = 0;
            while (iter->valid()) {
                cnt++;
                iter->next();
            }
            if (cnt != FLAGS_total_vertices_size) {
                LOG(ERROR) << "Vertices insert error , expected : "
                           << FLAGS_total_vertices_size
                           << "actual : " << cnt;
                return;
            }
        }
        {
            auto prefix = NebulaKeyUtils::indexPrefix(0, 1);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto status = kv->prefix(0, 0, prefix, &iter);
            if (status != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Index scan error";
                return;
            }
            int32_t cnt = 0;
            while (iter->valid()) {
                cnt++;
                iter->next();
            }
            if (cnt != 0) {
                LOG(ERROR) << "Indexes number error , expected : "
                           << FLAGS_total_vertices_size
                           << "actual : " << cnt;
                return;
            }
        }
    };
    BENCHMARK_SUSPEND {
        schemaMan.release();
        kv.release();
        fs::FileUtils::remove(FLAGS_root_data_path.c_str(), true);
    };
}

void insertDupVertices() {
    std::unique_ptr<kvstore::KVStore> kv;
    std::unique_ptr<meta::SchemaManager> schemaMan;
    std::unique_ptr<meta::IndexManager> indexMan;
    VertexID vId = 0;
    BENCHMARK_SUSPEND {
        auto rootPath = folly::stringPrintf("%s/%s",
                                            FLAGS_root_data_path.c_str(),
                                            "duplicateIndex");
        kv = TestUtils::initKV(std::move(rootPath).c_str());
        schemaMan = TestUtils::mockSchemaMan();
        indexMan = TestUtils::mockIndexMan(0, 3001, 3002, 0, 0);
    };

    while (vId < FLAGS_total_vertices_size) {
        if (!processVertices(kv.get(), schemaMan.get(), indexMan.get(), vId)) {
            LOG(ERROR) << "Vertices bulk insert error";
            return;
        }
    }

    // insert new version vertices.
    vId = 0;
    while (vId < FLAGS_total_vertices_size) {
        if (!processVertices(kv.get(), schemaMan.get(), indexMan.get(), vId)) {
            LOG(ERROR) << "Vertices bulk insert error";
            return;
        }
    }

    BENCHMARK_SUSPEND {
        {
            auto prefix = NebulaKeyUtils::prefix(0);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto status = kv->prefix(0, 0, prefix, &iter);
            if (status != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Index scan error";
                return;
            }
            int32_t cnt = 0;
            while (iter->valid()) {
                cnt++;
                iter->next();
            }
            if (cnt != FLAGS_total_vertices_size * 2) {
                LOG(ERROR) << "Vertices insert error , expected : "
                           << FLAGS_total_vertices_size * 2
                           << "actual : " << cnt;
                return;
            }
        }
        {
            auto prefix = NebulaKeyUtils::indexPrefix(0, 4001);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto status = kv->prefix(0, 0, prefix, &iter);
            if (status != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Index scan error";
                return;
            }
            int32_t cnt = 0;
            while (iter->valid()) {
                cnt++;
                iter->next();
            }
            if (cnt != FLAGS_total_vertices_size) {
                LOG(ERROR) << "Indexes number error , expected : "
                           << FLAGS_total_vertices_size
                           << "actual : " << cnt;
                return;
            }
        }
    };
    BENCHMARK_SUSPEND {
        schemaMan.release();
        kv.release();
        fs::FileUtils::remove(FLAGS_root_data_path.c_str(), true);
    };
}

void insertVerticesMultIndex() {
    std::unique_ptr<kvstore::KVStore> kv;
    std::unique_ptr<meta::SchemaManager> schemaMan;
    std::unique_ptr<meta::IndexManager> indexMan;
    VertexID vId = 0;
    BENCHMARK_SUSPEND {
        std::string rootPath;
        rootPath = folly::stringPrintf("%s/%s",
                                       FLAGS_root_data_path.c_str(),
                                       "multIndex");

        kv = TestUtils::initKV(std::move(rootPath).c_str());
        indexMan = TestUtils::mockMultiIndexMan(0, 3001, 3004, 0, 0);
        schemaMan = TestUtils::mockSchemaMan(0);
    };
    while (vId < FLAGS_total_vertices_size) {
        if (!processVertices(kv.get(), schemaMan.get(), indexMan.get(), vId)) {
            LOG(ERROR) << "Vertices bulk insert error";
            return;
        }
    }

    BENCHMARK_SUSPEND {
        {
            auto prefix = NebulaKeyUtils::prefix(0);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto status = kv->prefix(0, 0, prefix, &iter);
            if (status != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Index scan error";
                return;
            }
            int32_t cnt = 0;
            while (iter->valid()) {
                cnt++;
                iter->next();
            }
            if (cnt != FLAGS_total_vertices_size) {
                LOG(ERROR) << "Vertices insert error , expected : "
                           << FLAGS_total_vertices_size
                           << "actual : " << cnt;
                return;
            }
        }
        {
            PartitionID partId = 0;
            PartitionID item = (partId << 8) | static_cast<uint32_t>(NebulaKeyType::kIndex);
            std::string prefix;
            prefix.reserve(sizeof(PartitionID));
            prefix.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
            std::unique_ptr<kvstore::KVIterator> iter;
            auto status = kv->prefix(0, 0, prefix, &iter);
            if (status != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Index scan error";
                return;
            }
            int32_t cnt = 0;
            while (iter->valid()) {
                cnt++;
                iter->next();
            }
            if (cnt != FLAGS_total_vertices_size * 3) {
                LOG(ERROR) << "Indexes number error , expected : "
                           << FLAGS_total_vertices_size * 3
                           << "actual : " << cnt;
                return;
            }
        }
    };
    BENCHMARK_SUSPEND {
        schemaMan.release();
        kv.release();
        fs::FileUtils::remove(FLAGS_root_data_path.c_str(), true);
    };
}

BENCHMARK(withoutIndex) {
    insertVertices(true);
}

BENCHMARK(unmatchIndex) {
    insertUnmatchIndex();
}


BENCHMARK(attachIndex) {
    insertVertices(false);
}

BENCHMARK(duplicateVerticesIndex) {
    insertDupVertices();
}

BENCHMARK(multipleIndex) {
    insertVerticesMultIndex();
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    folly::runBenchmarks();
    return 0;
}

/**
withoutIndex: Without index, and doesn't through way asyncAtomicOp.
unmatchIndex: Without match index, and through asyncAtomicOp.
attachIndex: One index, the index contains all the columns of tag.
duplicateVerticesIndex: One index, and insert deplicate vertices.
multipleIndex: Three indexes by one tag.


--total_vertices_size=2000000
============================================================================
src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                  3.00s  333.45m
unmatchIndex                                                  4.72s  212.02m
attachIndex                                                  40.56s   24.66m
duplicateVerticesIndex                                       46.14s   21.67m
multipleIndex                                               1.19min   14.00m
============================================================================

--total_vertices_size=1000000
============================================================================
src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                  1.49s  672.17m
unmatchIndex                                                  2.40s  415.88m
attachIndex                                                  19.57s   51.11m
duplicateVerticesIndex                                       21.22s   47.12m
multipleIndex                                                33.55s   29.81m
============================================================================

--total_vertices_size=100000
============================================================================
src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                               136.06ms     7.35
unmatchIndex                                               207.66ms     4.82
attachIndex                                                   1.53s  651.66m
duplicateVerticesIndex                                        1.55s  647.09m
multipleIndex                                                 2.41s  414.81m
============================================================================

--total_vertices_size=10000
============================================================================
src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                13.07ms    76.50
unmatchIndex                                                20.58ms    48.59
attachIndex                                                148.69ms     6.73
duplicateVerticesIndex                                     140.48ms     7.12
multipleIndex                                              214.98ms     4.65
============================================================================

--total_vertices_size=1000
============================================================================
src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                 1.42ms   702.73
unmatchIndex                                                 3.00ms   333.57
attachIndex                                                 13.92ms    71.85
duplicateVerticesIndex                                      13.94ms    71.72
multipleIndex                                               20.45ms    48.91
============================================================================
**/

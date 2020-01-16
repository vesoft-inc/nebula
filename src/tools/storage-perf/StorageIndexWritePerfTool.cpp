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
#include "meta/AdHocSchemaManager.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "dataman/RowWriter.h"
#include <folly/Benchmark.h>
#include "storage/StorageFlags.h"


DEFINE_int32(bulk_insert_size, 1000, "The number of vertices by bulk insert");
DEFINE_int32(total_vertices_size, 1000000, "The number of vertices");
DEFINE_string(root_data_path, "/tmp/IndexWritePref", "Engine data path");

namespace nebula {
namespace storage {
std::unique_ptr<kvstore::KVStore> initKV(
        const char* rootPath,
        int32_t partitionNumber = 1,
        HostAddr localhost = {0, 0},
        meta::MetaClient* mClient = nullptr,
        bool useMetaServer = false,
        std::unique_ptr<kvstore::CompactionFilterFactoryBuilder> cffBuilder = nullptr) {
    auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    auto workers = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(
            1, true /*stats*/);
    workers->setNamePrefix("executor");
    workers->start();


    kvstore::KVOptions options;
    if (useMetaServer) {
        options.partMan_ = std::make_unique<kvstore::MetaServerBasedPartManager>(
                localhost,
                mClient);
    } else {
        auto memPartMan = std::make_unique<kvstore::MemPartManager>();
        auto& partsMap = memPartMan->partsMap();
        for (auto partId = 0; partId < partitionNumber; partId++) {
            partsMap[0][partId] = PartMeta();
        }

        options.partMan_ = std::move(memPartMan);
    }
    if (fs::FileUtils::exist(rootPath)) {
        fs::FileUtils::remove(rootPath, true);
    }
    fs::FileUtils::makeDir(rootPath);
    std::vector<std::string> paths;
    paths.emplace_back(rootPath);

    // Prepare KVStore
    options.dataPaths_ = std::move(paths);
    options.cffBuilder_ = std::move(cffBuilder);
    auto store = std::make_unique<kvstore::NebulaStore>(std::move(options),
                                                        ioPool,
                                                        localhost,
                                                        workers);
    store->init();
    sleep(1);
    return store;
}

std::shared_ptr<meta::SchemaProviderIf> genTagSchema(TagID tagId) {
    nebula::cpp2::Schema schema;
    for (auto i = 0; i < 3; i++) {
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
        column.type.type = nebula::cpp2::SupportedType::INT;
        schema.columns.emplace_back(std::move(column));
    }
    for (auto i = 3; i < 6; i++) {
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
        column.type.type = nebula::cpp2::SupportedType::STRING;
        schema.columns.emplace_back(std::move(column));
    }
    return std::make_shared<ResultSchemaProvider>(std::move(schema));
}

std::unique_ptr<meta::SchemaManager> mockSchemaMan(TagID tagId,
    const std::vector<nebula::cpp2::IndexItem>& tagIndexes = {}) {
    auto* schemaMan = new AdHocSchemaManager();
    schemaMan->addTagSchema(0 /*space id*/, tagId, genTagSchema(tagId));
    for (auto& index : tagIndexes) {
        schemaMan->addTagIndex(0 /*space id*/, index);
    }
    std::unique_ptr<meta::SchemaManager> sm(schemaMan);
    return sm;
}

std::vector<nebula::cpp2::IndexItem> mockIndexes(TagID tagId) {
    std::vector<nebula::cpp2::IndexItem> indexes;
    std::vector<nebula::cpp2::ColumnDef> cols;
    for (auto i = 0; i < 3; i++) {
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
        column.type.type = nebula::cpp2::SupportedType::INT;
        cols.emplace_back(std::move(column));
    }
    for (auto i = 3; i < 6; i++) {
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
        column.type.type = nebula::cpp2::SupportedType::STRING;
        cols.emplace_back(std::move(column));
    }
    // indexId can be same with tagId
    nebula::cpp2::IndexItem index;
    index.set_index_id(tagId);
    index.set_tagOrEdge(tagId);
    index.set_cols(std::move(cols));
    indexes.emplace_back(std::move(index));
    return indexes;
}

std::vector<nebula::cpp2::IndexItem> mockMultipleIndexes(TagID tagId) {
    std::vector<nebula::cpp2::IndexItem> indexes;
    std::vector<nebula::cpp2::ColumnDef> cols;
    IndexID indexId = 0;
    for (auto i = 0; i < 3; i++) {
        {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
            column.type.type = nebula::cpp2::SupportedType::INT;
            cols.emplace_back(std::move(column));
            column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i + 3);
            column.type.type = nebula::cpp2::SupportedType::STRING;
            cols.emplace_back(std::move(column));
            nebula::cpp2::IndexItem index;
            index.set_index_id(++indexId);
            index.set_tagOrEdge(tagId);
            index.set_cols(std::move(cols));
            indexes.emplace_back(std::move(index));
        }
    }
    return indexes;
}

std::string genData() {
    RowWriter writer;
    for (int64_t numInt = 0; numInt < 3; numInt++) {
        writer << static_cast<int64_t>(folly::Random::rand32());
    }
    for (auto numString = 3; numString < 6; numString++) {
        writer << folly::stringPrintf("tag_string_col_%u", folly::Random::rand32());
    }
    return writer.encode();
}

std::vector<storage::cpp2::Vertex> genVertices(VertexID &vId, TagID tagId) {
    std::vector<storage::cpp2::Vertex> vertices;
    for (auto i = 0; i < FLAGS_bulk_insert_size; i++) {
        storage::cpp2::Vertex v;
        v.set_id(++vId);
        decltype(v.tags) tags;
        storage::cpp2::Tag tag;
        tag.set_tag_id(tagId);
        auto props = genData();
        tag.set_props(std::move(props));
        tags.emplace_back(std::move(tag));
        v.set_tags(std::move(tags));
        vertices.emplace_back(std::move(v));
    }
    return vertices;
}

bool processVertices(kvstore::KVStore* kv,
                     meta::SchemaManager* schemaMan,
                     VertexID &vId) {
    cpp2::AddVerticesRequest req;
    BENCHMARK_SUSPEND {
        req.space_id = 0;
        req.overwritable = true;
        std::vector<cpp2::Vertex> vertices;
        auto v = genVertices(vId, 1);
        vertices.insert(vertices.end(), v.begin(), v.end());
        req.parts.emplace(0, std::move(vertices));
    };
    auto* processor = AddVerticesProcessor::instance(kv, schemaMan, nullptr);
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

void insertVertices(bool withoutIndex, bool skipIndexCheck = false) {
    std::unique_ptr<kvstore::KVStore> kv;
    std::unique_ptr<meta::SchemaManager> schemaMan;
    VertexID vId = 0;
    auto skipCheck = FLAGS_ignore_index_check_pre_insert;
    BENCHMARK_SUSPEND {
        std::string rootPath;
        if (withoutIndex) {
            rootPath = folly::stringPrintf("%s/%s",
                                           FLAGS_root_data_path.c_str(),
                                           "withoutIndex");
        } else {
            if (skipIndexCheck) {
                FLAGS_ignore_index_check_pre_insert = true;
                rootPath = folly::stringPrintf("%s/%s",
                                               FLAGS_root_data_path.c_str(),
                                               "skipIndexCheck");
            } else {
                rootPath = folly::stringPrintf("%s/%s",
                                               FLAGS_root_data_path.c_str(),
                                               "attachIndex");
            }
        }
        kv = initKV(std::move(rootPath).c_str());
        if (withoutIndex) {
            schemaMan = mockSchemaMan(1);
        } else {
            auto indexes = mockIndexes(1);
            schemaMan = mockSchemaMan(1, std::move(indexes));
        }
    };
    while (vId < FLAGS_total_vertices_size) {
        if (!processVertices(kv.get(), schemaMan.get(), vId)) {
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
        }
        if (!withoutIndex) {
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
        FLAGS_ignore_index_check_pre_insert = skipCheck;
        schemaMan.release();
        kv.release();
        fs::FileUtils::remove(FLAGS_root_data_path.c_str(), true);
    };
}

void insertUnmatchIndex() {
    std::unique_ptr<kvstore::KVStore> kv;
    std::unique_ptr<meta::SchemaManager> schemaMan;
    VertexID vId = 0;
    BENCHMARK_SUSPEND {
        auto rootPath = folly::stringPrintf("%s/%s", FLAGS_root_data_path.c_str(), "unmatchIndex");
        kv = initKV(std::move(rootPath).c_str());
        auto indexes = mockIndexes(100);
        schemaMan = mockSchemaMan(1, std::move(indexes));
    };
    while (vId < FLAGS_total_vertices_size) {
        if (!processVertices(kv.get(), schemaMan.get(), vId)) {
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
    VertexID vId = 0;
    BENCHMARK_SUSPEND {
        auto rootPath = folly::stringPrintf("%s/%s",
                FLAGS_root_data_path.c_str(),
                "duplicateIndex");
        kv = initKV(std::move(rootPath).c_str());
        auto indexes = mockIndexes(1);
        schemaMan = mockSchemaMan(1, std::move(indexes));
    };
    BENCHMARK_SUSPEND {
        while (vId < FLAGS_total_vertices_size) {
            if (!processVertices(kv.get(), schemaMan.get(), vId)) {
                LOG(ERROR) << "Vertices bulk insert error";
                return;
            }
        }
    };
    vId = 0;
    while (vId < FLAGS_total_vertices_size) {
        if (!processVertices(kv.get(), schemaMan.get(), vId)) {
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

void insertVerticesMultIndex(bool skipIndexCheck = false) {
    std::unique_ptr<kvstore::KVStore> kv;
    std::unique_ptr<meta::SchemaManager> schemaMan;
    auto skipCheck = FLAGS_ignore_index_check_pre_insert;
    VertexID vId = 0;
    BENCHMARK_SUSPEND {
        std::string rootPath;
        if (skipIndexCheck) {
            FLAGS_ignore_index_check_pre_insert = true;
            rootPath = folly::stringPrintf("%s/%s",
                                           FLAGS_root_data_path.c_str(),
                                           "skipCheckMultIndex");
        } else {
            rootPath = folly::stringPrintf("%s/%s",
                                           FLAGS_root_data_path.c_str(),
                                           "multIndex");
        }

        kv = initKV(std::move(rootPath).c_str());
        auto indexes = mockMultipleIndexes(1);
        schemaMan = mockSchemaMan(1, std::move(indexes));
    };
    while (vId < FLAGS_total_vertices_size) {
        if (!processVertices(kv.get(), schemaMan.get(), vId)) {
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
        FLAGS_ignore_index_check_pre_insert = skipCheck;
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

BENCHMARK(skipCheckOneIndex) {
    insertVertices(false, true);
}

BENCHMARK(duplicateVerticesIndex) {
    insertDupVertices();
}

BENCHMARK(multipleIndex) {
    insertVerticesMultIndex();
}

BENCHMARK(skipCheckMultiIndex) {
    insertVerticesMultIndex(true);
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
skipCheckOneIndex: Skip check obsolete index by one index.
duplicateVerticesIndex: One index, and insert deplicate vertices.
multipleIndex: Three indexes by one tag.
skipCheckMultiIndex: Skip check obsolete index by three index.


--total_vertices_size=2000000
============================================================================
src/tools/storage-perf/StorageIndexWritePerfTool.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                  3.00s  333.45m
unmatchIndex                                                  4.72s  212.02m
attachIndex                                                  40.56s   24.66m
skipCheckOneIndex                                            24.44s   40.91m
duplicateVerticesIndex                                       46.14s   21.67m
multipleIndex                                               1.19min   14.00m
skipCheckMultiIndex                                          33.51s   29.84m
============================================================================

--total_vertices_size=1000000
============================================================================
src/tools/storage-perf/StorageIndexWritePerfTool.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                  1.49s  672.17m
unmatchIndex                                                  2.40s  415.88m
attachIndex                                                  19.57s   51.11m
skipCheckOneIndex                                            12.53s   79.80m
duplicateVerticesIndex                                       21.22s   47.12m
multipleIndex                                                33.55s   29.81m
skipCheckMultiIndex                                          17.01s   58.80m
============================================================================

--total_vertices_size=100000
============================================================================
src/tools/storage-perf/StorageIndexWritePerfTool.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                               136.06ms     7.35
unmatchIndex                                               207.66ms     4.82
attachIndex                                                   1.53s  651.66m
skipCheckOneIndex                                             1.19s  837.83m
duplicateVerticesIndex                                        1.55s  647.09m
multipleIndex                                                 2.41s  414.81m
skipCheckMultiIndex                                           1.61s  620.78m
============================================================================

--total_vertices_size=10000
============================================================================
src/tools/storage-perf/StorageIndexWritePerfTool.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                13.07ms    76.50
unmatchIndex                                                20.58ms    48.59
attachIndex                                                148.69ms     6.73
skipCheckOneIndex                                          115.72ms     8.64
duplicateVerticesIndex                                     140.48ms     7.12
multipleIndex                                              214.98ms     4.65
skipCheckMultiIndex                                        142.01ms     7.04
============================================================================

--total_vertices_size=1000
============================================================================
src/tools/storage-perf/StorageIndexWritePerfTool.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                 1.42ms   702.73
unmatchIndex                                                 3.00ms   333.57
attachIndex                                                 13.92ms    71.85
skipCheckOneIndex                                           11.71ms    85.38
duplicateVerticesIndex                                      13.94ms    71.72
multipleIndex                                               20.45ms    48.91
skipCheckMultiIndex                                         13.89ms    71.98
============================================================================
**/

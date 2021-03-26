/* Copyright (c) 2020 vesoft inc. All rights reserved.
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
#include "storage/index/LookUpIndexProcessor.h"
#include "dataman/RowWriter.h"
#include <folly/Benchmark.h>
#include "storage/test/TestUtils.h"
#include "storage/StorageFlags.h"

DEFINE_int64(total_vertices_size, 20000000, "The number of vertices");
DEFINE_string(root_data_path, "/tmp/LookUpPref", "Engine data path");

namespace nebula {
namespace storage {

GraphSpaceID spaceId = 0;
TagID tagId = 1;
IndexID indexId = 1;
int32_t partNums = 100;
static int64_t modeInt = 100;
auto tf = std::make_shared<folly::NamedThreadFactory>("lookup-pool");
auto pool = std::make_shared<folly::IOThreadPoolExecutor>(40, std::move(tf));

using IndexValues = std::vector<std::pair<nebula::cpp2::SupportedType, std::string>>;

std::string indexStr(RowReader* reader, const std::string& col) {
    auto res = RowReader::getPropByName(reader, col);
    if (!ok(res)) {
        LOG(ERROR) << "Skip bad column prop " << col;
        return "";
    }
    auto&& v = value(std::move(res));
    return NebulaKeyUtils::encodeInt64(boost::get<int64_t>(v));
}

IndexValues collectIndexValues(RowReader* reader,
                               const std::vector<nebula::cpp2::ColumnDef>& cols) {
    IndexValues values;
    if (reader == nullptr) {
        return values;
    }
    for (auto& col : cols) {
        auto val = indexStr(reader, col.get_name());
        values.emplace_back(col.get_type().get_type(), std::move(val));
    }
    return values;
}

std::string genVertexIndexKey(meta::SchemaManager* schemaMan,
                              const std::string &prop,
                              std::shared_ptr<nebula::cpp2::IndexItem> &index,
                              VertexID vId,
                              PartitionID partId) {
    auto reader = RowReader::getTagPropReader(schemaMan,
                                              prop,
                                              spaceId,
                                              tagId);
    auto values = collectIndexValues(reader.get(),
                                     index->get_fields());
    auto indexKey = NebulaKeyUtils::vertexIndexKey(partId,
                                                   index->get_index_id(),
                                                   vId,
                                                   values);
    return indexKey;
}

std::unique_ptr<AdHocSchemaManager> mockSchemaMan() {
    auto schemaMan = std::make_unique<AdHocSchemaManager>();
    nebula::cpp2::Schema schema;
    for (int32_t i = 0; i < 3; i++) {
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("col_%d", i);
        column.type.type = nebula::cpp2::SupportedType::INT;
        schema.columns.emplace_back(std::move(column));
    }
    schemaMan->addTagSchema(spaceId, tagId,
                            std::make_shared<ResultSchemaProvider>(std::move(schema)));
    return schemaMan;
}

std::unique_ptr<meta::IndexManager> mockIndexMan() {
    auto* indexMan = new AdHocIndexManager();
    std::vector<nebula::cpp2::ColumnDef> columns;
    for (int32_t i = 0; i < 3; i++) {
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("col_%d", i);
        column.type.type = nebula::cpp2::SupportedType::INT;
        columns.emplace_back(std::move(column));
    }
    indexMan->addTagIndex(spaceId, indexId, tagId, std::move(columns));
    std::unique_ptr<meta::IndexManager> im(indexMan);
    return im;
}

std::string genVertexProp(int64_t vId) {
    RowWriter writer;
    int64_t colVal = folly::to<int64_t >(vId % modeInt);
    writer << colVal << colVal << colVal;
    return writer.encode();
}

bool genData(kvstore::KVStore* kv,
             meta::SchemaManager* schemaMan,
             std::shared_ptr<nebula::cpp2::IndexItem> &index) {
    std::unordered_map<PartitionID, std::vector<kvstore::KV>> partData;
    for (int vid = 1; vid <= FLAGS_total_vertices_size; vid++) {
        PartitionID partId = vid % partNums;
        auto val = genVertexProp(vid);
        auto indexKey = genVertexIndexKey(schemaMan, val, index, vid, partId);
        partData[partId].emplace_back(std::move(indexKey), "");
    }
    for (auto data : partData) {
        folly::Baton<true, std::atomic> baton;
        bool ret = false;
        kv->asyncMultiPut(spaceId, data.first, std::move(data.second),
                          [&ret, &baton] (kvstore::ResultCode code) {
                              if (kvstore::ResultCode::SUCCEEDED == code) {
                                  ret = true;
                              }
                              baton.post();
                          });
        baton.wait();
        if (!ret) {
            return false;
        }
    }

    auto code = kv->compact(spaceId);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        return false;
    }
    return true;
}

bool processLookup(kvstore::KVStore* kv,
                   meta::SchemaManager* schemaMan,
                   meta::IndexManager* indexMan,
                   const std::string& filter,
                   size_t expectRowNum,
                   bool con) {
    cpp2::LookUpIndexRequest req;
    decltype(req.parts) parts;
    for (int partId = 0; partId < partNums; partId++) {
        parts.emplace_back(partId);
    }
    req.set_space_id(spaceId);
    req.set_parts(std::move(parts));
    req.set_index_id(indexId);
    req.set_filter(filter);

    if (con) {
        auto* processor = LookUpIndexProcessor::instance(kv, schemaMan, indexMan,
                                                         nullptr, pool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        BENCHMARK_SUSPEND {
            if (resp.result.failed_codes.size() > 0) {
                LOG(ERROR) << "Process error";
                return false;
            }
            if (resp.get_vertices()->size() != expectRowNum) {
                LOG(ERROR) << "Row number error , expected : " << expectRowNum
                           << " actual ： " << resp.get_vertices()->size();
                return false;
            }
        };
    } else {
        auto* processor = LookUpIndexProcessor::instance(kv, schemaMan, indexMan, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        BENCHMARK_SUSPEND {
            if (resp.result.failed_codes.size() > 0) {
                LOG(ERROR) << "Process error";
                return false;
            }
            if (resp.get_vertices()->size() != expectRowNum) {
                LOG(ERROR) << "Row number error , expected : " << expectRowNum
                           << " actual ： " << resp.get_vertices()->size();
                return false;
            }
        };
    }
    return true;
}

/**
 *
 * where col1 = XXXXX
 */
void lookupExec(int32_t expectRowNum,
                const std::string& filter,
                bool con) {
    std::unique_ptr<kvstore::KVStore> kv;
    std::unique_ptr<meta::SchemaManager> schemaMan;
    std::unique_ptr<meta::IndexManager> indexMan;
    std::shared_ptr<nebula::cpp2::IndexItem> index;
    BENCHMARK_SUSPEND {
        std::string rootPath;
        rootPath = folly::stringPrintf("%s/%s_%d",
                                       FLAGS_root_data_path.c_str(),
                                       "Precise", expectRowNum);
        kv = TestUtils::initKV(std::move(rootPath).c_str(), partNums);
        schemaMan = mockSchemaMan();
        indexMan = mockIndexMan();
        auto iRet = indexMan->getTagIndex(spaceId, indexId);
        if (!iRet.ok()) {
            LOG(ERROR) << "Index get error";
            return;
        }
        index = iRet.value();
        if (!genData(kv.get(), schemaMan.get(), index)) {
            LOG(ERROR) << "Data load error";
            return;
        }
    };
    auto ret = processLookup(kv.get(), schemaMan.get(), indexMan.get(), filter, expectRowNum, con);
    if (!ret) {
        LOG(ERROR) << "Processor error";
        return;
    }
}

void lookupPrecise(int32_t expectRowNum, bool con = false) {
    auto* col = new std::string("col_0");
    auto* alias = new std::string("col0");
    auto* ape = new AliasPropertyExpression(new std::string(""), alias, col);
    auto* pe = new PrimaryExpression(1L);
    auto r =  std::make_unique<RelationalExpression>(ape,
                                                     RelationalExpression::Operator::EQ,
                                                     pe);
    lookupExec(expectRowNum, Expression::encode(r.get()), con);
}

void lookupFilter(int32_t expectRowNum, bool con = false) {
    auto* col = new std::string("col_0");
    auto* alias = new std::string("col0");
    auto* ape = new AliasPropertyExpression(new std::string(""), alias, col);
    auto* pe = new PrimaryExpression(3L);
    auto r =  std::make_unique<RelationalExpression>(ape,
                                                     RelationalExpression::Operator::LT,
                                                     pe);
    lookupExec(expectRowNum, Expression::encode(r.get()), con);
}

BENCHMARK(PreciseScan_5000000) {
    modeInt = 4;
    lookupPrecise(5000000);
}

BENCHMARK(PreciseScanConcurrently_5000000) {
    modeInt = 4;
    lookupPrecise(5000000, true);
}

BENCHMARK(FilterScan_3000000) {
    modeInt = 20;
    lookupFilter(3000000);
}

BENCHMARK(FilterScanConcurrently_3000000) {
    modeInt = 20;
    lookupFilter(3000000, true);
}

BENCHMARK(PreciseScan_2000000) {
    modeInt = 10;
    lookupPrecise(2000000);
}

BENCHMARK(PreciseScanConcurrently_2000000) {
    modeInt = 10;
    lookupPrecise(2000000, true);
}

BENCHMARK(PreciseScan_1000000) {
    modeInt = 20;
    lookupPrecise(1000000);
}

BENCHMARK(PreciseScanConcurrently_1000000) {
    modeInt = 20;
    lookupPrecise(1000000, true);
}


}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    folly::runBenchmarks();
    return 0;
}


/**
 * CPU : Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
 * MemTotal : 8G
 * Total vertices : 20000000
 * Total partition number : 100
 * tag (col_0 int, col_1 int, col_2 int)
 * index on tag (col_0, col_1, col_2)
 *
 * PreciseScan_5000000 : Prefix scan and no expression filtering is required.
 *                       Return 5000000 vertices.
 *                       Where clause col_1 == 1
 *
 * PreciseScanConcurrently_5000000 : Prefix scan concurrently and no expression filtering is required.
 *                                   Return 5000000 vertices.
 *                                   Where clause col_1 == 1
 *
 * FilterScan_3000000     : Index scan and expression filtering is required.
 *                          Return 3000000 vertex.
 *                          Where clause col_1 < 3
 *
 * FilterScanConcurrently_3000000     : Index scan concurrently and expression filtering is required.
 *                                      Return 3000000 vertex.
 *                                      Where clause col_1 < 3
 *
 * PreciseScan_2000000   :  Prefix scan and no expression filtering is required.
 *                          Return 2000000 vertices.
 *                          Where clause col_1 == 1
 *
 * PreciseScanConcurrently_2000000   :  Prefix scan Concurrently and no expression filtering is required.
 *                                      Return 2000000 vertices.
 *                                      Where clause col_1 == 1
 *
 * PreciseScan_1000000    : Prefix scan and no expression filtering is required.
 *                          Return 1000000 vertices.
 *                          Where clause col_1 == 1
 *
 * PreciseScanConcurrently_1000000    : Prefix scan Concurrently and no expression filtering is required.
 *                                      Return 1000000 vertices.
 *                                      Where clause col_1 == 1
 *
 **/


/**
 * /opt/packages/nebula/src/storage/test/StorageLookupConcurrentlyBenchmark.cpprelative  time/iter  iters/s
 * ============================================================================
 * PreciseScan_5000000                                           1.32s  757.14m
 * PreciseScanConcurrently_5000000                            615.66ms     1.62
 * FilterScan_3000000                                         792.04ms     1.26
 * FilterScanConcurrently_3000000                             388.27ms     2.58
 * PreciseScan_2000000                                        511.91ms     1.95
 * PreciseScanConcurrently_2000000                            329.12ms     3.04
 * PreciseScan_1000000                                        270.57ms     3.70
 * PreciseScanConcurrently_1000000                            237.87ms     4.20
 * ============================================================================
 */

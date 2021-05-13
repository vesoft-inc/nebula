/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "kvstore/KVStore.h"
#include "kvstore/NebulaStore.h"
#include "storage/test/AdHocSchemaManager.h"
#include "storage/test/TestUtils.h"
#include "storage/StorageFlags.h"
#include "storage/index/LookUpVertexIndexProcessor.h"
#include "codec/RowWriter.h"
#include <folly/Benchmark.h>

DEFINE_int64(total_vertices_size, 1000000, "The number of vertices");
DEFINE_string(root_data_path, "/tmp/LookUpPref", "Engine data path");

namespace nebula {
namespace storage {

GraphSpaceID spaceId = 0;
TagID tagId = 1;
IndexID indexId = 1;
PartitionID partId = 0;
static int64_t modeInt = 100;

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
                              VertexID vId) {
    auto reader = RowReaderWrapper::getTagPropReader(schemaMan,
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
    std::vector<kvstore::KV> data;
    for (auto i = 0; i < FLAGS_total_vertices_size; i++) {
        auto val = genVertexProp(i);
        auto indexKey = genVertexIndexKey(schemaMan, val, index, i);
        data.emplace_back(std::move(indexKey), "");
    }
    folly::Baton<true, std::atomic> baton;
    bool ret = false;
    kv->asyncMultiPut(spaceId, partId, std::move(data),
                      [&ret, &baton] (nebula::cpp2::ErrorCode code) {
                          if (nebula::cpp2::ErrorCode::SUCCEEDED == code) {
                              ret = true;
                          }
                          baton.post();
                      });
    baton.wait();
    if (!ret) {
        return false;
    }

    auto code = kv->compact(spaceId);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return false;
    }
    return true;
}

bool processLookup(kvstore::KVStore* kv,
                   meta::SchemaManager* schemaMan,
                   meta::IndexManager* indexMan,
                   const std::string& filter,
                   size_t expectRowNum) {
    cpp2::LookUpIndexRequest req;
    decltype(req.parts) parts;
    parts.emplace_back(partId);
    req.set_space_id(spaceId);
    req.set_parts(std::move(parts));
    req.set_index_id(indexId);
    req.set_filter(filter);
    auto* processor = LookUpVertexIndexProcessor::instance(kv, schemaMan, indexMan, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    BENCHMARK_SUSPEND {
        if (resp.result.failed_codes.size() > 0) {
            LOG(ERROR) << "Process error";
            return false;
        }
        if (resp.rows.size() != expectRowNum) {
            LOG(ERROR) << "Row number error , expected : " << expectRowNum
                       << " actual ： " << resp.rows.size();
            return false;
        }
    };
    return true;
}

/**
 *
 * where col1 = XXXXX
 */
void lookupExec(int32_t expectRowNum,
                const std::string& filter) {
    std::unique_ptr<kvstore::KVStore> kv;
    std::unique_ptr<meta::SchemaManager> schemaMan;
    std::unique_ptr<meta::IndexManager> indexMan;
    std::shared_ptr<nebula::cpp2::IndexItem> index;
    BENCHMARK_SUSPEND {
        std::string rootPath;
        rootPath = folly::stringPrintf("%s/%s_%d",
                                       FLAGS_root_data_path.c_str(),
                                       "Precise", expectRowNum);
        kv = TestUtils::initKV(std::move(rootPath).c_str());
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
    auto ret = processLookup(kv.get(), schemaMan.get(), indexMan.get(), filter, expectRowNum);
    if (!ret) {
        LOG(ERROR) << "Processor error";
        return;
    }
}

void lookupPrecise(int32_t expectRowNum) {
    auto* col = new std::string("col_0");
    auto* alias = new std::string("col0");
    auto* ape = new AliasPropertyExpression(new std::string(""), alias, col);
    auto* pe = new PrimaryExpression(1L);
    auto r =  std::make_unique<RelationalExpression>(ape,
                                                     RelationalExpression::Operator::EQ,
                                                     pe);
    lookupExec(expectRowNum, Expression::encode(r.get()));
}

void lookupFilter(int32_t expectRowNum) {
    auto* col = new std::string("col_0");
    auto* alias = new std::string("col0");
    auto* ape = new AliasPropertyExpression(new std::string(""), alias, col);
    auto* pe = new PrimaryExpression(1L);
    auto r =  std::make_unique<RelationalExpression>(ape,
                                                     RelationalExpression::Operator::LT,
                                                     pe);
    lookupExec(expectRowNum, Expression::encode(r.get()));
}

BENCHMARK(PreciseScan_10000) {
    modeInt = 100;
    lookupPrecise(10000);
}

BENCHMARK(PreciseScan_100) {
    modeInt = 10000;
    lookupPrecise(100);
}

BENCHMARK(PreciseScan_10) {
    modeInt = 100000;
    lookupPrecise(10);
}

BENCHMARK(FilterScan_1) {
    modeInt = FLAGS_total_vertices_size;
    lookupFilter(1);
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
 * Total vertices : 1000000
 * Total partition number : 1
 * tag (col_0 int, col_1 int, col_2 int)
 * index on tag (col_0, col_1, col_2)
 *
 * PreciseScan_10000 : Prefix scan and no expression filtering is required.
 *                     Return 10000 vertices.
 *                     Where clause col_1 == 1
 *
 * PreciseScan_100   : Prefix scan and no expression filtering is required.
 *                     Return 100 vertices.
 *                     Where clause col_1 == 1
 *
 * PreciseScan_10    : Prefix scan and no expression filtering is required.
 *                     Return 10 vertices.
 *                     Where clause col_1 == 1
 *
 * FilterScan_10     : Index scan and expression filtering is required.
 *                     Return 1 vertex.
 *                     Where clause col_1 < 1
 *
 **/

/**
 * ============================================================================
 * src/storage/test/StorageLookupBenchmark.cpp relative  time/iter  iters/s
 * ============================================================================
 * PreciseScan_10000                                          9.65ms   103.65
 * PreciseScan_100                                            5.23ms   191.03
 * PreciseScan_10                                             4.14ms   241.64
 * FilterScan_10                                              1.47s  679.35m
 * ============================================================================
 **/


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
#include "storage/index/LookUpVertexIndexProcessor.h"
#include "dataman/RowWriter.h"
#include <folly/Benchmark.h>
#include "storage/test/TestUtils.h"
#include "storage/StorageFlags.h"

DEFINE_int32(total_vertices_size, 1000000, "The number of vertices");
DEFINE_string(root_data_path, "/tmp/LookUpPref", "Engine data path");

namespace nebula {
namespace storage {

GraphSpaceID spaceId = 0;
TagID tagId = 1;
IndexID indexId = 1;
PartitionID partId = 0;
static int32_t modeInt = 100;

using IndexValues = std::vector<std::pair<nebula::cpp2::SupportedType, std::string>>;

std::string indexStr(RowReader* reader, const nebula::cpp2::ColumnDef& col) {
    auto res = RowReader::getPropByName(reader, col.get_name());
    if (!ok(res)) {
        LOG(ERROR) << "Skip bad column prop " << col.get_name();
        return "";
    }
    auto&& v = value(std::move(res));
    switch (col.get_type().get_type()) {
        case nebula::cpp2::SupportedType::BOOL: {
            auto val = boost::get<bool>(v);
            std::string raw;
            raw.reserve(sizeof(bool));
            raw.append(reinterpret_cast<const char*>(&val), sizeof(bool));
            return raw;
        }
        case nebula::cpp2::SupportedType::INT:
        case nebula::cpp2::SupportedType::TIMESTAMP: {
            return NebulaKeyUtils::encodeInt64(boost::get<int64_t>(v));
        }
        case nebula::cpp2::SupportedType::FLOAT:
        case nebula::cpp2::SupportedType::DOUBLE: {
            return NebulaKeyUtils::encodeDouble(boost::get<double>(v));
        }
        case nebula::cpp2::SupportedType::STRING: {
            return boost::get<std::string>(v);
        }
        default:
            LOG(ERROR) << "Unknown type: "
                       << static_cast<int32_t>(col.get_type().get_type());
    }
    return "";
}

IndexValues collectIndexValues(RowReader* reader,
                               const std::vector<nebula::cpp2::ColumnDef>& cols) {
    IndexValues values;
    if (reader == nullptr) {
        return values;
    }
    for (auto& col : cols) {
        auto val = indexStr(reader, col);
        values.emplace_back(col.get_type().get_type(), std::move(val));
    }
    return values;
}

std::string genVertexIndexKey(meta::SchemaManager* schemaMan,
                              const std::string &prop,
                              std::shared_ptr<nebula::cpp2::IndexItem> &index,
                              VertexID vId) {
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
        column.type.type = nebula::cpp2::SupportedType::STRING;
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
        column.type.type = nebula::cpp2::SupportedType::STRING;
        columns.emplace_back(std::move(column));
    }
    indexMan->addTagIndex(spaceId, indexId, tagId, std::move(columns));
    std::unique_ptr<meta::IndexManager> im(indexMan);
    return im;
}

std::string genVertexProp(int32_t vId) {
    RowWriter writer;
    writer << folly::stringPrintf("col0_row_%d", vId % modeInt)
           << folly::stringPrintf("col1_row_%d", vId)
           << folly::stringPrintf("col2_row_%d", vId);
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
                      [&ret, &baton] (kvstore::ResultCode code) {
                          if (kvstore::ResultCode::SUCCEEDED == code) {
                              ret = true;
                          }
                          baton.post();
                      });
    baton.wait();
    return ret;
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
    std::string c0("col0_row_1");
    auto* pe = new PrimaryExpression(c0);
    auto r =  std::make_unique<RelationalExpression>(ape,
                                                     RelationalExpression::Operator::EQ,
                                                     pe);
    lookupExec(expectRowNum, Expression::encode(r.get()));
}

void lookupFilter(int32_t expectRowNum) {
    auto* col = new std::string("col_1");
    auto* alias = new std::string("col1");
    auto* ape = new AliasPropertyExpression(new std::string(""), alias, col);
    std::string c0("col1_row_9999");
    auto* pe = new PrimaryExpression(c0);
    auto r =  std::make_unique<RelationalExpression>(ape,
                                                     RelationalExpression::Operator::EQ,
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
 * PreciseScan_10000 : Precise scan , return 10000 vertices
 * PreciseScan_100   : Precise scan , return 100 vertices
 * PreciseScan_10    : Precise scan , return 10 vertices
 * FilterScan_10     : Full index scan, return 1 vertex
 **/

/**
 * ============================================================================
 * src/storage/test/StorageLookupBenchmark.cpprelative  time/iter  iters/s
 * ============================================================================
 * PreciseScan_10000                                          242.67ms     4.12
 * PreciseScan_100                                            215.78ms     4.63
 * PreciseScan_10                                             221.32ms     4.52
 * FilterScan_10                                              354.26ms     2.82
 * ============================================================================
 **/


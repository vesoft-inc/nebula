/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include <gtest/gtest.h>
#include "fs/TempDir.h"
#include "time/WallClock.h"
#include "mock/AdHocSchemaManager.h"
#include "storage/exec/FilterNode.h"
#include "storage/exec/EdgeNode.h"
#include "storage/test/QueryTestUtils.h"

namespace nebula {
namespace storage {

class ScanEdgePropBench : public ::testing::TestWithParam<std::pair<int, int>> {
};

TEST_P(ScanEdgePropBench, ProcessEdgeProps) {
    auto param = GetParam();
    SchemaVer schemaVerCount = param.first;
    EdgeRanking rankCount = param.second;
    fs::TempDir rootPath("/tmp/ScanEdgePropBench.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path(), {0, 0}, schemaVerCount);
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockBenchEdgeData(env, totalParts, schemaVerCount, rankCount));

    std::hash<std::string> hash;
    VertexID vId = "Tim Duncan";
    GraphSpaceID spaceId = 1;
    PartitionID partId = (hash(vId) % totalParts) + 1;
    EdgeType edgeType = 101;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId).value();

    std::vector<PropContext> props;
    {
        std::vector<std::string> names = {"playerName", "teamName", "startYear", "endYear"};
        for (const auto& name : names) {
            PropContext ctx(name.c_str());
            ctx.returned_ = true;
            props.emplace_back(std::move(ctx));
        }
    }

    {
        // mock the process in 1.0, each time we get the schema from SchemaMan
        // (cache in MetaClient), and then collect values
        auto* schemaMan = dynamic_cast<mock::AdHocSchemaManager*>(env->schemaMan_);
        EdgeTypePrefixScanNode executor(
            nullptr, env, spaceId, partId, vIdLen, vId, nullptr, nullptr, nullptr);
        int64_t edgeRowCount = 0;
        nebula::DataSet dataSet;
        auto tick = time::WallClock::fastNowInMicroSec();
        auto retCode = executor.processEdgeProps(edgeType, edgeRowCount,
            [&executor, schemaMan, spaceId, edgeType, &props, &dataSet]
            (std::unique_ptr<RowReader>* reader, folly::StringPiece key, folly::StringPiece val)
            -> kvstore::ResultCode {
                UNUSED(reader);
                SchemaVer schemaVer;
                int32_t readerVer;
                RowReaderWrapper::getVersions(val, schemaVer, readerVer);
                auto schema = schemaMan->getEdgeSchemaFromMap(spaceId, edgeType, schemaVer);
                if (schema == nullptr) {
                    return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                }
                auto wrapper = std::make_unique<RowReaderWrapper>();
                if (!wrapper->reset(schema.get(), val, readerVer)) {
                    return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                }

                return executor.collectEdgeProps(edgeType, wrapper.get(), key, props,
                                                 dataSet, nullptr);
            });
        EXPECT_EQ(retCode, kvstore::ResultCode::SUCCEEDED);
        auto tock = time::WallClock::fastNowInMicroSec();
        LOG(WARNING) << "ProcessEdgeProps with schema from map: process " << edgeRowCount
                     << " edges takes " << tock - tick << " us.";
    }
    {
        // new edge reader each time
        EdgeTypePrefixScanNode executor(
            nullptr, env, spaceId, partId, vIdLen, vId, nullptr, nullptr, nullptr);
        int64_t edgeRowCount = 0;
        nebula::DataSet dataSet;
        auto tick = time::WallClock::fastNowInMicroSec();
        auto retCode = executor.processEdgeProps(edgeType, edgeRowCount,
            [&executor, spaceId, edgeType, env, &props, &dataSet]
            (std::unique_ptr<RowReader>* reader, folly::StringPiece key, folly::StringPiece val)
            -> kvstore::ResultCode {
                *reader = RowReader::getEdgePropReader(env->schemaMan_, spaceId,
                                                       std::abs(edgeType), val);
                if (!reader) {
                    return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                }

                return executor.collectEdgeProps(edgeType, reader->get(), key, props,
                                                 dataSet, nullptr);
            });
        EXPECT_EQ(retCode, kvstore::ResultCode::SUCCEEDED);
        auto tock = time::WallClock::fastNowInMicroSec();
        LOG(WARNING) << "ProcessEdgeProps without reader reset: process " << edgeRowCount
                     << " edges takes " << tock - tick << " us.";
    }
    {
        // reset edge reader each time instead of new one
        EdgeTypePrefixScanNode executor(
            nullptr, env, spaceId, partId, vIdLen, vId, nullptr, nullptr, nullptr);
        int64_t edgeRowCount = 0;
        nebula::DataSet dataSet;
        auto tick = time::WallClock::fastNowInMicroSec();
        auto retCode = executor.processEdgeProps(edgeType, edgeRowCount,
            [&executor, spaceId, edgeType, env, &props, &dataSet]
            (std::unique_ptr<RowReader>* reader, folly::StringPiece key, folly::StringPiece val)
            -> kvstore::ResultCode {
                if (reader->get() == nullptr) {
                    *reader = RowReader::getEdgePropReader(env->schemaMan_, spaceId,
                                                           std::abs(edgeType), val);
                    if (!reader) {
                        return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                    }
                } else if (!(*reader)->resetEdgePropReader(env->schemaMan_, spaceId,
                                                           std::abs(edgeType), val)) {
                    return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                }

                return executor.collectEdgeProps(edgeType, reader->get(), key, props,
                                                 dataSet, nullptr);
            });
        EXPECT_EQ(retCode, kvstore::ResultCode::SUCCEEDED);
        auto tock = time::WallClock::fastNowInMicroSec();
        LOG(WARNING) << "ProcessEdgeProps with reader reset: process " << edgeRowCount
                     << " edges takes " << tock - tick << " us.";
    }
    {
        // use the schema saved in processor
        EdgeTypePrefixScanNode executor(
            nullptr, env, spaceId, partId, vIdLen, vId, nullptr, nullptr, nullptr);
        int64_t edgeRowCount = 0;
        nebula::DataSet dataSet;

        // find all version of edge schema
        auto edges = env->schemaMan_->getAllVerEdgeSchema(spaceId);
        ASSERT_TRUE(edges.ok());
        auto edgeSchemas = std::move(edges).value();
        auto edgeIter = edgeSchemas.find(std::abs(edgeType));
        ASSERT_TRUE(edgeIter != edgeSchemas.end());
        const auto& schemas = edgeIter->second;

        auto tick = time::WallClock::fastNowInMicroSec();
        auto retCode = executor.processEdgeProps(edgeType, edgeRowCount,
            [&executor, spaceId, edgeType, &props, &dataSet, &schemas]
            (std::unique_ptr<RowReader>* reader, folly::StringPiece key, folly::StringPiece val)
            -> kvstore::ResultCode {
                if (reader->get() == nullptr) {
                    *reader = RowReader::getRowReader(schemas, val);
                    if (!reader) {
                        return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                    }
                } else if (!(*reader)->reset(schemas, val)) {
                    return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                }

                return executor.collectEdgeProps(edgeType, reader->get(), key, props,
                                                 dataSet, nullptr);
            });
        EXPECT_EQ(retCode, kvstore::ResultCode::SUCCEEDED);
        auto tock = time::WallClock::fastNowInMicroSec();
        LOG(WARNING) << "ProcessEdgeProps using local schmeas: process " << edgeRowCount
                     << " edges takes " << tock - tick << " us.";
    }
}

TEST_P(ScanEdgePropBench, ScanEdgesVsProcessEdgeProps) {
    auto param = GetParam();
    SchemaVer schemaVerCount = param.first;
    EdgeRanking rankCount = param.second;
    fs::TempDir rootPath("/tmp/ScanEdgePropBench.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path(), {0, 0}, schemaVerCount);
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockBenchEdgeData(env, totalParts, schemaVerCount, rankCount));

    std::hash<std::string> hash;
    VertexID vId = "Tim Duncan";
    GraphSpaceID spaceId = 1;
    PartitionID partId = (hash(vId) % totalParts) + 1;
    EdgeType serve = 101, teammate = 102;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId).value();

    std::vector<EdgeType> edgeTypes = {serve, teammate};
    std::vector<std::vector<PropContext>> ctxs;
    std::vector<PropContext> serveProps;
    {
        std::vector<std::string> names = {"playerName", "teamName", "startYear", "endYear"};
        for (const auto& name : names) {
            PropContext ctx(name.c_str());
            ctx.returned_ = true;
            serveProps.emplace_back(std::move(ctx));
        }
        ctxs.emplace_back(serveProps);
    }
    std::vector<PropContext> teammateProps;
    {
        std::vector<std::string> names = {"teamName", "startYear", "endYear"};
        for (const auto& name : names) {
            PropContext ctx(name.c_str());
            ctx.returned_ = true;
            teammateProps.emplace_back(std::move(ctx));
        }
        ctxs.emplace_back(teammateProps);
    }

    // find all version of edge schema
    auto edges = env->schemaMan_->getAllVerEdgeSchema(spaceId);
    ASSERT_TRUE(edges.ok());
    EdgeContext ctx;
    ctx.schemas_ = std::move(edges).value();
    ctx.propContexts_.emplace_back(serve, serveProps);
    ctx.propContexts_.emplace_back(teammate, teammateProps);
    ctx.indexMap_.emplace(serve, 0);
    ctx.indexMap_.emplace(teammate, 1);

    {
        // scan with vertex prefix
        FilterNode filter;
        nebula::Row row;
        row.columns.emplace_back(vId);
        row.columns.emplace_back(NullType::__NULL__);

        VertexPrefixScanNode executor(
            &ctx, env, spaceId, partId, vIdLen, vId, nullptr, &filter, &row);
        {
            auto tick = time::WallClock::fastNowInMicroSec();
            auto retCode = executor.execute().get();
            EXPECT_EQ(retCode, kvstore::ResultCode::SUCCEEDED);
            auto tock = time::WallClock::fastNowInMicroSec();
            LOG(WARNING) << "ScanEdges using local schmeas: process"
                         << " edges takes " << tock - tick << " us.";
        }
    }
    {
        // scan with vertex and edgeType prefix
        FilterNode filter;
        nebula::Row row;
        row.columns.emplace_back(vId);
        row.columns.emplace_back(NullType::__NULL__);

        EdgeTypePrefixScanNode executor(
            &ctx, env, spaceId, partId, vIdLen, vId, nullptr, &filter, &row);
        // find all version of edge schema
        auto status = env->schemaMan_->getAllVerEdgeSchema(spaceId);
        ASSERT_TRUE(status.ok());
        auto edgeSchemas = std::move(status).value();
        int64_t edgeRowCount = 0;

        auto tick = time::WallClock::fastNowInMicroSec();
        for (size_t i = 0; i < edgeTypes.size(); i++) {
            nebula::DataSet dataSet;
            EdgeType edgeType = edgeTypes[i];
            auto edgeIter = edgeSchemas.find(std::abs(edgeType));
            ASSERT_TRUE(edgeIter != edgeSchemas.end());
            const auto& schemas = edgeIter->second;
            const auto& props = ctxs[i];

            auto ttl = executor.getEdgeTTLInfo(edgeType);
            auto retCode = executor.processEdgeProps(edgeType, edgeRowCount,
                [&executor, spaceId, edgeType, &props, &dataSet, &schemas, &ttl]
                (std::unique_ptr<RowReader>* reader, folly::StringPiece key, folly::StringPiece val)
                -> kvstore::ResultCode {
                    if (reader->get() == nullptr) {
                        *reader = RowReader::getRowReader(schemas, val);
                        if (!reader) {
                            return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                        }
                    } else if (!(*reader)->reset(schemas, val)) {
                        return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                    }

                    const auto& latestSchema = schemas.back();
                    if (ttl.has_value() &&
                        CommonUtils::checkDataExpiredForTTL(latestSchema.get(),
                                                            reader->get(),
                                                            ttl.value().first,
                                                            ttl.value().second)) {
                        return kvstore::ResultCode::ERR_RESULT_EXPIRED;
                    }

                    return executor.collectEdgeProps(edgeType, reader->get(), key, props,
                                                     dataSet, nullptr);
                });
            EXPECT_EQ(retCode, kvstore::ResultCode::SUCCEEDED);
            row.columns.emplace_back(std::move(dataSet));
        }
        auto tock = time::WallClock::fastNowInMicroSec();
        LOG(WARNING) << "ProcessEdgeProps using local schmeas: process " << edgeRowCount
                     << " edges takes " << tock - tick << " us.";
    }
}

// the parameter pair<int, int> is count of edge schema version,
// and how many edges of diffent rank of a mock edge
INSTANTIATE_TEST_CASE_P(
    ScanEdgePropBench,
    ScanEdgePropBench,
    ::testing::Values(
        std::make_pair(1, 10000),
        std::make_pair(10, 10000),
        std::make_pair(100, 10000)));


}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::WARNING);
    return RUN_ALL_TESTS();
}

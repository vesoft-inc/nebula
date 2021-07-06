/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include <folly/Benchmark.h>
#include "common/fs/TempDir.h"
#include "storage/query/GetNeighborsProcessor.h"
#include "storage/test/QueryTestUtils.h"
#include "storage/exec/EdgeNode.h"

DEFINE_uint64(max_rank, 1000, "max rank of each edge");
DEFINE_double(filter_ratio, 0.1, "ratio of data would pass filter");
DEFINE_bool(go_record, false, "");
DEFINE_bool(kv_record, false, "");

std::unique_ptr<nebula::mock::MockCluster> gCluster;
auto pool = &gCluster->pool_;
namespace nebula {
namespace storage {

cpp2::GetNeighborsRequest buildRequest(const std::vector<VertexID>& vertex,
                                       const std::vector<std::string>& playerProps,
                                       const std::vector<std::string>& serveProps) {
    TagID player = 1;
    EdgeType serve = 101;
    auto totalParts = gCluster->getTotalParts();
    std::vector<VertexID> vertices = vertex;
    std::vector<EdgeType> over = {serve};
    std::vector<std::pair<TagID, std::vector<std::string>>> tags;
    std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
    tags.emplace_back(player, playerProps);
    edges.emplace_back(serve, serveProps);
    auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
    return req;
}

void setUp(const char* path, EdgeRanking maxRank) {
    gCluster = std::make_unique<nebula::mock::MockCluster>();
    gCluster->initStorageKV(path);
    auto* env = gCluster->storageEnv_.get();
    auto totalParts = gCluster->getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockBenchEdgeData(env, totalParts, 1, maxRank));
}

}  // namespace storage
}   // namespace nebula

std::string encode(const nebula::storage::cpp2::GetNeighborsResponse &resp) {
    std::string val;
    apache::thrift::CompactSerializer::serialize(resp, &val);
    return val;
}

void initContext(std::unique_ptr<nebula::storage::PlanContext>& planCtx,
                 std::unique_ptr<nebula::storage::RunTimeContext>& context,
                 nebula::storage::EdgeContext& edgeContext,
                 const std::vector<std::string>& serveProps) {
    nebula::GraphSpaceID spaceId = 1;
    auto* env = gCluster->storageEnv_.get();
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId).value();
    planCtx = std::make_unique<nebula::storage::PlanContext>(env, spaceId, vIdLen, false);
    context = std::make_unique<nebula::storage::RunTimeContext>(planCtx.get());

    nebula::EdgeType serve = 101;
    edgeContext.schemas_ = std::move(env->schemaMan_->getAllVerEdgeSchema(spaceId)).value();

    auto edgeName = env->schemaMan_->toEdgeName(spaceId, std::abs(serve));
    edgeContext.edgeNames_.emplace(serve, std::move(edgeName).value());
    auto iter = edgeContext.schemas_.find(std::abs(serve));
    const auto& edgeSchema = iter->second.back();

    std::vector<nebula::storage::PropContext> ctxs;
    for (const auto& prop : serveProps) {
        auto field = edgeSchema->field(prop);
        nebula::storage::PropContext ctx(prop.c_str());
        ctx.returned_ = true;
        ctx.field_ = field;
        ctxs.emplace_back(std::move(ctx));
    }
    edgeContext.propContexts_.emplace_back(serve, std::move(ctxs));
    edgeContext.indexMap_.emplace(serve, edgeContext.propContexts_.size() - 1);

    const auto& ec = edgeContext.propContexts_.front();
    context->props_ = &ec.second;
}

void go(int32_t iters,
        const std::vector<nebula::VertexID>& vertex,
        const std::vector<std::string>& playerProps,
        const std::vector<std::string>& serveProps) {
    nebula::storage::cpp2::GetNeighborsRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildRequest(vertex, playerProps, serveProps);
    }
    auto* env = gCluster->storageEnv_.get();
    for (decltype(iters) i = 0; i < iters; i++) {
        auto* processor = nebula::storage::GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        auto encoded = encode(resp);
        folly::doNotOptimizeAway(encoded);
    }
}

void goFilter(int32_t iters,
              const std::vector<nebula::VertexID>& vertex,
              const std::vector<std::string>& playerProps,
              const std::vector<std::string>& serveProps,
              int64_t value = FLAGS_max_rank * FLAGS_filter_ratio,
              bool oneFilter = true) {
    nebula::storage::cpp2::GetNeighborsRequest req;
    BENCHMARK_SUSPEND {
        nebula::EdgeType serve = 101;
        req = nebula::storage::buildRequest(vertex, playerProps, serveProps);
        if (oneFilter) {
            // where serve.startYear < value
            const auto& exp = *nebula::RelationalExpression::makeLT(
                pool,
                nebula::EdgePropertyExpression::make(
                    pool, folly::to<std::string>(serve), "startYear"),
                nebula::ConstantExpression::make(pool, nebula::Value(value)));
            (*req.traverse_spec_ref()).set_filter(nebula::Expression::encode(exp));
        } else {
            // where serve.startYear < value && serve.endYear < value
            // since startYear always equal to endYear, the data of which can pass filter is same,
            // just to test perf of multiple filter
            const auto& exp = *nebula::LogicalExpression::makeAnd(
                pool,
                nebula::RelationalExpression::makeLT(
                    pool,
                    nebula::EdgePropertyExpression::make(
                        pool, folly::to<std::string>(serve), "startYear"),
                    nebula::ConstantExpression::make(pool, nebula::Value(value))),
                nebula::RelationalExpression::makeLT(
                    pool,
                    nebula::EdgePropertyExpression::make(
                        pool, folly::to<std::string>(serve), "endYear"),
                    nebula::ConstantExpression::make(pool, nebula::Value(value))));
            (*req.traverse_spec_ref()).set_filter(nebula::Expression::encode(exp));
        }
    }
    auto* env = gCluster->storageEnv_.get();
    for (decltype(iters) i = 0; i < iters; i++) {
        auto* processor = nebula::storage::GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        auto encoded = encode(resp);
        folly::doNotOptimizeAway(encoded);
    }
}

void goEdgeNode(int32_t iters,
                const std::vector<nebula::VertexID>& vertex,
                const std::vector<std::string>& playerProps,
                const std::vector<std::string>& serveProps) {
    UNUSED(playerProps);
    std::unique_ptr<nebula::storage::PlanContext> planCtx;
    std::unique_ptr<nebula::storage::RunTimeContext> context;
    std::unique_ptr<nebula::storage::SingleEdgeNode> edgeNode;
    nebula::storage::EdgeContext edgeContext;
    BENCHMARK_SUSPEND {
        initContext(planCtx, context, edgeContext, serveProps);
        const auto& ec = edgeContext.propContexts_.front();
        edgeNode = std::make_unique<nebula::storage::SingleEdgeNode>(
            context.get(), &edgeContext, ec.first, &ec.second);
    }
    auto totalParts = gCluster->getTotalParts();
    for (decltype(iters) i = 0; i < iters; i++) {
        nebula::storage::cpp2::GetNeighborsResponse resp;
        nebula::DataSet resultDataSet;
        std::hash<std::string> hash;
        for (const auto& vId : vertex) {
            nebula::PartitionID partId = (hash(vId) % totalParts) + 1;
            std::vector<nebula::Value> row;
            row.emplace_back(vId);
            row.emplace_back(nebula::List());
            {
                edgeNode->execute(partId, vId);
                int32_t count = 0;
                auto& cell = row[1].mutableList();
                for (; edgeNode->valid(); edgeNode->next()) {
                    nebula::List list;
                    auto key = edgeNode->key();
                    folly::doNotOptimizeAway(key);
                    auto reader = edgeNode->reader();
                    auto props = context->props_;
                    for (const auto& prop : *props) {
                        auto value = nebula::storage::QueryUtils::readValue(
                            reader, prop.name_, prop.field_);
                        CHECK(value.ok());
                        list.emplace_back(std::move(value).value());
                    }
                    cell.values.emplace_back(std::move(list));
                    count++;
                }
                CHECK_EQ(FLAGS_max_rank, count);
            }
            resultDataSet.rows.emplace_back(std::move(row));
        }
        CHECK_EQ(vertex.size(), resultDataSet.rowSize());
        nebula::storage::cpp2::ResponseCommon result;
        resp.set_result(std::move(result));
        resp.set_vertices(std::move(resultDataSet));
        auto encoded = encode(resp);
        folly::doNotOptimizeAway(encoded);
    }
}

void prefix(int32_t iters,
            const std::vector<nebula::VertexID>& vertex,
            const std::vector<std::string>& playerProps,
            const std::vector<std::string>& serveProps) {
    std::unique_ptr<nebula::storage::PlanContext> planCtx;
    std::unique_ptr<nebula::storage::RunTimeContext> context;
    nebula::storage::EdgeContext edgeContext;
    BENCHMARK_SUSPEND {
        initContext(planCtx, context, edgeContext, serveProps);
    }
    for (decltype(iters) i = 0; i < iters; i++) {
        nebula::storage::cpp2::GetNeighborsResponse resp;
        nebula::DataSet resultDataSet;

        nebula::GraphSpaceID spaceId = 1;
        nebula::TagID player = 1;
        nebula::EdgeType serve = 101;

        std::hash<std::string> hash;
        auto* env = gCluster->storageEnv_.get();
        auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId).value();
        auto totalParts = gCluster->getTotalParts();

        auto tagSchemas = env->schemaMan_->getAllVerTagSchema(spaceId).value();
        auto tagSchemaIter = tagSchemas.find(player);
        CHECK(tagSchemaIter != tagSchemas.end());
        CHECK(!tagSchemaIter->second.empty());
        auto* tagSchema = &(tagSchemaIter->second);

        auto edgeSchemas = env->schemaMan_->getAllVerEdgeSchema(spaceId).value();
        auto edgeSchemaIter = edgeSchemas.find(std::abs(serve));
        CHECK(edgeSchemaIter != edgeSchemas.end());
        CHECK(!edgeSchemaIter->second.empty());
        auto* edgeSchema = &(edgeSchemaIter->second);

        nebula::RowReaderWrapper reader;
        for (const auto& vId : vertex) {
            nebula::PartitionID partId = (hash(vId) % totalParts) + 1;
            std::vector<nebula::Value> row;
            row.emplace_back(vId);
            row.emplace_back(nebula::List());
            row.emplace_back(nebula::List());
            {
                // read tags
                std::unique_ptr<nebula::kvstore::KVIterator> iter;
                auto prefix = nebula::NebulaKeyUtils::vertexPrefix(vIdLen, partId, vId, player);
                auto code = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
                ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);
                CHECK(iter->valid());
                auto val = iter->val();
                reader.reset(*tagSchema, val);
                CHECK_NOTNULL(reader);
                auto& cell = row[1].mutableList();
                for (const auto& prop : playerProps) {
                    cell.emplace_back(reader->getValueByName(prop));
                }
            }
            {
                // read edges
                std::unique_ptr<nebula::kvstore::KVIterator> iter;
                auto prefix = nebula::NebulaKeyUtils::edgePrefix(vIdLen, partId, vId, serve);
                auto code = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
                ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);
                int32_t count = 0;
                auto& cell = row[2].mutableList();
                for (; iter->valid(); iter->next()) {
                    nebula::List list;
                    auto key = iter->key();
                    folly::doNotOptimizeAway(key);
                    auto val = iter->val();
                    reader.reset(*edgeSchema, val);
                    auto props = context->props_;
                    for (const auto& prop : *props) {
                        auto value = nebula::storage::QueryUtils::readValue(
                            reader.get(), prop.name_, prop.field_);
                        CHECK(value.ok());
                        list.emplace_back(std::move(value).value());
                    }
                    cell.values.emplace_back(std::move(list));
                    count++;
                }
                CHECK_EQ(FLAGS_max_rank, count);
            }
            resultDataSet.rows.emplace_back(std::move(row));
        }
        CHECK_EQ(vertex.size(), resultDataSet.rowSize());
        nebula::storage::cpp2::ResponseCommon result;
        resp.set_result(std::move(result));
        resp.set_vertices(std::move(resultDataSet));
        auto encoded = encode(resp);
        folly::doNotOptimizeAway(encoded);
    }
}

void encodeBench(int32_t iters,
        const std::vector<nebula::VertexID>& vertex,
        const std::vector<std::string>& playerProps,
        const std::vector<std::string>& serveProps) {
    nebula::storage::cpp2::GetNeighborsResponse resp;
    BENCHMARK_SUSPEND {
        auto* env = gCluster->storageEnv_.get();
        auto req = nebula::storage::buildRequest(vertex, playerProps, serveProps);
        auto* processor = nebula::storage::GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        resp = std::move(fut).get();
    }
    for (decltype(iters) i = 0; i < iters; i++) {
        auto encoded = encode(resp);
        folly::doNotOptimizeAway(encoded);
    }
}

BENCHMARK(EncodeOneProperty, iters) {
    encodeBench(iters, {"Tim Duncan"}, {"name"}, {"teamName"});
}
BENCHMARK_RELATIVE(EncodeThreeProperty, iters) {
    encodeBench(iters, {"Tim Duncan"}, {"name"}, {"teamName", "startYear", "endYear"});
}
BENCHMARK_RELATIVE(EncodeFiveProperty, iters) {
    encodeBench(iters, {"Tim Duncan"}, {"name"},
                {"teamName", "startYear", "endYear", "teamCareer", "teamGames"});
}

BENCHMARK_DRAW_LINE();

// Players may serve more than one team, the total edges = teamCount * maxRank, which would effect
// the final result, so select some player only serve one team
BENCHMARK(OneVertexOneProperty, iters) {
    go(iters, {"Tim Duncan"}, {"name"}, {"teamName"});
}
BENCHMARK_RELATIVE(OneVertexOnlyId, iters) {
    go(iters, {"Tim Duncan"}, {"name"}, {nebula::kDst});
}
BENCHMARK_RELATIVE(OneVertexThreeProperty, iters) {
    go(iters, {"Tim Duncan"}, {"name"}, {"teamName", "startYear", "endYear"});
}
BENCHMARK_RELATIVE(OneVertexFiveProperty, iters) {
    go(iters, {"Tim Duncan"}, {"name"},
       {"teamName", "startYear", "endYear", "teamCareer", "teamGames"});
}
BENCHMARK_RELATIVE(OneVertexOnePropertyOnlyEdgeNode, iters) {
    goEdgeNode(iters, {"Tim Duncan"}, {"name"}, {"teamName"});
}
BENCHMARK_RELATIVE(OneVertexOneProperyOnlyKV, iters) {
    prefix(iters, {"Tim Duncan"}, {"name"}, {"teamName"});
}

BENCHMARK_DRAW_LINE();

BENCHMARK(NoFilter, iters) {
    go(iters, {"Tim Duncan"}, {"name"}, {"teamName"});
}
BENCHMARK_RELATIVE(OneFilterNonePass, iters) {
    goFilter(iters, {"Tim Duncan"}, {"name"}, {"teamName"}, FLAGS_max_rank * 0);
}
BENCHMARK_RELATIVE(OneFilterFewPass, iters) {
    goFilter(iters, {"Tim Duncan"}, {"name"}, {"teamName"}, FLAGS_max_rank * 0.1);
}
BENCHMARK_RELATIVE(OneFilterHalfPass, iters) {
    goFilter(iters, {"Tim Duncan"}, {"name"}, {"teamName"}, FLAGS_max_rank * 0.5);
}
BENCHMARK_RELATIVE(OneFilterAllPass, iters) {
    goFilter(iters, {"Tim Duncan"}, {"name"}, {"teamName"}, FLAGS_max_rank * 1);
}
BENCHMARK_RELATIVE(TwoFilterNonePass, iters) {
    goFilter(iters, {"Tim Duncan"}, {"name"}, {"teamName"}, FLAGS_max_rank * 0, false);
}
BENCHMARK_RELATIVE(TwoFilterFewPass, iters) {
    goFilter(iters, {"Tim Duncan"}, {"name"}, {"teamName"}, FLAGS_max_rank * 0.1, false);
}
BENCHMARK_RELATIVE(TwoFilterHalfPass, iters) {
    goFilter(iters, {"Tim Duncan"}, {"name"}, {"teamName"}, FLAGS_max_rank * 0.5, false);
}
BENCHMARK_RELATIVE(TwoFilterAllPass, iters) {
    goFilter(iters, {"Tim Duncan"}, {"name"}, {"teamName"}, FLAGS_max_rank * 1, false);
}
BENCHMARK_DRAW_LINE();

BENCHMARK(TenVertexOneProperty, iters) {
    go(iters,
       {"Tim Duncan", "Kobe Bryant", "Stephen Curry", "Manu Ginobili", "Joel Embiid",
        "Giannis Antetokounmpo", "Yao Ming", "Damian Lillard", "Dirk Nowitzki", "Klay Thompson"},
       {"name"},
       {"teamName"});
}
BENCHMARK_RELATIVE(TenVertexOnlyId, iters) {
    go(iters,
       {"Tim Duncan", "Kobe Bryant", "Stephen Curry", "Manu Ginobili", "Joel Embiid",
        "Giannis Antetokounmpo", "Yao Ming", "Damian Lillard", "Dirk Nowitzki", "Klay Thompson"},
       {"name"},
       {nebula::kDst});
}
BENCHMARK_RELATIVE(TenVertexThreeProperty, iters) {
    go(iters,
       {"Tim Duncan", "Kobe Bryant", "Stephen Curry", "Manu Ginobili", "Joel Embiid",
        "Giannis Antetokounmpo", "Yao Ming", "Damian Lillard", "Dirk Nowitzki", "Klay Thompson"},
       {"name"},
       {"teamName", "startYear", "endYear"});
}
BENCHMARK_RELATIVE(TenVertexFiveProperty, iters) {
    go(iters,
       {"Tim Duncan", "Kobe Bryant", "Stephen Curry", "Manu Ginobili", "Joel Embiid",
        "Giannis Antetokounmpo", "Yao Ming", "Damian Lillard", "Dirk Nowitzki", "Klay Thompson"},
       {"name"},
       {"teamName", "startYear", "endYear", "teamCareer", "teamGames"});
}
BENCHMARK_RELATIVE(TenVertexOnePropertyOnlyEdgeNode, iters) {
    goEdgeNode(
        iters,
        {"Tim Duncan", "Kobe Bryant", "Stephen Curry", "Manu Ginobili", "Joel Embiid",
        "Giannis Antetokounmpo", "Yao Ming", "Damian Lillard", "Dirk Nowitzki", "Klay Thompson"},
        {"name"},
        {"teamName"});
}
BENCHMARK_RELATIVE(TenVertexOneProperyOnlyKV, iters) {
    prefix(
        iters,
        {"Tim Duncan", "Kobe Bryant", "Stephen Curry", "Manu Ginobili", "Joel Embiid",
        "Giannis Antetokounmpo", "Yao Ming", "Damian Lillard", "Dirk Nowitzki", "Klay Thompson"},
        {"name"},
        {"teamName"});
}

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    nebula::fs::TempDir rootPath("/tmp/GetNeighborsBenchmark.XXXXXX");
    nebula::storage::setUp(rootPath.path(), FLAGS_max_rank);
    if (FLAGS_go_record) {
        go(100000, {"Tim Duncan"}, {"name"}, {"teamName"});
    } else if (FLAGS_kv_record) {
        prefix(100000, {"Tim Duncan"}, {"name"}, {"teamName"});
    } else {
        folly::runBenchmarks();
    }
    gCluster.reset();
    return 0;
}


/*
40 processors, Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz.
release

--max_rank=1000 --filter_ratio=0.1
============================================================================
GetNeighborsBenchmark.cpprelative                         time/iter  iters/s
============================================================================
EncodeOneProperty                                           65.01us   15.38K
EncodeThreeProperty                               49.36%   131.70us    7.59K
EncodeFiveProperty                                40.26%   161.47us    6.19K
----------------------------------------------------------------------------
OneVertexOneProperty                                       446.58us    2.24K
OneVertexOnlyId                                  119.10%   374.96us    2.67K
OneVertexThreeProperty                            59.50%   750.53us    1.33K
OneVertexFiveProperty                             46.25%   965.55us    1.04K
OneVertexOnePropertyOnlyEdgeNode                 113.92%   392.01us    2.55K
OneVertexOneProperyOnlyKV                        113.08%   394.93us    2.53K
----------------------------------------------------------------------------
NoFilter                                                   444.84us    2.25K
OneFilterNonePass                                106.16%   419.01us    2.39K
OneFilterFewPass                                  98.26%   452.73us    2.21K
OneFilterHalfPass                                 73.95%   601.51us    1.66K
OneFilterAllPass                                  57.33%   775.97us    1.29K
TwoFilterNonePass                                 66.52%   668.76us    1.50K
TwoFilterFewPass                                  62.92%   706.97us    1.41K
TwoFilterHalfPass                                 51.78%   859.14us    1.16K
TwoFilterAllPass                                  42.60%     1.04ms   957.56
----------------------------------------------------------------------------
TenVertexOneProperty                                         4.40ms   227.06
TenVertexOnlyId                                  119.68%     3.68ms   271.76
TenVertexThreeProperty                            59.25%     7.43ms   134.53
TenVertexFiveProperty                             45.67%     9.64ms   103.69
TenVertexOnePropertyOnlyEdgeNode                 109.45%     4.02ms   248.53
TenVertexOneProperyOnlyKV                        109.23%     4.03ms   248.03
============================================================================
*/

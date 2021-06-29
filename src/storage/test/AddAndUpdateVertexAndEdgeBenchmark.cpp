/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/UpdateVertexProcessor.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "common/expression/ConstantExpression.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "codec/test/RowWriterV1.h"
#include <folly/Benchmark.h>

namespace nebula {
namespace storage {

GraphSpaceID spaceId = 1;
VertexID vertexId = "";
TagID tagId = 1;
PartitionID partId = 0;
int32_t spaceVidLen = 0;
VertexID srcId = "";
VertexID dstId = "";
EdgeRanking rank = 0;
EdgeType edgeType = 0;
storage::cpp2::EdgeKey edgeKey;
int parts = 0;
storage::StorageEnv* env;
ObjectPool objPool;
auto pool = &objPool;

bool encodeV2(const meta::NebulaSchemaProvider* schema,
              const std::string& key,
              const std::vector<Value>& props,
              std::vector<kvstore::KV>& data) {
    RowWriterV2 writer(schema);
    for (size_t i = 0; i < props.size(); i++) {
        auto r = writer.setValue(i, props[i]);
        if (r != WriteResult::SUCCEEDED) {
            LOG(ERROR) << "Invalid prop " << i;
            return false;
        }
    }
    auto ret = writer.finish();
    if (ret != WriteResult::SUCCEEDED) {
        LOG(ERROR) << "Failed to write data";
        return false;
    }
    auto encode = std::move(writer).moveEncodedStr();
    data.emplace_back(std::move(key), std::move(encode));
    return true;
}


bool encodeV1(const meta::NebulaSchemaProvider* schema,
              const std::string& key,
              const std::vector<Value>& props,
              std::vector<kvstore::KV>& data) {
    RowWriterV1 writer(schema);
    for (size_t i = 0; i < props.size(); i++) {
        switch (props[i].type()) {
            case Value::Type::INT: {
                writer << props[i].getInt();
                break;
            }
            case Value::Type::STRING: {
                writer << props[i].getStr();
                break;
            }
            case Value::Type::BOOL: {
                writer << props[i].getBool();
                break;
            }
            case Value::Type::FLOAT: {
                writer << props[i].getFloat();
                break;
            }
            case Value::Type::NULLVALUE: {
                writer  << 0;
                break;
            }
            case Value::Type::__EMPTY__: {
                writer  << "";
                break;
            }
            default: {
                LOG(ERROR) << "Failed to write data";
                return false;
            }
        }
    }

    auto val = writer.encode();
    data.emplace_back(std::move(key), std::move(val));
    return true;
}

// add one record
bool mockVertexData(storage::StorageEnv* ev, int32_t totalParts, int32_t vidLen, bool isVersionV2) {
    mock::VertexData vertex;
    vertex.vId_ = "Tim Duncan";
    if (!isVersionV2) {
        vertex.vId_ += "v1";
    }
    vertex.tId_ = 1;

    std::vector<Value> props;
    props.emplace_back(vertex.vId_);
    props.emplace_back(44);
    props.emplace_back(false);
    props.emplace_back(19);
    props.emplace_back(1997);
    props.emplace_back(2016);
    props.emplace_back(1392);
    props.emplace_back(19.0);
    props.emplace_back(1);
    props.emplace_back("America");
    props.emplace_back(5);
    vertex.props_ = std::move(props);
    auto pId = std::hash<std::string>()(vertex.vId_) % totalParts + 1;

    folly::Baton<true, std::atomic> baton;
    std::atomic<size_t> count(1);
    std::vector<kvstore::KV> data;

    auto key = NebulaKeyUtils::vertexKey(vidLen,
                                         pId,
                                         vertex.vId_,
                                         vertex.tId_);
    auto schema = ev->schemaMan_->getTagSchema(spaceId, vertex.tId_);
    if (!schema) {
        LOG(ERROR) << "Invalid tagId " << vertex.tId_;
        return false;
    }

    bool ret;
    if (isVersionV2) {
        ret = encodeV2(schema.get(), key, vertex.props_, data);
    } else {
        ret = encodeV1(schema.get(), key, vertex.props_, data);
    }
    if (!ret) {
        LOG(ERROR) << "Write field failed";
        return false;
    }

    ev->kvstore_->asyncMultiPut(spaceId, pId, std::move(data),
                                [&](nebula::cpp2::ErrorCode code) {
                                    ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);
                                    count.fetch_sub(1);
                                    if (count.load() == 0) {
                                        baton.post();
                                    }
                                });

    baton.wait();
    return true;
}

// add one record
bool mockEdgeData(storage::StorageEnv* ev, int32_t totalParts, int32_t vidLen, bool isVersionV2) {
    mock::EdgeData edgedata;
    edgedata.srcId_ = "Tim Duncan";
    if (!isVersionV2) {
        edgedata.srcId_ += "v1";
    }
    edgedata.type_ = 101;
    edgedata.rank_ = 0;
    edgedata.dstId_ = "Spurs";

    std::vector<Value> props;
    props.emplace_back(edgedata.srcId_);
    props.emplace_back(edgedata.dstId_);
    props.emplace_back(edgedata.rank_);
    props.emplace_back(2016);
    props.emplace_back(19);
    props.emplace_back(1392);
    props.emplace_back(19.0);
    props.emplace_back("zzzzz");
    props.emplace_back(5);
    auto pId = std::hash<std::string>()(edgedata.srcId_) % totalParts + 1;

    folly::Baton<true, std::atomic> baton;
    std::atomic<size_t> count(1);
    std::vector<kvstore::KV> data;

    // Switch version to big-endian, make sure the key is in ordered.
    auto version = std::numeric_limits<int64_t>::max() - 0L;
    version = folly::Endian::big(version);
    auto key = NebulaKeyUtils::edgeKey(vidLen,
                                       pId,
                                       edgedata.srcId_,
                                       edgedata.type_,
                                       edgedata.rank_,
                                       edgedata.dstId_,
                                       version);
    auto schema = ev->schemaMan_->getEdgeSchema(spaceId, std::abs(edgedata.type_));
    if (!schema) {
        LOG(ERROR) << "Invalid edge " << edgedata.type_;
        return false;
    }

    bool ret;
    if (isVersionV2) {
        ret = encodeV2(schema.get(), key, edgedata.props_, data);
    } else {
        ret = encodeV1(schema.get(), key, edgedata.props_, data);
    }

    if (!ret) {
        LOG(ERROR) << "Write field failed";
        return false;
    }
    ev->kvstore_->asyncMultiPut(spaceId, pId, std::move(data),
                                [&](nebula::cpp2::ErrorCode code) {
                                    ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);
                                    count.fetch_sub(1);
                                    if (count.load() == 0) {
                                        baton.post();
                                    }
                                });
    baton.wait();
    return true;
}

void setUp(storage::StorageEnv* ev) {
    auto status = ev->schemaMan_->getSpaceVidLen(spaceId);
    if (!status.ok()) {
        LOG(ERROR) << "Get space vidLen failed";
        return;
    }
    spaceVidLen = status.value();

    // v2 data
    if (!mockVertexData(ev, parts, spaceVidLen, true)) {
        LOG(ERROR) << "Mock data faild";
        return;
    }
    // v1 data
    if (!mockVertexData(ev, parts, spaceVidLen, false)) {
        LOG(ERROR) << "Mock data faild";
        return;
    }
    // v2 data
    if (!mockEdgeData(ev, parts, spaceVidLen, true)) {
        LOG(ERROR) << "Mock data faild";
        return;
    }
    // v1 data
    if (!mockEdgeData(ev, parts, spaceVidLen, false)) {
        LOG(ERROR) << "Mock data faild";
        return;
    }
}

cpp2::AddVerticesRequest buildAddVertexReq() {
    cpp2::AddVerticesRequest req;
    req.set_space_id(1);
    req.set_if_not_exists(true);

    vertexId = "Tony Parker";

    nebula::storage::cpp2::NewVertex newVertex;
    nebula::storage::cpp2::NewTag newTag;
    partId = std::hash<std::string>()(vertexId) % parts + 1;

    std::vector<Value> props;
    props.emplace_back(vertexId);
    props.emplace_back(38);
    props.emplace_back(false);
    props.emplace_back(18);
    props.emplace_back(2001);
    props.emplace_back(2019);
    props.emplace_back(1254);
    props.emplace_back(15.5);
    props.emplace_back(2);
    props.emplace_back("France");
    props.emplace_back(5);
    newTag.set_tag_id(1);
    newTag.set_props(std::move(props));

    std::vector<nebula::storage::cpp2::NewTag> newTags;
    newTags.push_back(std::move(newTag));

    newVertex.set_id(vertexId);
    newVertex.set_tags(std::move(newTags));
    (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    return req;
}

cpp2::AddEdgesRequest buildAddEdgeReq() {
    cpp2::AddEdgesRequest req;
    srcId = "Tony Parker";
    dstId = "Spurs";
    req.set_space_id(1);
    req.set_if_not_exists(true);

    nebula::storage::cpp2::NewEdge newEdge;
    partId = std::hash<std::string>()(srcId) % parts + 1;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(101);
    edgeKey.set_ranking(0);
    edgeKey.set_dst(dstId);

    std::vector<Value> props;
    props.emplace_back(srcId);
    props.emplace_back(dstId);
    props.emplace_back(2001);
    props.emplace_back(2018);
    props.emplace_back(17);
    props.emplace_back(1198);
    props.emplace_back(16.6);
    props.emplace_back("trade");
    props.emplace_back(4);
    newEdge.set_key(std::move(edgeKey));
    newEdge.set_props(std::move(props));

    (*req.parts_ref())[partId].emplace_back(std::move(newEdge));
    return req;
}

cpp2::UpdateVertexRequest buildUpdateVertexReq(bool isVersionV2) {
    cpp2::UpdateVertexRequest req;
    req.set_space_id(spaceId);
    req.set_tag_id(tagId);

    vertexId = "Tim Duncan";
    if (!isVersionV2) {
        vertexId += "v1";
    }
    partId = std::hash<std::string>()(vertexId) % parts + 1;
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);

    // Build updated props
    std::vector<cpp2::UpdatedProp> updatedProps;
    // int: player.age = 45
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("age");
    const auto& val1 = *ConstantExpression::make(pool, 45L);
    uProp1.set_value(Expression::encode(val1));
    updatedProps.emplace_back(uProp1);

    // string: player.country= China
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("country");
    std::string col4new("China");
    const auto& val2 = *ConstantExpression::make(pool, col4new);
    uProp2.set_value(Expression::encode(val2));
    updatedProps.emplace_back(uProp2);
    req.set_updated_props(std::move(updatedProps));

    // Build yield
    // Return player props: name, age, country
    std::vector<std::string> tmpProps;
    const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "name");
    tmpProps.emplace_back(Expression::encode(sourcePropExp1));

    const auto& sourcePropExp2 = *SourcePropertyExpression::make(pool, "1", "age");
    tmpProps.emplace_back(Expression::encode(sourcePropExp2));

    const auto& sourcePropExp3 = *SourcePropertyExpression::make(pool, "1", "country");
    tmpProps.emplace_back(Expression::encode(sourcePropExp3));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);
    return req;
}


cpp2::UpdateEdgeRequest buildUpdateEdgeReq(bool isVersionV2) {
    cpp2::UpdateEdgeRequest req;
    req.set_space_id(spaceId);

    srcId = "Tim Duncan";
    if (!isVersionV2) {
        srcId += "v1";
    }

    partId = std::hash<std::string>()(srcId) % parts + 1;
    req.set_part_id(partId);

    dstId = "Spurs";
    rank = 0;
    edgeType = 101;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    // Build updated props
    std::vector<cpp2::UpdatedProp> updatedProps;
    // int: 101.teamCareer = 20
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("teamCareer");
    const auto& val1 = *ConstantExpression::make(pool, 20L);
    uProp1.set_value(Expression::encode(val1));
    updatedProps.emplace_back(uProp1);

    // bool: 101.type = trade
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("type");
    std::string colnew("trade");
    const auto& val2 = *ConstantExpression::make(pool, colnew);
    uProp2.set_value(Expression::encode(val2));
    updatedProps.emplace_back(uProp2);
    req.set_updated_props(std::move(updatedProps));

    // Return serve props: playerName, teamName, teamCareer, type
    std::vector<std::string> tmpProps;
    const auto& edgePropExp1 = *EdgePropertyExpression::make(pool, "101", "playerName");
    tmpProps.emplace_back(Expression::encode(edgePropExp1));

    const auto& edgePropExp2 = *EdgePropertyExpression::make(pool, "101", "teamName");
    tmpProps.emplace_back(Expression::encode(edgePropExp2));

    const auto& edgePropExp3 = *EdgePropertyExpression::make(pool, "101", "teamCareer");
    tmpProps.emplace_back(Expression::encode(edgePropExp3));

    const auto& edgePropExp4 = *EdgePropertyExpression::make(pool, "101", "type");
    tmpProps.emplace_back(Expression::encode(edgePropExp4));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);
    return req;
}

}  // namespace storage
}  // namespace nebula

void insertVertex(int32_t iters) {
    nebula::storage::cpp2::AddVerticesRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildAddVertexReq();
    }

    for (decltype(iters) i = 0; i < iters; i++) {
        // Test AddVertexRequest
        auto* processor
            = nebula::storage::AddVerticesProcessor::instance(nebula::storage::env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        if (!resp.result.failed_parts.empty()) {
            LOG(ERROR) << "Add faild";
            return;
        }
    }
}


void insertEdge(int32_t iters) {
    nebula::storage::cpp2::AddEdgesRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildAddEdgeReq();
    }

    for (decltype(iters) i = 0; i < iters; i++) {
        // Test AddVertexRequest
        auto* processor
            = nebula::storage::AddEdgesProcessor::instance(nebula::storage::env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        if (!resp.result.failed_parts.empty()) {
            LOG(ERROR) << "Add faild";
            return;
        }
    }
}


void updateVertex(int32_t iters, bool isVersion2) {
    nebula::storage::cpp2::UpdateVertexRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildUpdateVertexReq(isVersion2);
    }

    for (decltype(iters) i = 0; i < iters; i++) {
        // Test UpdateVertexRequest
        auto* processor
            = nebula::storage::UpdateVertexProcessor::instance(nebula::storage::env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        if (!resp.result.failed_parts.empty()) {
            LOG(ERROR) << "update faild";
            return;
        }
    }
}

void updateEdge(int32_t iters, bool isVersion2) {
    nebula::storage::cpp2::UpdateEdgeRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildUpdateEdgeReq(isVersion2);
    }

    for (decltype(iters) i = 0; i < iters; i++) {
        // Test UpdateEdgeRequest
        auto* processor
            =  nebula::storage::UpdateEdgeProcessor::instance(nebula::storage::env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        if (!resp.result.failed_parts.empty()) {
            LOG(ERROR) << "update faild";
            return;
        }
    }
}

BENCHMARK(update_vertexV1, iters) {
    updateVertex(iters, false);
}

BENCHMARK_RELATIVE(update_vertexV2, iters) {
    updateVertex(iters, true);
}

BENCHMARK(update_edgeV1, iters) {
    updateEdge(iters, false);
}

BENCHMARK_RELATIVE(update_edgeV2, iters) {
    updateEdge(iters, true);
}

BENCHMARK(insert_vertexV2, iters) {
    insertVertex(iters);
}

BENCHMARK(insert_edgeV2, iters) {
    insertEdge(iters);
}

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    nebula::fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    nebula::mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    nebula::storage::env = cluster.storageEnv_.get();
    nebula::storage::parts = cluster.getTotalParts();
    nebula::storage::setUp(nebula::storage::env);
    folly::runBenchmarks();
    return 0;
}


/**

CPU : Intel(R) Xeon(R) CPU E5-2697 v3 @ 2.60GHz

release version
when update, there are four record(two tag record and two edge record)
before insert, there are four record(two tag record and two edge record)

update_vertex   : tag data exist and update

update_edge     : edge data exist and update

insert_vertex   : insert one record of one tag of one vertex

insert_edge     : insert one record of one edge


V1.0 in nebula 1.0
==============================================================================
src/storage/test/UpdateVertexAndEdgeBenchmark.cpprelative  time/iter  iters/s
==============================================================================
update_vertex                                               46.54us   21.49K
update_edge                                                 48.17us   20.76K
insert_vertex                                               17.07us   58.60K
insert_edge                                                 18.03us   55.48K
==============================================================================

v1.0 v2.0 in nbuela 2.0
==============================================================================
src/storage/test/UpdateVertexAndEdgeBenchmark.cpprelative  time/iter  iters/s
==============================================================================
update_vertexV1                                             45.58us   21.94K
update_vertexV2                                   98.65%    46.21us   21.64K
update_edgeV1                                               58.40us   17.12K
update_edgeV2                                    100.21%    58.28us   17.16K
insert_vertexV2                                             18.14us   55.11K
insert_edgeV2                                               30.94us   32.32K
==============================================================================
**/

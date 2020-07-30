/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/QueryBoundProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

DEFINE_int32(reserved_edges_one_vertex, 1024, "reserve edges for one vertex");

namespace nebula {
namespace storage {

kvstore::ResultCode QueryBoundProcessor::processEdgeImpl(const PartitionID partId,
                                                         const VertexID vId,
                                                         const EdgeType edgeType,
                                                         const std::vector<PropContext>& props,
                                                         FilterContext& fcontext,
                                                         cpp2::VertexData& vdata) {
    bool onlyStructure = onlyStructures_[edgeType];
    std::shared_ptr<meta::SchemaProviderIf> currEdgeSchema;
    if (!onlyStructure) {
        auto schema = edgeSchema_.find(edgeType);
        if (schema == edgeSchema_.end()) {
            LOG(ERROR) << "Not found the edge type: " << edgeType;
            return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
        }
        currEdgeSchema = schema->second;
    }
    std::vector<cpp2::IdAndProp> edges;
    edges.reserve(FLAGS_reserved_edges_one_vertex);
    auto ret = collectEdgeProps(
        partId, vId, edgeType, &fcontext,
        [&, this](RowReader reader, folly::StringPiece k) {
            cpp2::IdAndProp edge;
            if (!onlyStructure) {
                RowWriter writer(currEdgeSchema);
                PropsCollector collector(&writer);
                this->collectProps(reader.get(), k, props, &fcontext, &collector);
                edge.set_dst(collector.getDstId());
                edge.set_props(writer.encode());
            } else {
                PropsCollector collector(nullptr);
                this->collectProps(reader.get(), k, props, &fcontext, &collector);
                edge.set_dst(collector.getDstId());
            }
            edges.emplace_back(std::move(edge));
        });
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return ret;
    }
    if (!edges.empty()) {
        cpp2::EdgeData edgeData;
        edgeData.set_type(edgeType);
        edgeData.set_edges(std::move(edges));
        vdata.edge_data.emplace_back(std::move(edgeData));
    }
    return ret;
}

kvstore::ResultCode QueryBoundProcessor::processEdge(PartitionID partId, VertexID vId,
                                                     FilterContext& fcontext,
                                                     cpp2::VertexData& vdata) {
    for (const auto& ec : edgeContexts_) {
        auto edgeType = ec.first;
        auto& props   = ec.second;
        if (!props.empty()) {
            CHECK(!onlyVertexProps_);
            auto ret = processEdgeImpl(partId, vId, edgeType, props, fcontext, vdata);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }
    }

    return kvstore::ResultCode::SUCCEEDED;
}

kvstore::ResultCode QueryBoundProcessor::processEdgeSampling(const PartitionID partId,
                                                             const VertexID vId,
                                                             FilterContext& fcontext,
                                                             cpp2::VertexData& vdata) {
    using Sample = std::tuple<
        EdgeType, /* type */
        std::string, /* key */
        std::unique_ptr<RowReader>, /* val */
        std::shared_ptr<meta::SchemaProviderIf>, /* schema of this value*/
        const std::vector<PropContext>* /* props needed */>;
    auto sampler = std::make_unique<
                    nebula::algorithm::ReservoirSampling<Sample>
                   >(FLAGS_max_edge_returned_per_vertex);

    for (const auto& ec : edgeContexts_) {
        auto edgeType = ec.first;
        auto* props   = &ec.second;
        bool onlyStructure = onlyStructures_[edgeType];
        std::shared_ptr<meta::SchemaProviderIf> currEdgeSchema;
        if (!onlyStructure) {
            auto schema = edgeSchema_.find(edgeType);
            if (schema == edgeSchema_.end()) {
                LOG(ERROR) << "Not found the edge type: " << edgeType;
                return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
            }
            currEdgeSchema = schema->second;
        }
        if (!props->empty()) {
            CHECK(!onlyVertexProps_);
            auto ret = collectEdgeProps(
                partId, vId, edgeType, &fcontext,
                [&](RowReader reader, folly::StringPiece k) {
                    sampler->sampling(
                        std::make_tuple(
                            edgeType, k.str(),
                            std::make_unique<RowReader>(std::move(reader)),
                            currEdgeSchema, props));
                });
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }
    }

    std::unordered_map<EdgeType, cpp2::EdgeData> edgeDataMap;
    auto samples = std::move(*sampler).samples();
    for (auto& sample : samples) {
        auto edgeType = std::get<0>(sample);
        auto currEdgeSchema = std::get<3>(sample);
        auto& props = *std::get<4>(sample);
        bool onlyStructure = onlyStructures_[edgeType];
        cpp2::IdAndProp edge;
        if (!onlyStructure) {
            RowWriter writer(currEdgeSchema);
            PropsCollector collector(&writer);
            this->collectProps(
                    std::get<2>(sample).get(), std::get<1>(sample), props, &fcontext, &collector);
            edge.set_dst(collector.getDstId());
            edge.set_props(writer.encode());
        } else {
            PropsCollector collector(nullptr);
            this->collectProps(
                    std::get<2>(sample).get(), std::get<1>(sample), props, &fcontext, &collector);
            edge.set_dst(collector.getDstId());
        }
        auto edges = edgeDataMap.find(edgeType);
        if (edges == edgeDataMap.end()) {
            cpp2::EdgeData edgeData;
            edgeData.set_type(edgeType);
            edgeData.edges.emplace_back(std::move(edge));
            edgeDataMap.emplace(edgeType, std::move(edgeData));
        } else {
            edges->second.edges.emplace_back(std::move(edge));
        }
    }

    std::transform(edgeDataMap.begin(), edgeDataMap.end(), std::back_inserter(vdata.edge_data),
            [] (auto& data) {
                return std::move(data).second;
            });
    return kvstore::ResultCode::SUCCEEDED;
}

kvstore::ResultCode QueryBoundProcessor::processVertex(PartitionID partId, VertexID vId) {
    cpp2::VertexData vResp;
    vResp.set_vertex_id(vId);
    FilterContext fcontext;
    if (!tagContexts_.empty()) {
        std::vector<cpp2::TagData> td;
        for (auto& tc : tagContexts_) {
            auto schema = vertexSchema_.find(tc.tagId_);
            if (schema == vertexSchema_.end()) {
                LOG(ERROR) << "Not found the tag: " << tc.tagId_;
                return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
            }
            RowWriter writer(schema->second);
            PropsCollector collector(&writer);
            VLOG(3) << "partId " << partId << ", vId " << vId << ", tagId " << tc.tagId_
                    << ", prop size " << tc.props_.size();
            auto ret = collectVertexProps(partId, vId, tc.tagId_, tc.props_, &fcontext, &collector);
            if (ret == kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                continue;
            }
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
            if (writer.size() > 1) {
                cpp2::TagData tagData;
                tagData.set_tag_id(tc.tagId_);
                tagData.set_data(writer.encode());
                td.emplace_back(std::move(tagData));
            }
        }
        vResp.set_tag_data(std::move(td));
    }

    if (onlyVertexProps_) {
        std::lock_guard<std::mutex> lg(this->lock_);
        vertices_.emplace_back(std::move(vResp));
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode ret;
    if (FLAGS_enable_reservoir_sampling) {
        ret = processEdgeSampling(partId, vId, fcontext, vResp);
    } else {
        ret = processEdge(partId, vId, fcontext, vResp);
    }

    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return ret;
    }

    if (!vResp.edge_data.empty()) {
        // Only return the vertex if edges existed.
        std::lock_guard<std::mutex> lg(this->lock_);
        for (auto& edata : vResp.edge_data) {
            totalEdges_ += edata.edges.size();
        }
        vertices_.emplace_back(std::move(vResp));
    }

    return kvstore::ResultCode::SUCCEEDED;
}

void QueryBoundProcessor::onProcessFinished(int32_t retNum) {
    (void)retNum;
    resp_.set_vertices(std::move(vertices_));
    resp_.set_total_edges(totalEdges_);
    if (!vertexSchemaResp_.empty()) {
        resp_.set_vertex_schema(std::move(vertexSchemaResp_));
    }

    if (!edgeSchemaResp_.empty()) {
        resp_.set_edge_schema(std::move(edgeSchemaResp_));
    }
}

}  // namespace storage
}  // namespace nebula

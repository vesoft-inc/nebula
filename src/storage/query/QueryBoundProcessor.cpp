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

namespace nebula {
namespace storage {

kvstore::ResultCode QueryBoundProcessor::processEdgeImpl(const PartitionID partId,
                                                         const VertexID vId,
                                                         const EdgeType edgeType,
                                                         const std::vector<PropContext>& props,
                                                         FilterContext& fcontext,
                                                         cpp2::VertexData& vdata) {
    RowSetWriter rsWriter;
    auto ret = collectEdgeProps(
        partId, vId, edgeType, props, &fcontext,
        [&, this](RowReader* reader, folly::StringPiece k, const std::vector<PropContext>& p) {
            RowWriter writer(rsWriter.schema());
            PropsCollector collector(&writer);
            this->collectProps(reader, k, p, &fcontext, &collector);
            rsWriter.addRow(writer);
        });
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return ret;
    }

    if (!rsWriter.data().empty()) {
        vdata.edge_data.emplace_back(apache::thrift::FragileConstructor::FRAGILE, edgeType,
                                     std::move(rsWriter.data()));
    }

    return ret;
}

kvstore::ResultCode QueryBoundProcessor::processEdge(PartitionID partId, VertexID vId,
                                                     FilterContext& fcontext,
                                                     cpp2::VertexData& vdata) {
    for (const auto& ec : edgeContexts_) {
        RowSetWriter rsWriter;
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

kvstore::ResultCode QueryBoundProcessor::processVertex(PartitionID partId, VertexID vId) {
    cpp2::VertexData vResp;
    vResp.set_vertex_id(vId);
    FilterContext fcontext;
    if (!tagContexts_.empty()) {
        std::vector<cpp2::TagData> td;
        for (auto& tc : tagContexts_) {
            RowWriter writer;
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
                td.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                tc.tagId_,
                                writer.encode());
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
    ret = processEdge(partId, vId, fcontext, vResp);

    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return ret;
    }

    if (!vResp.edge_data.empty()) {
        // Only return the vertex if edges existed.
        std::lock_guard<std::mutex> lg(this->lock_);
        vertices_.emplace_back(std::move(vResp));
    }

    return kvstore::ResultCode::SUCCEEDED;
}

void QueryBoundProcessor::onProcessFinished(int32_t retNum) {
    (void)retNum;
    resp_.set_vertices(std::move(vertices_));
    std::unordered_map<TagID, nebula::cpp2::Schema> vertexSchema;
    if (!this->tagContexts_.empty()) {
        for (auto& tc : this->tagContexts_) {
            nebula::cpp2::Schema respTag;
            for (auto& prop : tc.props_) {
                if (prop.returned_) {
                    respTag.columns.emplace_back(
                        columnDef(std::move(prop.prop_.name), prop.type_.type));
                }
            }

            if (!respTag.columns.empty()) {
                auto it = vertexSchema.find(tc.tagId_);
                if (it == vertexSchema.end()) {
                    vertexSchema.emplace(tc.tagId_, respTag);
                }
            }
        }
        if (!vertexSchema.empty()) {
            resp_.set_vertex_schema(std::move(vertexSchema));
        }
    }

    std::unordered_map<EdgeType, nebula::cpp2::Schema> edgeSchema;
    if (!edgeContexts_.empty()) {
        for (const auto& ec : edgeContexts_) {
            nebula::cpp2::Schema respEdge;
            RowSetWriter rsWriter;
            auto& props = ec.second;
            for (auto& p : props) {
                respEdge.columns.emplace_back(columnDef(std::move(p.prop_.name), p.type_.type));
            }

            if (!respEdge.columns.empty()) {
                auto it = edgeSchema.find(ec.first);
                if (it == edgeSchema.end()) {
                    edgeSchema.emplace(ec.first, std::move(respEdge));
                }
            }
        }

        if (!edgeSchema.empty()) {
            resp_.set_edge_schema(std::move(edgeSchema));
        }
    }
}

}  // namespace storage
}  // namespace nebula

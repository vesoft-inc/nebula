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
    auto schema = edgeSchema_.find(edgeType);
    if (schema == edgeSchema_.end()) {
        LOG(ERROR) << "Not found the edge type: " << edgeType;
        return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
    }
    RowSetWriter rsWriter(std::move(schema)->second);
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
        cpp2::EdgeData edgeData;
        edgeData.set_type(edgeType);
        edgeData.set_data(std::move(rsWriter.data()));
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

    if (!vertexSchemaResp_.empty()) {
        resp_.set_vertex_schema(std::move(vertexSchemaResp_));
    }

    if (!edgeSchemaResp_.empty()) {
        resp_.set_edge_schema(std::move(edgeSchemaResp_));
    }
}

}  // namespace storage
}  // namespace nebula

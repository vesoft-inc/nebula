/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/QueryBoundProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace storage {

kvstore::ResultCode QueryBoundProcessor::processVertex(PartitionID partId,
                                                       VertexID vId) {
    cpp2::VertexData vResp;
    vResp.set_vertex_id(vId);
    if (!this->tagContexts_.empty()) {
        RowWriter writer;
        PropsCollector collector(&writer);
        for (auto& tc : this->tagContexts_) {
            VLOG(3) << "partId " << partId << ", vId " << vId
                    << ", tagId " << tc.tagId_ << ", prop size " << tc.props_.size();
            auto ret = collectVertexProps(partId, vId, tc.tagId_, tc.props_, &collector);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }
        vResp.set_vertex_data(writer.encode());
    }

    if (!this->edgeContext_.props_.empty()) {
        RowSetWriter rsWriter;
        auto ret = collectEdgeProps(partId, vId,
                                    this->edgeContext_.edgeType_,
                                    this->edgeContext_.props_,
                                    [&, this] (RowReader* reader,
                                               folly::StringPiece key,
                                               const std::vector<PropContext>& props) {
                                        RowWriter writer(rsWriter.schema());
                                        PropsCollector collector(&writer);
                                        this->collectProps(reader, key, props, &collector);
                                        rsWriter.addRow(writer);
                                    });
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        if (!rsWriter.data().empty()) {
            vResp.set_edge_data(std::move(rsWriter.data()));
        }
    }
    {
        std::lock_guard<std::mutex> lg(this->lock_);
        vertices_.emplace_back(std::move(vResp));
    }
    return kvstore::ResultCode::SUCCEEDED;
}


void QueryBoundProcessor::onProcessFinished(int32_t retNum) {
    resp_.set_vertices(std::move(vertices_));
    if (!this->tagContexts_.empty()) {
        nebula::cpp2::Schema respTag;
        respTag.columns.reserve(retNum - this->edgeContext_.props_.size());
        for (auto& tc : this->tagContexts_) {
            for (auto& prop : tc.props_) {
                respTag.columns.emplace_back(columnDef(std::move(prop.prop_.name),
                                                       prop.type_.type));
            }
        }
        if (!respTag.get_columns().empty()) {
            resp_.set_vertex_schema(std::move(respTag));
        }
    }
    if (!this->edgeContext_.props_.empty()) {
        nebula::cpp2::Schema respEdge;
        decltype(respEdge.columns) cols;
        cols.reserve(this->edgeContext_.props_.size());
        for (auto& prop : this->edgeContext_.props_) {
            cols.emplace_back(columnDef(std::move(prop.prop_.name),
                                                  prop.type_.type));
        }
        respEdge.set_columns(std::move(cols));
        if (!respEdge.get_columns().empty()) {
            resp_.set_edge_schema(std::move(respEdge));
        }
    }
}

}  // namespace storage
}  // namespace nebula

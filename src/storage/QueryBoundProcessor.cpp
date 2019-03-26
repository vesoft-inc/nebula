/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "storage/QueryBoundProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "storage/KeyUtils.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace storage {

kvstore::ResultCode QueryBoundProcessor::processVertex(PartitionID partId,
                                                       VertexID vId,
                                                       std::vector<TagContext>& tagContexts,
                                                       EdgeContext& edgeContext) {
    cpp2::VertexData vResp;
    vResp.set_vertex_id(vId);
    if (!tagContexts.empty()) {
        RowWriter writer;
        PropsCollector collector(&writer);
        for (auto& tc : tagContexts) {
            VLOG(3) << "partId " << partId << ", vId " << vId
                    << ", tagId " << tc.tagId_ << ", prop size " << tc.props_.size();
            auto ret = collectVertexProps(partId, vId, tc.tagId_, tc.props_, &collector);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }
        vResp.set_vertex_data(writer.encode());
    }

    if (!edgeContext.props_.empty()) {
        RowSetWriter rsWriter;
        auto ret = collectEdgeProps(partId, vId,
                                    edgeContext.edgeType_,
                                    edgeContext.props_,
                                    [&, this] (RowReader* reader,
                                               folly::StringPiece key,
                                               std::vector<PropContext>& props) {
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
    vertices_.emplace_back(std::move(vResp));
    return kvstore::ResultCode::SUCCEEDED;
}


void QueryBoundProcessor::onProcessed(std::vector<TagContext>& tagContexts,
                                      EdgeContext& edgeContext, int32_t retNum) {
    resp_.set_vertices(std::move(vertices_));
    if (!tagContexts.empty()) {
        nebula::cpp2::Schema respTag;
        respTag.columns.reserve(retNum - edgeContext.props_.size());
        for (auto& tc : tagContexts) {
            for (auto& prop : tc.props_) {
                respTag.columns.emplace_back(columnDef(std::move(prop.prop_.name),
                                                       prop.type_.type));
            }
        }
        if (!respTag.get_columns().empty()) {
            resp_.set_vertex_schema(std::move(respTag));
        }
    }
    if (!edgeContext.props_.empty()) {
        nebula::cpp2::Schema respEdge;
        decltype(respEdge.columns) cols;
        cols.reserve(edgeContext.props_.size());
        for (auto& prop : edgeContext.props_) {
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

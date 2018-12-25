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

kvstore::ResultCode QueryBoundProcessor::collectVertexProps(
                            PartitionID partId,
                            VertexID vId,
                            TagID tagId,
                            SchemaProviderIf* tagSchema,
                            std::vector<PropContext>& props,
                            RowWriter& writer) {
    auto prefix = KeyUtils::prefix(partId, vId, tagId);
    std::unique_ptr<kvstore::StorageIter> iter;
    auto ret = kvstore_->prefix(spaceId_, partId, prefix, iter);
    if (ret != kvstore::ResultCode::SUCCESSED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
        return ret;
    }
    // Only get the latest version.
    if (iter && iter->valid()) {
        auto key = iter->key();
        auto val = iter->val();
        PropsCollector collector(&writer);
        this->collectProps(tagSchema, key, val, props, &collector);
    } else {
        VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagId;
    }
    return ret;
}

kvstore::ResultCode QueryBoundProcessor::collectEdgeProps(
                                               PartitionID partId,
                                               VertexID vId,
                                               EdgeType edgeType,
                                               SchemaProviderIf* schema,
                                               std::vector<PropContext>& props,
                                               RowSetWriter& rsWriter) {
    auto prefix = KeyUtils::prefix(partId, vId, edgeType);
    std::unique_ptr<kvstore::StorageIter> iter;
    auto ret = kvstore_->prefix(spaceId_, partId, prefix, iter);
    while(iter && iter->valid()) {
        RowWriter writer;
        auto key = iter->key();
        auto val = iter->val();
        PropsCollector collector(&writer);
        this->collectProps(schema, key, val, props, &collector);
        iter->next();
        rsWriter.addRow(writer);
    }
    return ret;
}

kvstore::ResultCode QueryBoundProcessor::processVertex(PartitionID partId, VertexID vId,
                                                       std::vector<TagContext>& tagContexts,
                                                       EdgeContext& edgeContext) {
    cpp2::VertexResponse vResp;
    vResp.vertex_id = vId;
    if (tagContexts.size() > 0) {
        RowWriter writer;
        for (auto& tc : tagContexts) {
            VLOG(3) << "partId " << partId << ", vId " << vId
                    << ", tagId " << tc.tagId_ << ", prop size " << tc.props_.size();
            auto ret = collectVertexProps(partId, vId, tc.tagId_, tc.schema_, tc.props_, writer);
            if (ret != kvstore::ResultCode::SUCCESSED) {
                return ret;
            }
        }
        vResp.vertex_data = std::move(writer.encode());
    }
    if (edgeContext.schema_ != nullptr) {
        RowSetWriter rsWriter;
        auto ret = collectEdgeProps(partId, vId, edgeContext.edgeType_,
                                    edgeContext.schema_, edgeContext.props_, rsWriter);
        if (ret != kvstore::ResultCode::SUCCESSED) {
            return ret;
        }
        vResp.edge_data = std::move(rsWriter.data());
    }
    resp_.vertices.emplace_back(std::move(vResp));
    return kvstore::ResultCode::SUCCESSED;
}

void QueryBoundProcessor::onProcessed(std::vector<TagContext>& tagContexts,
                                      EdgeContext& edgeContext, int32_t retNum) {
    if (tagContexts.size() > 0) {
        cpp2::Schema respTag;
        respTag.columns.reserve(retNum - edgeContext.props_.size());
        for (auto& tc : tagContexts) {
            for (auto& prop : tc.props_) {
                respTag.columns.emplace_back(columnDef(std::move(prop.prop_.name),
                                                       prop.type_.type));
            }
        }
        resp_.vertex_schema = std::move(respTag);
    }
    if (edgeContext.schema_ != nullptr) {
        cpp2::Schema respEdge;
        respEdge.columns.reserve(edgeContext.props_.size());
        for (auto& prop : edgeContext.props_) {
            respEdge.columns.emplace_back(columnDef(std::move(prop.prop_.name),
                                                    prop.type_.type));
        }
        resp_.edge_schema = std::move(respEdge);
    }
}

}  // namespace storage
}  // namespace nebula

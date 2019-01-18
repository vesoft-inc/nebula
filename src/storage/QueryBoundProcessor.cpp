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
                            std::vector<PropContext>& props,
                            RowWriter& writer) {
    auto prefix = KeyUtils::prefix(partId, vId, tagId);
    std::unique_ptr<kvstore::StorageIter> iter;
    auto ret = kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCESSED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
        return ret;
    }
    // Will decode the properties according to teh schema version
    // stored along with the properties
    if (iter && iter->valid()) {
        PropsCollector collector(&writer);
        auto reader = RowReader::getTagPropReader(iter->val(), spaceId_, tagId);
        this->collectProps(reader.get(), props, &collector);
    } else {
        VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagId;
    }
    return ret;
}


kvstore::ResultCode QueryBoundProcessor::collectEdgeProps(
                                               PartitionID partId,
                                               VertexID vId,
                                               EdgeType edgeType,
                                               std::vector<PropContext>& props,
                                               RowSetWriter& rsWriter) {
    auto prefix = KeyUtils::prefix(partId, vId, edgeType);
    std::unique_ptr<kvstore::StorageIter> iter;
    auto ret = kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCESSED || !iter) {
        return ret;
    }
    while (iter->valid()) {
        RowWriter writer(rsWriter.schema());
        PropsCollector collector(&writer);
        auto reader = RowReader::getEdgePropReader(iter->val(), spaceId_, edgeType);
        this->collectProps(reader.get(), props, &collector);
        rsWriter.addRow(writer);

        iter->next();
    }
    return ret;
}


kvstore::ResultCode QueryBoundProcessor::processVertex(PartitionID partId,
                                                       VertexID vId,
                                                       std::vector<TagContext>& tagContexts,
                                                       EdgeContext& edgeContext) {
    cpp2::VertexData vResp;
    vResp.vertex_id = vId;
    if (!tagContexts.empty()) {
        RowWriter writer;
        for (auto& tc : tagContexts) {
            VLOG(3) << "partId " << partId << ", vId " << vId
                    << ", tagId " << tc.tagId_ << ", prop size " << tc.props_.size();
            auto ret = collectVertexProps(partId, vId, tc.tagId_, tc.props_, writer);
            if (ret != kvstore::ResultCode::SUCCESSED) {
                return ret;
            }
        }
        vResp.vertex_data = writer.encode();
    }

    RowSetWriter rsWriter;
    auto ret = collectEdgeProps(partId, vId, edgeContext.edgeType_,
                                edgeContext.props_, rsWriter);
    if (ret != kvstore::ResultCode::SUCCESSED) {
        return ret;
    }
    vResp.edge_data = std::move(rsWriter.data());

    resp_.vertices.emplace_back(std::move(vResp));
    return kvstore::ResultCode::SUCCESSED;
}


void QueryBoundProcessor::onProcessed(std::vector<TagContext>& tagContexts,
                                      EdgeContext& edgeContext, int32_t retNum) {
    if (!tagContexts.empty()) {
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
    cpp2::Schema respEdge;
    respEdge.columns.reserve(edgeContext.props_.size());
    for (auto& prop : edgeContext.props_) {
        respEdge.columns.emplace_back(columnDef(std::move(prop.prop_.name),
                                                prop.type_.type));
    }
    resp_.edge_schema = std::move(respEdge);
}

}  // namespace storage
}  // namespace nebula

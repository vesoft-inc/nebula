/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/QueryVertexPropsProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace storage {

void QueryVertexPropsProcessor::process(const cpp2::VertexPropRequest& vertexReq) {
    spaceId_ = vertexReq.get_space_id();
    auto colSize = vertexReq.get_return_columns().size();
    if (colSize > 0) {
        cpp2::GetNeighborsRequest req;
        req.set_space_id(spaceId_);
        req.set_parts(std::move(vertexReq.get_parts()));
        decltype(req.return_columns) tmpColumns;
        tmpColumns.reserve(colSize);
        for (auto& col : vertexReq.get_return_columns()) {
            tmpColumns.emplace_back(std::move(col));
        }
        req.set_return_columns(std::move(tmpColumns));
        this->onlyVertexProps_ = true;
        QueryBoundProcessor::process(req);
    } else {
        std::vector<cpp2::VertexData> vertices;
        for (auto& part : vertexReq.get_parts()) {
            auto partId = part.first;
            for (auto& vId : part.second) {
                cpp2::VertexData vResp;
                vResp.set_vertex_id(vId);
                std::vector<cpp2::TagData> td;
                auto ret = collectVertexProps(partId, vId, td);
                if (ret != kvstore::ResultCode::ERR_KEY_NOT_FOUND
                        && ret != kvstore::ResultCode::SUCCEEDED) {
                    if (ret == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                        this->handleLeaderChanged(spaceId_, partId);
                    } else {
                        this->pushResultCode(this->to(ret), partId);
                    }
                    continue;
                }
                vResp.set_tag_data(std::move(td));
                vertices.emplace_back(std::move(vResp));
            }
        }
        resp_.set_vertices(std::move(vertices));
        onFinished();
    }
}

}  // namespace storage
}  // namespace nebula

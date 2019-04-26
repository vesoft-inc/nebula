/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/GetEdgeProcessor.h"

namespace nebula {
namespace meta {

void GetEdgeProcessor::process(const cpp2::GetEdgeReq& req) {
    GraphSpaceID spaceId;
    EdgeType edgeType;
    GET_SPACE_ID_AND_RETURN(req.get_space_name(), spaceId);
    GET_EDGE_TYPE_AND_RETURN(spaceId, req.get_edge_name(), edgeType);

    folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeLock());
    std::string edgeKey = MetaServiceUtils::schemaEdgeKey(spaceId,
                                                          edgeType,
                                                          req.get_version());
    auto ret = doGet(std::move(edgeKey));
    if (!ret.ok()) {
        LOG(ERROR) << "Get Edge Failed : SpaceName " << req.get_space_name() << ", EdgeName "
                   << req.get_edge_name() << ", version " << req.get_version();
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    LOG(INFO) << "Get Edge : SpaceName" << req.get_space_name() << ", EdgeName "
              << req.get_edge_name() << ", version " << req.get_version();
    resp_.set_schema(MetaServiceUtils::parseSchema(ret.value()));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

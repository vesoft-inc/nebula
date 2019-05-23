/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/GetEdgeProcessor.h"

namespace nebula {
namespace meta {

void GetEdgeProcessor::process(const cpp2::GetEdgeReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeLock());
    std::string edgeKey = MetaServiceUtils::schemaEdgeKey(req.get_space_id(),
                                                          req.get_edge_type(),
                                                          req.get_version());
    auto ret = doGet(std::move(edgeKey));
    if (!ret.ok()) {
        LOG(ERROR) << "Get Edge SpaceID: " << req.get_space_id() << ", edgeType: "
                   << req.get_edge_type() << ", version " << req.get_version() << " not found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    VLOG(3) << "Get Edge SpaceID: " << req.get_space_id() << ", edgeType: "
            << req.get_edge_type() << ", version " << req.get_version();
    resp_.set_schema(MetaServiceUtils::parseSchema(ret.value()));
    onFinished();
}
}  // namespace meta
}  // namespace nebula


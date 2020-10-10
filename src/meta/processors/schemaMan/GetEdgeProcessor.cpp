/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/GetEdgeProcessor.h"

namespace nebula {
namespace meta {

void GetEdgeProcessor::process(const cpp2::GetEdgeReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeLock());
    auto edgeTypeRet = getEdgeType(req.get_space_id(), req.get_edge_name());
    if (!edgeTypeRet.ok()) {
        handleErrorCode(MetaCommon::to(edgeTypeRet.status()));
        onFinished();
        return;
    }
    auto edgeType = edgeTypeRet.value();

    std::string schemaValue;
    // Get the lastest version
    if (req.get_version() < 0) {
        auto edgePrefix = MetaServiceUtils::schemaEdgePrefix(req.get_space_id(), edgeType);
        auto ret = doPrefix(edgePrefix);
        if (!ret.ok()) {
            LOG(ERROR) << "Get Edge SpaceID: " << req.get_space_id() << ", edgeName: "
                       << req.get_edge_name() << ", version " << req.get_version() << " not found";
            handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
        schemaValue = ret.value()->val().str();
    } else {
        auto edgeKey = MetaServiceUtils::schemaEdgeKey(req.get_space_id(),
                                                       edgeType,
                                                       req.get_version());
        auto ret = doGet(edgeKey);
        if (!ret.ok()) {
            LOG(ERROR) << "Get Edge SpaceID: " << req.get_space_id() << ", edgeName: "
                       << req.get_edge_name() << ", version " << req.get_version() << " not found";
            handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
        schemaValue = ret.value();
    }

    VLOG(3) << "Get Edge SpaceID: " << req.get_space_id() << ", edgeName: "
            << req.get_edge_name() << ", version " << req.get_version();
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_schema(MetaServiceUtils::parseSchema(schemaValue));
    onFinished();
}
}  // namespace meta
}  // namespace nebula


/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/CreateEdgeProcessor.h"
#include "meta/processors/schemaMan/SchemaUtil.h"

namespace nebula {
namespace meta {

void CreateEdgeProcessor::process(const cpp2::CreateEdgeReq& req) {
    GraphSpaceID spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);
    auto edgeName = req.get_edge_name();
    {
        // if there is an tag of the same name
        // TODO: there exists race condition, we should address it in the future
        folly::SharedMutex::ReadHolder rHolder(LockUtils::tagLock());
        auto conflictRet = getTagId(spaceId, edgeName);
        if (nebula::ok(conflictRet)) {
            LOG(ERROR) << "Failed to create edge `" << edgeName
                       << "': some tag with the same name already exists.";
            resp_.set_id(to(nebula::value(conflictRet), EntryType::EDGE));
            handleErrorCode(nebula::cpp2::ErrorCode::E_CONFLICT);
            onFinished();
            return;
        } else {
            auto retCode = nebula::error(conflictRet);
            if (retCode != nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND) {
                LOG(ERROR) << "Failed to create edge " << edgeName << " error "
                           << apache::thrift::util::enumNameSafe(retCode);
                handleErrorCode(retCode);
                onFinished();
                return;
            }
        }
    }

    auto columns = req.get_schema().get_columns();
    if (!SchemaUtil::checkType(columns)) {
        handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    cpp2::Schema schema;
    schema.set_columns(std::move(columns));
    schema.set_schema_prop(req.get_schema().get_schema_prop());

    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
    auto ret = getEdgeType(spaceId, edgeName);
    if (nebula::ok(ret)) {
        if (req.get_if_not_exists()) {
            handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
        } else {
            LOG(ERROR) << "Create Edge Failed :" << edgeName << " has existed";
            handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
        }
        resp_.set_id(to(nebula::value(ret), EntryType::EDGE));
        onFinished();
        return;
    } else {
        auto retCode = nebula::error(ret);
        if (retCode != nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND) {
            LOG(ERROR) << "Failed to create edge " << edgeName << " error "
                       << apache::thrift::util::enumNameSafe(retCode);
            handleErrorCode(retCode);
            onFinished();
            return;
        }
    }

    auto edgeTypeRet = autoIncrementId();
    if (!nebula::ok(edgeTypeRet)) {
        LOG(ERROR) << "Create edge failed : Get edge type id failed";
        handleErrorCode(nebula::error(edgeTypeRet));
        onFinished();
        return;
    }

    auto edgeType = nebula::value(edgeTypeRet);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexEdgeKey(spaceId, edgeName),
                      std::string(reinterpret_cast<const char*>(&edgeType), sizeof(EdgeType)));
    data.emplace_back(MetaServiceUtils::schemaEdgeKey(spaceId, edgeType, 0),
                      MetaServiceUtils::schemaVal(edgeName, schema));

    LOG(INFO) << "Create Edge " << edgeName << ", edgeType " << edgeType;
    resp_.set_id(to(edgeType, EntryType::EDGE));
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula

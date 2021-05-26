/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/DropEdgeProcessor.h"

namespace nebula {
namespace meta {

void DropEdgeProcessor::process(const cpp2::DropEdgeReq& req) {
    GraphSpaceID spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);

    folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
    auto edgeName = req.get_edge_name();

    EdgeType edgeType;
    auto indexKey = MetaServiceUtils::indexEdgeKey(spaceId, edgeName);
    auto iRet = doGet(indexKey);
    if (nebula::ok(iRet)) {
        edgeType = *reinterpret_cast<const EdgeType *>(nebula::value(iRet).c_str());
        resp_.set_id(to(edgeType, EntryType::EDGE));
    } else {
        auto retCode = nebula::error(iRet);
        if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
            if (req.get_if_exists()) {
                retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
            } else {
                LOG(ERROR) << "Drop edge failed :" << edgeName << " not found.";
                retCode = nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
            }
        } else {
             LOG(ERROR) << "Get edgetype failed, edge name " << edgeName
                        << " error: " << apache::thrift::util::enumNameSafe(retCode);
        }
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto indexes = getIndexes(spaceId, edgeType);
    if (!nebula::ok(indexes)) {
        handleErrorCode(nebula::error(indexes));
        onFinished();
        return;
    }
    if (!nebula::value(indexes).empty()) {
        LOG(ERROR) << "Drop edge error, index conflict, please delete index first.";
        handleErrorCode(nebula::cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    auto ftIdxRet = getFTIndex(spaceId, edgeType);
    if (nebula::ok(ftIdxRet)) {
        LOG(ERROR) << "Drop edge error, fulltext index conflict, "
                   << "please delete fulltext index first.";
        handleErrorCode(nebula::cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    if (nebula::error(ftIdxRet) != nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND) {
        handleErrorCode(nebula::error(ftIdxRet));
        onFinished();
        return;
    }

    auto ret = getEdgeKeys(spaceId, edgeType);
    if (!nebula::ok(ret)) {
        handleErrorCode(nebula::error(ret));
        onFinished();
        return;
    }

    auto keys = nebula::value(ret);
    keys.emplace_back(std::move(indexKey));
    LOG(INFO) << "Drop Edge " << edgeName;
    doSyncMultiRemoveAndUpdate(std::move(keys));
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>>
DropEdgeProcessor::getEdgeKeys(GraphSpaceID id, EdgeType edgeType) {
    std::vector<std::string> keys;
    auto key = MetaServiceUtils::schemaEdgePrefix(id, edgeType);
    auto iterRet = doPrefix(key);
    if (!nebula::ok(iterRet)) {
        LOG(ERROR) << "Edge schema prefix failed, edgetype " << edgeType;
        return nebula::error(iterRet);
    }

    auto iter = nebula::value(iterRet).get();
    while (iter->valid()) {
        keys.emplace_back(iter->key());
        iter->next();
    }
    return keys;
}

}  // namespace meta
}  // namespace nebula

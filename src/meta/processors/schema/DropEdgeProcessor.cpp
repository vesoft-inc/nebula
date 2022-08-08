/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/schema/DropEdgeProcessor.h"

#include "kvstore/LogEncoder.h"

namespace nebula {
namespace meta {

void DropEdgeProcessor::process(const cpp2::DropEdgeReq& req) {
  GraphSpaceID spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);

  folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  const auto& edgeName = req.get_edge_name();

  EdgeType edgeType;
  auto indexKey = MetaKeyUtils::indexEdgeKey(spaceId, edgeName);
  auto iRet = doGet(indexKey);
  if (nebula::ok(iRet)) {
    edgeType = *reinterpret_cast<const EdgeType*>(nebula::value(iRet).c_str());
    resp_.id_ref() = to(edgeType, EntryType::EDGE);
  } else {
    auto retCode = nebula::error(iRet);
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      if (req.get_if_exists()) {
        retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
      } else {
        LOG(INFO) << "Drop edge failed :" << edgeName << " not found.";
        retCode = nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
      }
    } else {
      LOG(INFO) << "Get edgetype failed, edge name " << edgeName
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
    LOG(INFO) << "Drop edge error, index conflict, please delete index first.";
    handleErrorCode(nebula::cpp2::ErrorCode::E_RELATED_INDEX_EXISTS);
    onFinished();
    return;
  }

  auto ftIdxRet = getFTIndex(spaceId, edgeType);
  if (nebula::ok(ftIdxRet)) {
    LOG(INFO) << "Drop edge error, fulltext index conflict, "
              << "please delete fulltext index first.";
    handleErrorCode(nebula::cpp2::ErrorCode::E_RELATED_INDEX_EXISTS);
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
  auto batchHolder = std::make_unique<kvstore::BatchHolder>();
  for (auto key : keys) {
    batchHolder->remove(std::move(key));
  }
  batchHolder->remove(std::move(indexKey));

  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(batchHolder.get(), timeInMilliSec);
  auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
  doBatchOperation(std::move(batch));
  LOG(INFO) << "Drop Edge " << edgeName;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> DropEdgeProcessor::getEdgeKeys(
    GraphSpaceID id, EdgeType edgeType) {
  std::vector<std::string> keys;
  auto key = MetaKeyUtils::schemaEdgePrefix(id, edgeType);
  auto iterRet = doPrefix(key);
  if (!nebula::ok(iterRet)) {
    LOG(INFO) << "Edge schema prefix failed, edgetype " << edgeType;
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

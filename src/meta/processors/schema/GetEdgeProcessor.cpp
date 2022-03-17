/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/schema/GetEdgeProcessor.h"

namespace nebula {
namespace meta {

void GetEdgeProcessor::process(const cpp2::GetEdgeReq& req) {
  GraphSpaceID spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);
  const auto& edgeName = req.get_edge_name();
  auto ver = req.get_version();

  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  auto edgeTypeRet = getEdgeType(spaceId, edgeName);
  if (!nebula::ok(edgeTypeRet)) {
    LOG(INFO) << "Get edge " << edgeName << " failed.";
    handleErrorCode(nebula::error(edgeTypeRet));
    onFinished();
    return;
  }
  auto edgeType = nebula::value(edgeTypeRet);

  std::string schemaValue;
  // Get the latest version
  if (ver < 0) {
    auto edgePrefix = MetaKeyUtils::schemaEdgePrefix(spaceId, edgeType);
    auto ret = doPrefix(edgePrefix);
    if (!nebula::ok(ret)) {
      LOG(INFO) << "Get Edge SpaceID: " << spaceId << ", edgeName: " << edgeName
                << ", latest version failed.";
      handleErrorCode(nebula::error(ret));
      onFinished();
      return;
    }
    auto iter = nebula::value(ret).get();
    if (!iter->valid()) {
      LOG(INFO) << "Get Edge SpaceID: " << spaceId << ", edgeName: " << edgeName
                << ", latest version "
                << " not found.";
      handleErrorCode(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND);
      onFinished();
      return;
    }
    folly::StringPiece iterVal;
    MetaKeyUtils::getLatestEdgeScheInfo(iter, iterVal);
    schemaValue = iterVal.str();
  } else {  // Get given version
    auto edgeKey = MetaKeyUtils::schemaEdgeKey(spaceId, edgeType, ver);
    auto ret = doGet(edgeKey);
    if (!nebula::ok(ret)) {
      LOG(INFO) << "Get Edge SpaceID: " << spaceId << ", edgeName: " << edgeName << ", version "
                << ver << " failed.";
      handleErrorCode(nebula::error(ret));
      onFinished();
      return;
    }
    schemaValue = nebula::value(ret);
  }

  VLOG(2) << "Get Edge SpaceID: " << spaceId << ", edgeName: " << edgeName << ", version " << ver;
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.schema_ref() = MetaKeyUtils::parseSchema(schemaValue);
  onFinished();
}

}  // namespace meta
}  // namespace nebula

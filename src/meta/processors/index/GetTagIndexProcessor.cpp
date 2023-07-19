/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/index/GetTagIndexProcessor.h"

namespace nebula {
namespace meta {

void GetTagIndexProcessor::process(const cpp2::GetTagIndexReq& req) {
  auto spaceID = req.get_space_id();
  const auto& indexName = req.get_index_name();
  CHECK_SPACE_ID_AND_RETURN(spaceID);
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());

  auto tagIndexIDRet = getIndexID(spaceID, indexName);
  if (!nebula::ok(tagIndexIDRet)) {
    auto retCode = nebula::error(tagIndexIDRet);
    LOG(INFO) << "Get Tag Index SpaceID: " << spaceID << " Index Name: " << indexName
              << " failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto indexId = nebula::value(tagIndexIDRet);
  LOG(INFO) << "Get Tag Index SpaceID: " << spaceID << " Index Name: " << indexName;
  const auto& indexKey = MetaKeyUtils::indexKey(spaceID, indexId);
  auto indexItemRet = doGet(indexKey);
  if (!nebula::ok(indexItemRet)) {
    auto retCode = nebula::error(indexItemRet);
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      retCode = nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
    LOG(INFO) << "Get Tag Index Failed: SpaceID " << spaceID << " Index Name: " << indexName
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto item = MetaKeyUtils::parseIndex(nebula::value(indexItemRet));
  if (item.get_schema_id().getType() != nebula::cpp2::SchemaID::Type::tag_id) {
    LOG(INFO) << "Get Tag Index Failed: Index Name " << indexName << " is not TagIndex";
    resp_.code_ref() = nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    onFinished();
    return;
  }

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.item_ref() = std::move(item);
  onFinished();
}

}  // namespace meta
}  // namespace nebula

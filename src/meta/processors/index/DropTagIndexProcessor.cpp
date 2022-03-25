/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/index/DropTagIndexProcessor.h"

#include "kvstore/LogEncoder.h"

namespace nebula {
namespace meta {

void DropTagIndexProcessor::process(const cpp2::DropTagIndexReq& req) {
  auto spaceID = req.get_space_id();
  const auto& indexName = req.get_index_name();
  CHECK_SPACE_ID_AND_RETURN(spaceID);
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());

  auto tagIndexIDRet = getIndexID(spaceID, indexName);
  if (!nebula::ok(tagIndexIDRet)) {
    auto retCode = nebula::error(tagIndexIDRet);
    if (retCode == nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND) {
      if (req.get_if_exists()) {
        retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
      } else {
        LOG(INFO) << "Drop Tag Index Failed, index name " << indexName
                  << " not exists in Space: " << spaceID;
      }
    } else {
      LOG(INFO) << "Drop Tag Index Failed, index name " << indexName
                << " error: " << apache::thrift::util::enumNameSafe(retCode);
    }
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto tagIndexID = nebula::value(tagIndexIDRet);

  auto batchHolder = std::make_unique<kvstore::BatchHolder>();
  batchHolder->remove(MetaKeyUtils::indexIndexKey(spaceID, indexName));

  auto indexKey = MetaKeyUtils::indexKey(spaceID, tagIndexID);
  auto indexItemRet = doGet(indexKey);
  if (!nebula::ok(indexItemRet)) {
    auto retCode = nebula::error(indexItemRet);
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      retCode = nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
    LOG(INFO) << "Drop Tag Index Failed: SpaceID " << spaceID << " Index Name: " << indexName
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto item = MetaKeyUtils::parseIndex(nebula::value(indexItemRet));
  if (item.get_schema_id().getType() != nebula::cpp2::SchemaID::Type::tag_id) {
    LOG(INFO) << "Drop Tag Index Failed: Index Name " << indexName << " is not TagIndex";
    resp_.code_ref() = nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    onFinished();
    return;
  }

  batchHolder->remove(std::move(indexKey));
  LOG(INFO) << "Drop Tag Index " << indexName;
  resp_.id_ref() = to(tagIndexID, EntryType::INDEX);

  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(batchHolder.get(), timeInMilliSec);
  auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
  doBatchOperation(std::move(batch));
}

}  // namespace meta
}  // namespace nebula

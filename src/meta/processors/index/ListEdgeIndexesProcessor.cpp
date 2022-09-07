/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/index/ListEdgeIndexesProcessor.h"

namespace nebula {
namespace meta {

void ListEdgeIndexesProcessor::process(const cpp2::ListEdgeIndexesReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);

  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  // The IndexItem obtained directly through indexId is inaccurate (it may be invalid data).
  // It is accurate to obtain indexId by traversing the index name, and then obtain IndexItem
  // through indexId
  auto indexIndexPrefix = MetaKeyUtils::indexIndexPrefix(space);
  auto indexIndexRet = doPrefix(indexIndexPrefix);
  if (!nebula::ok(indexIndexRet)) {
    auto retCode = nebula::error(indexIndexRet);
    LOG(INFO) << "List edge index failed, SpaceID: " << space
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto indexIndexIter = nebula::value(indexIndexRet).get();
  std::vector<cpp2::IndexItem> items;

  while (indexIndexIter->valid()) {
    auto indexId = *reinterpret_cast<const IndexID*>(indexIndexIter->val().toString().c_str());

    auto indexSchemaKey = MetaKeyUtils::indexKey(space, indexId);
    auto indexSchemaRet = doGet(indexSchemaKey);
    if (!nebula::ok(indexSchemaRet)) {
      auto retCode = nebula::error(indexSchemaRet);
      if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
        retCode = nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
      }
      LOG(INFO) << "Get edge index failed, SpaceID: " << space << ", indexId: " << indexId;
      handleErrorCode(retCode);
      onFinished();
      return;
    }

    auto val = nebula::value(indexSchemaRet);
    auto item = MetaKeyUtils::parseIndex(val);
    if (item.get_schema_id().getType() == nebula::cpp2::SchemaID::Type::edge_type) {
      items.emplace_back(std::move(item));
    }
    indexIndexIter->next();
  }

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.items_ref() = std::move(items);
  onFinished();
}

}  // namespace meta
}  // namespace nebula

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
  const auto& prefix = MetaKeyUtils::indexPrefix(space);
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "List Edge Index Failed: SpaceID " << space
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(iterRet).get();
  std::vector<cpp2::IndexItem> items;
  while (iter->valid()) {
    auto val = iter->val();
    auto item = MetaKeyUtils::parseIndex(val);
    if (item.get_schema_id().getType() == nebula::cpp2::SchemaID::Type::edge_type) {
      items.emplace_back(std::move(item));
    }
    iter->next();
  }
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.items_ref() = std::move(items);
  onFinished();
}

}  // namespace meta
}  // namespace nebula

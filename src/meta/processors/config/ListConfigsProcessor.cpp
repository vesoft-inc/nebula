/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/config/ListConfigsProcessor.h"

namespace nebula {
namespace meta {

void ListConfigsProcessor::process(const cpp2::ListConfigsReq& req) {
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());

  const auto& prefix = MetaKeyUtils::configKeyPrefix(req.get_module());
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "List configs failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(iterRet).get();
  std::vector<cpp2::ConfigItem> items;
  while (iter->valid()) {
    auto key = iter->key();
    auto value = iter->val();
    auto item = MetaKeyUtils::parseConfigValue(value);
    auto configName = MetaKeyUtils::parseConfigKey(key);
    item.module_ref() = configName.first;
    item.name_ref() = configName.second;
    items.emplace_back(std::move(item));
    iter->next();
  }
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.items_ref() = std::move(items);
  onFinished();
}

}  // namespace meta
}  // namespace nebula

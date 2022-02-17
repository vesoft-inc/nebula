/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/config/ListConfigsProcessor.h"

#include <folly/SharedMutex.h>              // for SharedMutex
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref

#include <algorithm>  // for max
#include <memory>     // for unique_ptr
#include <string>     // for basic_string, operator<<
#include <vector>     // for vector

#include "common/base/ErrorOr.h"              // for error, ok, value
#include "common/base/Logging.h"              // for LOG, LogMessage, _LOG...
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode:...
#include "kvstore/KVIterator.h"               // for KVIterator
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor::doPrefix
#include "meta/processors/Common.h"           // for LockUtils

namespace nebula {
namespace meta {

void ListConfigsProcessor::process(const cpp2::ListConfigsReq& req) {
  folly::SharedMutex::ReadHolder rHolder(LockUtils::configLock());

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

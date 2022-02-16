/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/config/RegConfigProcessor.h"

#include <folly/SharedMutex.h>              // for SharedMutex
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe

#include <algorithm>  // for max
#include <ostream>    // for operator<<, basic_ost...
#include <string>     // for operator<<, char_traits
#include <vector>     // for vector

#include "common/base/ErrorOr.h"              // for error, ok
#include "common/base/Logging.h"              // for LogMessage, COMPACT_G...
#include "common/datatypes/Value.h"           // for operator<<
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode:...
#include "kvstore/Common.h"                   // for KV
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor::doGet
#include "meta/processors/Common.h"           // for LockUtils

namespace nebula {
namespace meta {

void RegConfigProcessor::process(const cpp2::RegConfigReq& req) {
  std::vector<kvstore::KV> data;
  {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::configLock());
    for (const auto& item : req.get_items()) {
      auto module = item.get_module();
      auto name = item.get_name();
      auto mode = item.get_mode();
      auto value = item.get_value();
      VLOG(1) << "Config name: " << name << ", mode: " << apache::thrift::util::enumNameSafe(mode)
              << ", module: " << apache::thrift::util::enumNameSafe(module) << ", value: " << value;

      std::string configKey = MetaKeyUtils::configKey(module, name);
      // ignore config which has been registered before
      auto configRet = doGet(configKey);
      if (nebula::ok(configRet)) {
        continue;
      } else {
        auto retCode = nebula::error(configRet);
        if (retCode != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
          LOG(INFO) << "Get config Failed, error: " << apache::thrift::util::enumNameSafe(retCode);
          handleErrorCode(retCode);
          onFinished();
          return;
        }
      }

      std::string configValue = MetaKeyUtils::configValue(mode, value);
      data.emplace_back(std::move(configKey), std::move(configValue));
    }

    if (!data.empty()) {
      doSyncPutAndUpdate(std::move(data));
      return;
    }
  }
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula

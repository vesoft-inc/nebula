/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/configMan/RegConfigProcessor.h"

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
            VLOG(1) << "Config name: " << name
                    << ", mode: " << apache::thrift::util::enumNameSafe(mode)
                    << ", module: " << apache::thrift::util::enumNameSafe(module)
                    << ", value: " << value;

            std::string configKey = MetaServiceUtils::configKey(module, name);
            // ignore config which has been registered before
            auto configRet = doGet(configKey);
            if (nebula::ok(configRet)) {
                continue;
            } else {
                auto retCode = nebula::error(configRet);
                if (retCode != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
                    LOG(ERROR) << "Get config Failed, error: "
                               << apache::thrift::util::enumNameSafe(retCode);
                    handleErrorCode(retCode);
                    onFinished();
                    return;
                }
            }

            std::string configValue = MetaServiceUtils::configValue(mode, value);
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

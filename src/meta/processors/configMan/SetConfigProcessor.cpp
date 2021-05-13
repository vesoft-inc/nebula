/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/configMan/SetConfigProcessor.h"
#include "common/conf/Configuration.h"

namespace nebula {
namespace meta {

void SetConfigProcessor::process(const cpp2::SetConfigReq& req) {
    std::vector<kvstore::KV> data;
    auto module = req.get_item().get_module();
    auto name = req.get_item().get_name();
    auto value = req.get_item().get_value();

    folly::SharedMutex::WriteHolder wHolder(LockUtils::configLock());
    auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
    do {
        if (module != cpp2::ConfigModule::ALL) {
            // When we set config of a specified module, check if it exists.
            // If it exists and is mutable, update it.
            code = setConfig(module, name, value, data);
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                break;
            }
        } else {
            // When we set config of all module, then try to set it of every module.
            code = setConfig(cpp2::ConfigModule::GRAPH, name, value, data);
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                break;
            }
            code = setConfig(cpp2::ConfigModule::STORAGE, name, value, data);
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                break;
            }
        }

        if (!data.empty()) {
            doSyncPutAndUpdate(std::move(data));
            return;
        }
    } while (false);

    handleErrorCode(code);
    onFinished();
}

nebula::cpp2::ErrorCode
SetConfigProcessor::setConfig(const cpp2::ConfigModule& module,
                              const std::string& name,
                              const Value& value,
                              std::vector<kvstore::KV>& data) {
    std::string configKey = MetaServiceUtils::configKey(module, name);
    auto ret = doGet(std::move(configKey));
    if (!nebula::ok(ret)) {
        auto retCode = nebula::error((ret));
        if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
            retCode = nebula::cpp2::ErrorCode::E_CONFIG_NOT_FOUND;
        }
        LOG(ERROR) << "Set config " << name << " failed, error "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }

    cpp2::ConfigItem item = MetaServiceUtils::parseConfigValue(nebula::value(ret));
    cpp2::ConfigMode curMode = item.get_mode();
    if (curMode == cpp2::ConfigMode::IMMUTABLE) {
        return nebula::cpp2::ErrorCode::E_CONFIG_IMMUTABLE;
    }
    std::string configValue = MetaServiceUtils::configValue(curMode, value);
    data.emplace_back(std::move(configKey), std::move(configValue));
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula

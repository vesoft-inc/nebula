/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/configMan/SetConfigProcessor.h"

namespace nebula {
namespace meta {

void SetConfigProcessor::process(const cpp2::SetConfigReq& req) {
    std::vector<kvstore::KV> data;
    auto module = req.get_item().get_module();
    auto name = req.get_item().get_name();
    auto type = req.get_item().get_type();
    auto mode = req.get_item().get_mode();
    auto value = req.get_item().get_value();

    folly::SharedMutex::WriteHolder wHolder(LockUtils::configLock());
    if (module != cpp2::ConfigModule::ALL) {
        // When we set config of a specified module, check if it exists.
        // If it exists and is mutable, update it.
        if (!setOneConfig(module, name, type, mode, value, data)) {
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
    } else {
        // When we set config of all module, then try to set it of every module.
        if (!setOneConfig(cpp2::ConfigModule::GRAPH, name, type, mode, value, data) ||
            !setOneConfig(cpp2::ConfigModule::META, name, type, mode, value, data) ||
            !setOneConfig(cpp2::ConfigModule::STORAGE, name, type, mode, value, data)) {
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
    }

    if (!data.empty()) {
        doPut(std::move(data));
    }
}

bool SetConfigProcessor::setOneConfig(const cpp2::ConfigModule& module, const std::string& name,
                                      const cpp2::ConfigType& type, const cpp2::ConfigMode& mode,
                                      const std::string& value, std::vector<kvstore::KV>& data) {
    std::string configKey = MetaServiceUtils::configKey(module, name);
    auto ret = doGet(std::move(configKey));
    if (!ret.ok()) {
        return false;
    }

    cpp2::ConfigItem item = MetaServiceUtils::parseConfigValue(ret.value());
    if (item.get_mode() == cpp2::ConfigMode::IMMUTABLE) {
        return false;
    }
    std::string configValue = MetaServiceUtils::configValue(type, mode, value);
    data.emplace_back(std::move(configKey), std::move(configValue));
    return true;
}

}  // namespace meta
}  // namespace nebula

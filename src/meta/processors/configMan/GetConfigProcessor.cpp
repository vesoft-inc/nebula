/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/configMan/GetConfigProcessor.h"

namespace nebula {
namespace meta {

void GetConfigProcessor::process(const cpp2::GetConfigReq& req) {
    auto module = req.get_item().get_module();
    auto name = req.get_item().get_name();
    std::vector<cpp2::ConfigItem> items;

    {
        folly::SharedMutex::ReadHolder rHolder(LockUtils::configLock());
        if (module != cpp2::ConfigModule::ALL) {
            getOneConfig(module, name, items);
        } else {
            getOneConfig(cpp2::ConfigModule::GRAPH, name, items);
            getOneConfig(cpp2::ConfigModule::STORAGE, name, items);
        }
    }

    if (items.empty()) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
    } else {
        resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
        resp_.set_items(std::move(items));
    }
    onFinished();
}

void GetConfigProcessor::getOneConfig(const cpp2::ConfigModule& module,
                                      const std::string& name,
                                      std::vector<cpp2::ConfigItem>& items) {
    std::string configKey = MetaServiceUtils::configKey(module, name);
    auto ret = doGet(configKey);
    if (!ret.ok()) {
        return;
    }

    cpp2::ConfigItem item = MetaServiceUtils::parseConfigValue(ret.value());
    item.set_module(module);
    item.set_name(name);
    items.emplace_back(std::move(item));
}

}  // namespace meta
}  // namespace nebula

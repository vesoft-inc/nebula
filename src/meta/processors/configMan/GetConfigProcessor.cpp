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
    auto code = nebula::cpp2::ErrorCode::SUCCEEDED;

    do {
        folly::SharedMutex::ReadHolder rHolder(LockUtils::configLock());
        if (module != cpp2::ConfigModule::ALL) {
            code = getOneConfig(module, name, items);
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                break;
            }
        } else {
            code = getOneConfig(cpp2::ConfigModule::GRAPH, name, items);
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                 break;
            }
            code = getOneConfig(cpp2::ConfigModule::STORAGE, name, items);
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                break;
            }
        }
    } while (false);

    if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
        resp_.set_items(std::move(items));
    }
    handleErrorCode(code);
    onFinished();
}

nebula::cpp2::ErrorCode
GetConfigProcessor::getOneConfig(const cpp2::ConfigModule& module,
                                 const std::string& name,
                                 std::vector<cpp2::ConfigItem>& items) {
    std::string configKey = MetaServiceUtils::configKey(module, name);
    auto ret = doGet(configKey);
    if (!nebula::ok(ret)) {
        auto retCode = nebula::error(ret);
        if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
            retCode = nebula::cpp2::ErrorCode::E_CONFIG_NOT_FOUND;
        }
        LOG(ERROR) << "Get config " << name << " failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }

    cpp2::ConfigItem item = MetaServiceUtils::parseConfigValue(nebula::value(ret));
    item.set_module(module);
    item.set_name(name);
    items.emplace_back(std::move(item));
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula

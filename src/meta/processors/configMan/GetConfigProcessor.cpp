/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/configMan/GetConfigProcessor.h"

namespace nebula {
namespace meta {

void GetConfigProcessor::process(const cpp2::GetConfigReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::configLock());

    auto space = req.get_item().get_space();
    auto module = req.get_item().get_module();
    auto name = req.get_item().get_name();
    auto type = req.get_item().get_type();
    std::string configKey = MetaServiceUtils::configKey(space, module, name, type);

    auto ret = doGet(std::move(configKey));
    if (!ret.ok()) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    cpp2::ConfigItem item = MetaServiceUtils::parseConfigValue(ret.value());
    item.set_space(space);
    item.set_module(module);
    item.set_name(name);
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_item(std::move(item));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

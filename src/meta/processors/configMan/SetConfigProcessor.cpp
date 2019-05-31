/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/configMan/SetConfigProcessor.h"

namespace nebula {
namespace meta {

void SetConfigProcessor::process(const cpp2::SetConfigReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::configLock());
    std::vector<kvstore::KV> data;
    auto space = req.get_item().get_space();
    auto module = req.get_item().get_module();
    auto name = req.get_item().get_name();
    auto type = req.get_item().get_type();
    auto value = req.get_item().get_value();
    std::string configKey = MetaServiceUtils::configKey(space, module, name, type);
    std::string configValue = MetaServiceUtils::configValue(type, value);

    data.emplace_back(configKey, configValue);
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula

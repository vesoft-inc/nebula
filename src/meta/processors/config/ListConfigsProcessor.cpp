/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/configMan/ListConfigsProcessor.h"

namespace nebula {
namespace meta {

void ListConfigsProcessor::process(const cpp2::ListConfigsReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::configLock());

    const auto& prefix = MetaServiceUtils::configKeyPrefix(req.get_module());
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "List configs failed, error: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto iter = nebula::value(iterRet).get();

    std::vector<cpp2::ConfigItem> items;
    while (iter->valid()) {
        auto key = iter->key();
        auto value = iter->val();
        auto item = MetaServiceUtils::parseConfigValue(value);
        auto configName = MetaServiceUtils::parseConfigKey(key);
        item.set_module(configName.first);
        item.set_name(configName.second);
        items.emplace_back(std::move(item));
        iter->next();
    }
    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    resp_.set_items(std::move(items));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

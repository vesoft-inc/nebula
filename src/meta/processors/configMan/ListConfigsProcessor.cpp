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

    auto prefix = MetaServiceUtils::configKeyPrefix(req.get_module());
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(MetaCommon::to(ret));
        onFinished();
        return;
    }

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
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_items(std::move(items));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

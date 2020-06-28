/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/GetPluginProcessor.h"

namespace nebula {
namespace meta {

void GetPluginProcessor::process(const cpp2::GetPluginReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::pluginLock());

    auto& pluginName = req.get_plugin_name();
    auto pluginRet = getPluginId(pluginName);
    if (!pluginRet.ok()) {
        LOG(ERROR) << "Plugin " << pluginName << " not found.";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto pluginKey = MetaServiceUtils::pluginKey(pluginRet.value());
    std::string val;
    auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, pluginKey, &val);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    decltype(resp_.item) pluginItem = MetaServiceUtils::parsePluginItem(val);
    resp_.set_item(std::move(pluginItem));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}
}  // namespace meta
}  // namespace nebula

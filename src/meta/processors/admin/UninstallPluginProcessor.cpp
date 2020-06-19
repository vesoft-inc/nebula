/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/UninstallPluginProcessor.h"

namespace nebula {
namespace meta {

void UninstallPluginProcessor::process(const cpp2::UninstallPluginReq& req) {
    auto& pluginName = req.get_plugin_name();
    folly::SharedMutex::WriteHolder wHolder(LockUtils::pluginLock());

    // Check if the plugin exists
    auto ret = getPluginId(pluginName);
    if (!ret.ok()) {
        LOG(ERROR) << "Uninstall plugin failed : plugin " << pluginName;
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    std::vector<std::string> keys;
    auto pluginId = ret.value();
    keys.emplace_back(MetaServiceUtils::indexPluginKey(pluginName));
    keys.emplace_back(MetaServiceUtils::pluginKey(pluginId));

    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    LOG(INFO) << "Uninstall plugin succeed : " << pluginName;
    resp_.set_id(to(pluginId, EntryType::PLUGIN));
    doSyncMultiRemoveAndUpdate(std::move(keys));
}

}  // namespace meta
}  // namespace nebula


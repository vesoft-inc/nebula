/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/InstallPluginProcessor.h"
#include "common/fs/FileUtils.h"

namespace nebula {
namespace meta {

// When installing, just register it, do not open it. open it when using
void InstallPluginProcessor::process(const cpp2::InstallPluginReq& req) {
    auto& pluginName = req.get_plugin_name();
    auto& soName = req.get_so_name();
    folly::SharedMutex::WriteHolder wHolder(LockUtils::pluginLock());

    // Check if the plugin exists
    auto ret = getPluginId(pluginName);
    if (ret.ok()) {
        LOG(ERROR) << "Install plugin failed : plugin " << pluginName;
        cpp2::ErrorCode code = cpp2::ErrorCode::E_EXISTED;
        handleErrorCode(code);
        onFinished();
        return;
    }

    auto idRet = autoIncrementId();
    if (!nebula::ok(idRet)) {
        LOG(ERROR) << "Install plugin failed : get plugin id failed";
        handleErrorCode(nebula::error(idRet));
        onFinished();
        return;
    }

    auto pluginId = nebula::value(idRet);
    std::vector<kvstore::KV> data;

    cpp2::PluginItem pluginItem;
    pluginItem.set_plugin_id(pluginId);
    pluginItem.set_plugin_name(pluginName);
    pluginItem.set_so_name(std::move(soName));
    data.emplace_back(MetaServiceUtils::indexPluginKey(pluginName),
                      std::string(reinterpret_cast<const char*>(&pluginId), sizeof(pluginId)));
    data.emplace_back(MetaServiceUtils::pluginKey(pluginId),
                      MetaServiceUtils::pluginVal(pluginItem));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(pluginId, EntryType::PLUGIN));
    LOG(INFO) << "Install plugin succeed : " << pluginName;
    doPut(std::move(data));
}
}  // namespace meta
}  // namespace nebula


/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "meta/processors/admin/InstallPluginProcessor.h"
#include "common/fs/FileUtils.h"

namespace nebula {
namespace meta {

// When installing, just register it, do not always open it, open it when using
void InstallPluginProcessor::process(const cpp2::InstallPluginReq& req) {
    const auto& pluginItem = req.get_item();
    auto& pluginName = pluginItem.get_plugin_name();
    auto& soName = pluginItem.get_so_name();
    folly::SharedMutex::WriteHolder wHolder(LockUtils::pluginLock());

    // Check if file exists
    using fs::FileUtils;
    auto dir = FileUtils::readLink("/proc/self/exe").value();
    dir = FileUtils::dirname(dir.c_str()) + "/../share";
    std::string sofile = dir + "/" + soName;

    cpp2::ErrorCode code;
    if (!FileUtils::exist(sofile)) {
        LOG(ERROR) << "Install plugin Failed : sofile " << sofile << " not exists";
        code = cpp2::ErrorCode::E_NOT_FOUND;
        handleErrorCode(code);
        onFinished();
        return;
    }

    // Check if the plugin exists
    auto ret = getPluginId(pluginName);
    if (ret.ok()) {
        LOG(ERROR) << "Install plugin Failed : plugin " << pluginName;
        code = cpp2::ErrorCode::E_EXISTED;
        handleErrorCode(code);
        onFinished();
        return;
    }

    auto idRet = autoIncrementId();
    if (!nebula::ok(idRet)) {
        LOG(ERROR) << "Install plugin Failed : Get plugin id failed";
        handleErrorCode(nebula::error(idRet));
        onFinished();
        return;
    }

    auto pluginId = nebula::value(idRet);
    std::vector<kvstore::KV> data;

    data.emplace_back(MetaServiceUtils::indexPluginKey(pluginName),
                      std::string(reinterpret_cast<const char*>(&pluginId), sizeof(pluginId)));
    data.emplace_back(MetaServiceUtils::pluginKey(pluginId),
                      MetaServiceUtils::pluginVal(pluginItem));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(pluginId, EntryType::PLUGIN));
    doPut(std::move(data));
}
}  // namespace meta
}  // namespace nebula


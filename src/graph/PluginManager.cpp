/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/PluginManager.h"
#include "graph/GraphFlags.h"
#include "fs/FileUtils.h"
#include <dlfcn.h>

namespace nebula {
namespace graph {

PluginManager::~PluginManager() {
    if (metaClient_ != nullptr) {
        metaClient_ = nullptr;
    }
    close();
}


Status PluginManager::init() {
    folly::RWSpinLock::WriteHolder holder(rwlock_);

    StatusOr<std::vector<nebula::meta::cpp2::PluginItem>>
        retPlugins = metaClient_->listPluginsFromCache();
    if (!retPlugins.ok()) {
        return Status::Error("List plugin failed.");
    }
    auto plugins = retPlugins.value();
    for (auto &plugin : plugins) {
        auto pluginName = plugin.get_plugin_name();
        auto soname = plugin.get_so_name();
        auto status = openPluginNoLock(pluginName, soname);
        if (!status.ok()) {
            LOG(ERROR) << status.toString();
            // Ignore, continue
            continue;
        }
    }
    return Status::OK();
}


Status PluginManager::open(const std::string& pluginName) {
    folly::RWSpinLock::WriteHolder holder(rwlock_);

    auto iter = pluginToInfo_.find(pluginName);
    if (iter != pluginToInfo_.end()) {
        LOG(INFO) << "Already open plugin.";
        return Status::OK();
    }

    // Open plugin
    StatusOr<nebula::meta::cpp2::PluginItem>
        retPlugin = metaClient_->getPluginFromCache(pluginName);
    if (!retPlugin.ok()) {
        return Status::Error("Get plugin %s failed.", pluginName.c_str());
    }

    auto plugin = retPlugin.value();
    auto soname = plugin.get_so_name();

    return openPluginNoLock(pluginName, soname);
}


Status PluginManager::openPluginNoLock(const std::string& pluginName,
                                       const std::string& soname) {
    using fs::FileUtils;
    auto dir = FileUtils::readLink("/proc/self/exe").value();
    dir = FileUtils::dirname(dir.c_str()) + "/../share";
    std::string sofile = dir + "/" + soname;

    if (!FileUtils::exist(sofile)) {
        return Status::Error("so file: %s not exist", sofile.c_str());
    }

    void* dlHandle = dlopen(sofile.c_str(), RTLD_NOW);
    if (!dlHandle) {
        return Status::Error("Open plugin %s failed: `%s", pluginName.c_str(), dlerror());
    }

    if (!pluginName.compare("auth_ldap")) {
        authLdapSimple_ = static_cast<auth_ldap_simple>(dlsym(dlHandle, "authLdapSimple"));
        if (!authLdapSimple_) {
            LOG(ERROR) << "Function authLdapSimple not found in so file";
            dlclose(dlHandle);
            return Status::Error("Function authLdap_Simple not found in so file");
        }
        authLdapSearchBind_ =
            static_cast<auth_ldap_search_bind>(dlsym(dlHandle, "authLdapSearchBind"));
        if (!authLdapSearchBind_) {
            LOG(ERROR) << "Function authLdapSearchBind not found in so file";
            dlclose(dlHandle);
            return Status::Error("Function authLdapSearchBind not found in so file");
        }

        auto pluginInfo = std::make_shared<PluginInfo>();
        pluginInfo->soName_ = soname;
        pluginInfo->handler_ = dlHandle;
        pluginInfo->funcName_.emplace_back("authLdapSimple");
        pluginInfo->funcName_.emplace_back("authLdapSearchBind");
        pluginToInfo_.emplace("auth_ldap", pluginInfo);
    } else {
        // TODO support udf function
        LOG(ERROR) << "Unsupport it yet";
        return Status::Error("Unsupport it yet");
    }
    return Status::OK();
}


bool PluginManager::findFunc(const std::string& funcName) {
    folly::RWSpinLock::ReadHolder holder(rwlock_);
    for (auto& pluginToInfo : pluginToInfo_) {
        for (auto&func : pluginToInfo.second->funcName_) {
            if (!func.compare(funcName)) {
                return true;
            }
        }
    }
    return false;
}


bool PluginManager::execAuthLdapSimple(std::string ldap_server,
                                       int32_t     ldap_port,
                                       std::string ldap_scheme,
                                       bool        ldap_tls,
                                       std::string ldap_prefix,
                                       std::string ldap_suffix,
                                       std::string userName,
                                       std::string passwd) {
    return authLdapSimple_(ldap_server,
                           ldap_port,
                           ldap_scheme,
                           ldap_tls,
                           ldap_prefix,
                           ldap_suffix,
                           userName,
                           passwd);
}


bool PluginManager::execAuthLdapSearchBind(std::string ldap_server,
                                           int32_t     ldap_port,
                                           std::string ldap_scheme,
                                           bool        ldap_tls,
                                           std::string ldap_basedn,
                                           std::string ldap_binddn,
                                           std::string ldap_bindpasswd,
                                           std::string ldap_searchattribute,
                                           std::string ldap_searchfilter,
                                           std::string userName,
                                           std::string passwd) {
    return authLdapSearchBind_(ldap_server,
                               ldap_port,
                               ldap_scheme,
                               ldap_tls,
                               ldap_basedn,
                               ldap_binddn,
                               ldap_bindpasswd,
                               ldap_searchattribute,
                               ldap_searchfilter,
                               userName,
                               passwd);
}


void PluginManager::close() {
    folly::RWSpinLock::WriteHolder holder(rwlock_);
    for (auto& pluginToInfo : pluginToInfo_) {
        if (pluginToInfo.second->handler_) {
            dlclose(pluginToInfo.second->handler_);
        }
    }
    authLdapSimple_ = nullptr;
    authLdapSearchBind_ = nullptr;
    pluginToInfo_.clear();
}

}   // namespace graph
}   // namespace nebula

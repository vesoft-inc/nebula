/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_PLUGINMANAGER_H_
#define GRAPH_PLUGINMANAGER_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include "thread/GenericWorker.h"
#include "meta/client/MetaClient.h"
/**
 * PluginManager manages the plugin message, e.g. plugin name, sofile,
 * dlopen handler, dlsym function, dlclose handler
 */

namespace nebula {
namespace graph {

class PluginManager final {
public:
    explicit PluginManager(meta::MetaClient* client) {
        metaClient_ = client;
    }

    ~PluginManager();

    /**
     * Initialize all plugins
     */
    Status init();

    /**
     * If the plugin has not been opened, dlopen opens the plugin, dlsym init function
     * and records the information. If the plugin has been opened, return directly.
     */
    Status open(const std::string& pluginName);

    /**
     * Dlopen opens the plugin, dlsym init function, records the information
     */
    Status openPluginNoLock(const std::string& pluginName, const std::string& soname);

    /**
     * Check if function exists
     */
    bool findFunc(const std::string& funcName);

    /**
     * Execute function, return result
     */
    bool execAuthLdapSimple(std::string ldap_server,
                            int32_t     ldap_port,
                            std::string ldap_scheme,
                            bool        ldap_tls,
                            std::string ldap_prefix,
                            std::string ldap_suffix,
                            std::string userName,
                            std::string passwd);

    bool execAuthLdapSearchBind(std::string ldap_server,
                                int32_t     ldap_port,
                                std::string ldap_scheme,
                                bool        ldap_tls,
                                std::string ldap_basedn,
                                std::string ldap_binddn,
                                std::string ldap_bindpasswd,
                                std::string ldap_searchattribute,
                                std::string ldap_searchfilter,
                                std::string userName,
                                std::string passwd);
    /**
     * For all open plugins, close
     */
    void close();

    struct PluginInfo {
        std::string soName_;
        void* handler_;
        // funcName
        std::vector<std::string> funcName_;
    };

    // Function pointers currently supported
    typedef bool (*auth_ldap_simple)(std::string ldap_server,
                                     int32_t     ldap_port,
                                     std::string ldap_scheme,
                                     bool        ldap_tls,
                                     std::string ldap_prefix,
                                     std::string ldap_suffix,
                                     std::string userName,
                                     std::string passwd);

    typedef bool (*auth_ldap_search_bind)(std::string ldap_server,
                                          int32_t     ldap_port,
                                          std::string ldap_scheme,
                                          bool        ldap_tls,
                                          std::string ldap_basedn,
                                          std::string ldap_binddn,
                                          std::string ldap_bindpasswd,
                                          std::string ldap_searchattribute,
                                          std::string ldap_searchfilter,
                                          std::string userName,
                                          std::string passwd);

private:
    folly::RWSpinLock                                              rwlock_;
    meta::MetaClient                                              *metaClient_;
    // pluginName -> std::pair<soNmae, dlopen handler>
    std::unordered_map<std::string, std::shared_ptr<PluginInfo>>   pluginToInfo_;
    auth_ldap_simple                                               authLdapSimple_{nullptr};
    auth_ldap_search_bind                                          authLdapSearchBind_{nullptr};
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_SESSIONMANAGER_H_

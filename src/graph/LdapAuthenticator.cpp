/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/LdapAuthenticator.h"
#include "graph/GraphFlags.h"

namespace nebula {
namespace graph {

LdapAuthenticator::LdapAuthenticator(meta::MetaClient* client,
                                     PluginManager* pluginManager) {
    metaClient_ = client;
    pluginManager_ = pluginManager;
}

Status LdapAuthenticator::prepare() {
    if (FLAGS_ldap_server.empty()) {
        return Status::Error("LDAP authentication \"ldap_server\" parameter is not set.");
    }

    if (FLAGS_ldap_scheme.compare("ldap")) {
        return Status::Error("LDAP authentication ldap_scheme only support \"ldap\".");
    }

    if (FLAGS_ldap_port == 0) {
        FLAGS_ldap_port = ldapPort_;
    }

    // Simple bind mode
    if (!FLAGS_ldap_prefix.empty() || !FLAGS_ldap_suffix.empty()) {
        if (!FLAGS_ldap_basedn.empty() ||
            !FLAGS_ldap_binddn.empty() ||
            !FLAGS_ldap_bindpasswd.empty() ||
            !FLAGS_ldap_searchattribute.empty() ||
            !FLAGS_ldap_searchfilter.empty()) {
            return Status::Error("LDAP authentication simple bind mode and search bind mode"
                                 " parameters cannot be mixed.");
        }
    // Here search bind mode must be set FLAGS_ldap_basedn
    } else if (FLAGS_ldap_basedn.empty()) {
        return Status::Error("LDAP authentication requires argument \"ldap_prefix\", "
                             "\"ldap_suffix\", or \"ldap_basedn\" to be set.");
    }

    /**
     * Search bind mode can either use FLAGS_ldap_searchattribute or
     * FLAGS_ldap_searchfilter, but can't use both. FLAGS_Ldap_searchattribute
     * default is "uid". FLAGS_Ldap_searchfilter is more flexible search filtes
     * than FLAGS_Ldap_searchattribute.
     */
    if (!FLAGS_ldap_searchattribute.empty() && !FLAGS_ldap_searchfilter.empty()) {
        return Status::Error("LDAP authentication cannot use ldap_searchattribute "
                             "together with ldap_searchfilter.");
    }

    return Status::OK();
}

bool LdapAuthenticator::auth(const std::string &user,
                             const std::string &password) {
    // The shadow account on the nebula side has been created
    // First, go to meta to check if the shadow account exists
    if (!metaClient_->checkShadowAccountFromCache(user)) {
        return false;
    }

    // Second, use user + password authentication methods
    if (password.empty()) {
        LOG(ERROR) << "Password cannot be empty in LDAP authentication.";
        return false;
    }

    userName_ = user;
    password_ = password;

    auto ret = prepare();
    if (!ret.ok()) {
        LOG(ERROR) << ret.toString();
        return false;
    }

    // find so function point
    ret = pluginManager_->open("auth_ldap");
    if (!ret.ok()) {
        LOG(ERROR) << ret.toString();
        return false;
    }

    // Search bind mode
    bool retAuth = false;
    if (!FLAGS_ldap_basedn.empty()) {
        retAuth = pluginManager_->execSearchBindLdapAuth(FLAGS_ldap_server,
                                                         FLAGS_ldap_port,
                                                         FLAGS_ldap_scheme,
                                                         FLAGS_ldap_tls,
                                                         FLAGS_ldap_basedn,
                                                         FLAGS_ldap_binddn,
                                                         FLAGS_ldap_bindpasswd,
                                                         FLAGS_ldap_searchattribute,
                                                         FLAGS_ldap_searchfilter,
                                                         user,
                                                         password);
    } else {
        retAuth = pluginManager_->execSimpleLdapAuth(FLAGS_ldap_server,
                                                     FLAGS_ldap_port,
                                                     FLAGS_ldap_scheme,
                                                     FLAGS_ldap_tls,
                                                     FLAGS_ldap_prefix,
                                                     FLAGS_ldap_suffix,
                                                     user,
                                                     password);
    }

    if (!retAuth) {
        LOG(ERROR) << "LDAP authentication failed.";
        return false;
    }

    return true;
}

}   // namespace graph
}   // namespace nebula

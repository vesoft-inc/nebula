/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/LdapAuthenticator.h"
#include "graph/GraphFlags.h"

namespace nebula {
namespace graph {

LdapAuthenticator::LdapAuthenticator(meta::MetaClient* client) {
    metaClient_ = client;
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
            return Status::Error("LDAP authentication direct bind mode and search bind mode"
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

    // Search bind mode
    bool retAuth = false;
    if (!FLAGS_ldap_basedn.empty()) {
        // ret = searchBindAuth();
    } else {
        // ret = simpleBindAuth();
    }

    if (!retAuth) {
        LOG(ERROR) << "LDAP authentication failed.";
        return false;
    }

    // Check if there is a shadow account, if not, create a shadow account
    auto retUser = metaClient_->createUser(userName_, "", true).get();
    if (!retUser.ok()) {
        LOG(ERROR) << "Create shadow account failed ," << retUser.status();
        return false;
    }

    return retUser.value();
}

}   // namespace graph
}   // namespace nebula

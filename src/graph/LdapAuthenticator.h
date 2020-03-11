/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_LDAPAUTHENTICATOR_H_
#define GRAPH_LDAPAUTHENTICATOR_H_

#include "base/Base.h"
#include "graph/Authenticator.h"
#include "base/StatusOr.h"
#include "base/Status.h"
#include "meta/client/MetaClient.h"
#include <ldap.h>

namespace nebula {
namespace graph {

/**
 * LDAP authentication has two modes: simple bind mode and search bind mode.
 * common parameters:
 * FLAGS_ldap_server, FLAGS_ldap_port
 * FLAGS_ldap_scheme, FLAGS_ldap_tls
 *
 * Simple bind mode uses the parameters:
 * FLAGS_ldap_prefix, FLAGS_ldap_suffix
 * dn(distinguished name) = FLAGS_ldap_prefix + userName + FLAGS_ldap_suffix
 *
 * Search bind mode uses the parameters:
 * FLAGS_ldap_basedn, FLAGS_ldap_binddn, FLAGS_ldap_bindpasswd,
 * one of FLAGS_ldap_searchattribute or FLAGS_ldap_searchfilter
 *
 * Disallow mixing the parameters of two modes.
 */
class LdapAuthenticator final : public Authenticator {
public:
    explicit LdapAuthenticator(meta::MetaClient* client);

    /**
     * Execute LDAP authentication.
     */
    bool auth(const std::string &user, const std::string &password) override;

private:
    meta::MetaClient*               metaClient_{nullptr};
    /**
     * Check if LDAP authentication parameters are set legally.
     */
    Status prepare();
    int                      ldapPort_{389};

    std::string              userName_;
    std::string              password_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_LDAPAUTHENTICATOR_H_

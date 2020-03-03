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
#include <ldap.h>

namespace nebula {
namespace graph {

class LdapAuthenticator final : public Authenticator {
public:
    /**
     * Execute LDAP authentication.
     */
    bool MUST_USE_RESULT auth(const std::string &user,
                              const std::string &password) override;

private:
    /**
     * Check if LDAP authentication parameters are set legally.
     */
    Status prepare();

    /**
     * Init LDAP connect to LADP server.
     * FLAGS_ldap_server may be a comma-separated list of IP addresses.
     */
    Status initLDAPConnection();

    /**
     * Build the filter
     * Either search filter, or attribute filter, or default value.
     */
    std::string buildFilter();

    /**
     * Build the custom search filter.
     * Replace all occurrences of the placeholder "$username" in FLAGS_ldap_searchfilter
     * with variable userName_.
     */
    std::string buildSearchFilter();

    /**
     * Execute LDAP search bind mode authentication
     */
    StatusOr<bool> searchBindAuth();

    /**
     * Execute LDAP simple bind mode authentication
     */
    StatusOr<bool> simpleBindAuth();


    LDAP                    *ldap_{nullptr};

    // Default LDAP port
    int                      ldapPort_{389};

    std::string              userName_;
    std::string              password_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_LDAPAUTHENTICATOR_H_

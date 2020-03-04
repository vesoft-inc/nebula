/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/LdapAuthenticator.h"
#include "graph/GraphFlags.h"
#define LDAP_DEPRECATED 1

namespace nebula {
namespace graph {

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


Status LdapAuthenticator::initLDAPConnection() {
    int ldap_version = LDAP_VERSION3;

    // Ldap_create interface is only available in versions higher than 2.4.49
    /*
    int ret  = ldap_create(&ldap_);
    if (ret != LDAP_SUCCESS) {
        return Status::Error("Init LDAP failed.");
    }
    ldap_->ld_options.ldo_defport = FLAGS_ldap_port;

    ret = ldap_set_option(ldap_, LDAP_OPT_HOST_NAME, FLAGS_ldap_server);
    if (ret != LDAP_SUCCESS) {
        ldap_ld_free(ldsp_, 1, NULL, NULL);
        return Status::Error("Init LDAP failed.");
    }
    */
    // ldap_init is deprecated, so use ldap_initialize

    // FLAGS_ldap_server is comma separated list of hosts
    auto ldap_server = folly::trimWhitespace(FLAGS_ldap_server);
    if (ldap_server.empty()) {
        return Status::Error("LDAP authentication ldap_server is illegal.");
    }
    const char *host = ldap_server.str().c_str();
    std::string uris("");
    do {
        auto size = std::strcspn(host, ",");
        if (uris.length() > 0) {
            uris.append(" ");
        }
        uris.append(FLAGS_ldap_scheme);
        uris.append("://");
        uris.append(host, size);
        uris += folly::stringPrintf(":%d", FLAGS_ldap_port);
        host += size;
        if (*host == ',') {
            host++;
        }
        while (*host == ' ') {
           host++;
        }
    } while (*host);

    // initialize the LDAP ldap://host[:port] host[:port]
    auto ret = ldap_initialize(&ldap_, uris.c_str());
    if (ret != LDAP_SUCCESS) {
        return Status::Error("Init LDAP failed.");
    }

    ret = ldap_set_option(ldap_, LDAP_OPT_PROTOCOL_VERSION, &ldap_version);
    if (ret != LDAP_SUCCESS) {
        ldap_unbind_ext(ldap_, NULL, NULL);
        return Status::Error("Set LDAP protocol version failed");
    }

    if (FLAGS_ldap_tls) {
        ret = ldap_start_tls_s(ldap_, NULL, NULL);
        if (ret != LDAP_SUCCESS) {
            ldap_unbind_ext(ldap_, NULL, NULL);
            return Status::Error("Start LDAP TLS session failed");
        }
    }
    return Status::OK();
}


std::string LdapAuthenticator::buildFilter() {
    std::string filter;
    if (!FLAGS_ldap_searchfilter.empty()) {
        filter = buildSearchFilter();
    } else if (!FLAGS_ldap_searchattribute.empty()) {
        filter = folly::stringPrintf("(%s=%s)", FLAGS_ldap_searchattribute.c_str(),
                                      userName_.c_str());
    } else {
        filter = folly::stringPrintf("(uid=%s)", userName_.c_str());
    }
    return filter;
}


std::string LdapAuthenticator::buildSearchFilter() {
    std::string result  = FLAGS_ldap_searchfilter;
    std::string ph = "$username";
    auto len = ph.length();

    std::string::size_type  pos = 0;
    while ((pos = result.find(ph)) != std::string::npos) {
        result.replace(pos, len, userName_);
    }
    return result;
}


StatusOr<bool> LdapAuthenticator::searchBindAuth() {
    LDAPMessage *searchMessage, *entry;
    char* distName;
    std::string dn;

    /**
     * Firstly, perform the LDAP seach to find the distinguished name
     * of the user who is logging in.
     */

    /**
     * LDAP authentication disallows any characters that need to escape
     * in user name.
     */
    for (auto& c : userName_) {
        if (c == '*' ||
            c == '(' ||
            c == ')' ||
            c == '/' ||
            c == '\\') {
            ldap_unbind_ext(ldap_, NULL, NULL);
            return Status::Error("User name contains invalid character in LDAP authentication.");
        }
    }

    /**
     * Bind with a pre-defined user name and password to search.
     * If none is specified, use the anonymous bind.
     */
    auto& binddn = FLAGS_ldap_binddn;
    auto& bindPassword = FLAGS_ldap_bindpasswd;

    struct berval cred1;
    cred1.bv_val = const_cast<char*>(bindPassword.c_str());
    cred1.bv_len = bindPassword.length();

    auto rc = ldap_sasl_bind_s(ldap_,
                              binddn.c_str(),
                              LDAP_SASL_SIMPLE,
                              &cred1,
                              NULL,
                              NULL,
                              NULL);

    if (rc != LDAP_SUCCESS) {
        ldap_unbind_ext(ldap_, NULL, NULL);
        return Status::Error("Perform initial LDAP bind for ldapbinddn \"%s\" on server "
                             "\"%s\" failed.", binddn.c_str(), FLAGS_ldap_server.c_str());
    }

    // Build the filter
    auto filter = buildFilter();


    // "1.1" ldap_no_attrs
    char ldap_no_attrs[] = "1.1";

    char *attributes[2];;
    attributes[1] = ldap_no_attrs;
    attributes[2] = nullptr;

    // Initiate an ldap search, synchronize
    rc = ldap_search_ext_s(ldap_,
                           FLAGS_ldap_basedn.c_str(),
                           0,  // The search scope, use LDAP_SCOPE_BASE
                           filter.c_str(),
                           attributes,
                           0,
                           NULL,
                           NULL,
                           NULL,
                           LDAP_NO_LIMIT,
                           &searchMessage);

    if (rc != LDAP_SUCCESS) {
        ldap_unbind_ext(ldap_, NULL, NULL);
        return Status::Error("Search LDAP for filter \"%s\" on server \"%s\" failed.",
                             filter.c_str(), FLAGS_ldap_server.c_str());
    }

    // Check the number of search result
    auto retCount = ldap_count_entries(ldap_, searchMessage);
    if (retCount != 1) {
        ldap_unbind_ext(ldap_, NULL, NULL);
        ldap_msgfree(searchMessage);
        if (retCount == 0) {
            return Status::Error("LDAP user \"%s\" does not exist.", userName_.c_str());
        } else {
            return Status::Error("LDAP user \"%s\" is not unique.", userName_.c_str());
        }
    }

    // Get distinguished name from search result
    entry = ldap_first_entry(ldap_, searchMessage);
    distName = ldap_get_dn(ldap_, entry);

    if (!distName) {
        ldap_unbind_ext(ldap_, NULL, NULL);
        ldap_msgfree(searchMessage);
        return Status::Error("Get distinguished name for the first entry with filter \"%s\" "
                             "on server \"%s\" failed.", filter.c_str(),
                             FLAGS_ldap_server.c_str());
    }

    dn = distName;
    ldap_memfree(distName);
    ldap_msgfree(searchMessage);

    // Unbind and disconnect the first connection from the LDAP server
    rc = ldap_unbind_ext_s(ldap_, NULL, NULL);
    if (rc != LDAP_SUCCESS) {
        return Status::Error("Unbind failed after searching for user \"%s\" on server \"%s\".",
                              userName_.c_str(), FLAGS_ldap_server.c_str());
    }

    /**
     * Secondly, reinit LDAP connect to LADP server for the second connect.
     * Using the distinguished name found on the first connection and the user's
     * login password to LDAP authentication
     */
    auto reInit = initLDAPConnection();
    if (!reInit.ok()) {
        return reInit;
    }

    struct berval cred2;
    cred2.bv_val = const_cast<char*>(password_.c_str());
    cred2.bv_len = password_.length();

    rc = ldap_sasl_bind_s(ldap_,
                          dn.c_str(),
                          LDAP_SASL_SIMPLE,
                          &cred2,
                          NULL,
                          NULL,
                          NULL);

    ldap_unbind_ext(ldap_, NULL, NULL);
    if (rc != LDAP_SUCCESS) {
        return Status::Error("LDAP login failed for user \"%s\" on server \"%s\".",
                             dn.c_str(), FLAGS_ldap_server.c_str());
    }
    return true;
}


StatusOr<bool> LdapAuthenticator::simpleBindAuth() {
    auto fullUserName = FLAGS_ldap_prefix + userName_ + FLAGS_ldap_suffix;

    struct berval cred;
    cred.bv_val = const_cast<char*>(password_.c_str());
    cred.bv_len = password_.length();

    auto ret = ldap_sasl_bind_s(ldap_,
                                fullUserName.c_str(),
                                LDAP_SASL_SIMPLE,
                                &cred,
                                NULL,
                                NULL,
                                NULL);

    ldap_unbind_ext(ldap_, NULL, NULL);
    if (ret != LDAP_SUCCESS) {
        return Status::Error("LDAP login failed for user \"%s\" on server \"%s\".",
                              fullUserName.c_str(), FLAGS_ldap_server.c_str());
    }
    return true;
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

    ret = initLDAPConnection();
    if (!ret.ok()) {
        LOG(ERROR) << ret.toString();
        return false;
    }

    // Search bind mode
    StatusOr<bool> rets;
    if (!FLAGS_ldap_basedn.empty()) {
        rets = searchBindAuth();
    } else {
        rets = simpleBindAuth();
    }

    if (!rets.ok()) {
        LOG(ERROR) << rets.status().toString();
        return false;
    }
    return rets.value();
}

}   // namespace graph
}   // namespace nebula

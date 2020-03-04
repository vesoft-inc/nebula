/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "graph/GraphFlags.h"
#include "graph/Authenticator.h"
#include "graph/LdapAuthenticator.h"


namespace nebula {
namespace graph {

class LdapAuthTest : public TestBase {
protected:
    void SetUp() override {
        TestBase::SetUp();
        // ...
    }

    void TearDown() override {
        // ...
        TestBase::TearDown();
    }
};

TEST_F(LdapAuthTest, SimpleBindAuth) {
    FLAGS_ldap_server = "127.0.0.1";
    FLAGS_ldap_port = 389;
    FLAGS_ldap_scheme = "ldap";

    FLAGS_ldap_prefix = "uid=";
    FLAGS_ldap_suffix = ",dc=sys,dc=com";

    std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
    auto ret = authenticator_->auth("admin", "123456");
    ASSERT_TRUE(ret);
}

TEST_F(LdapAuthTest, SimpleBindAuthWithTLS) {
    FLAGS_ldap_server = "127.0.0.1";
    FLAGS_ldap_port = 389;
    FLAGS_ldap_scheme = "ldap";
    FLAGS_ldap_tls = true;

    FLAGS_ldap_prefix = "uid=";
    FLAGS_ldap_suffix = ",dc=sys,dc=com";

    std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
    auto ret = authenticator_->auth("admin", "123456");
    ASSERT_TRUE(ret);
}

TEST_F(LdapAuthTest, SearchBindAuth) {
    FLAGS_ldap_server = "127.0.0.1";
    FLAGS_ldap_port = 389;
    FLAGS_ldap_scheme = "ldap";

    FLAGS_ldap_basedn = "dc=sys,dc=com";

    std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
    auto ret = authenticator_->auth("admin", "123456");
    ASSERT_TRUE(ret);
}

TEST_F(LdapAuthTest, SearchBindAuthWithTLS) {
    FLAGS_ldap_server = "127.0.0.1";
    FLAGS_ldap_port = 389;
    FLAGS_ldap_scheme = "ldap";
    FLAGS_ldap_tls = true;

    FLAGS_ldap_basedn = "dc=sys,dc=com";

    std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
    auto ret = authenticator_->auth("admin", "123456");
    ASSERT_TRUE(ret);
}

}   // namespace graph
}   // namespace nebula

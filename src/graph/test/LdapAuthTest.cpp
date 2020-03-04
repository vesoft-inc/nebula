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
    FLAGS_ldap_suffix = ",ou=it,dc=sys,dc=com";

    // Ldap server contains corresponding records
    {
        std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
        auto ret = authenticator_->auth("test2", "passwdtest2");
        ASSERT_TRUE(ret);
    }
    // Ldap server has no corresponding record
    {
        std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
        auto ret = authenticator_->auth("admin", "987");
        ASSERT_FALSE(ret);
    }
    {
        std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
        auto ret = authenticator_->auth("panda", "123456");
        ASSERT_FALSE(ret);
    }
}


/*
TEST_F(LdapAuthTest, SimpleBindAuthWithTLS) {
    FLAGS_ldap_server = "127.0.0.1";
    FLAGS_ldap_port = 389;
    FLAGS_ldap_scheme = "ldap";
    FLAGS_ldap_tls = true;

    FLAGS_ldap_prefix = "uid=";
    FLAGS_ldap_suffix = ",ou=it,dc=sys,dc=com";

    // Ldap server contains corresponding records
    {
        std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
        auto ret = authenticator_->auth("test2", "passwdtest2");
        ASSERT_TRUE(ret);
    }
    // Ldap server has no corresponding record
    {
        std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
        auto ret = authenticator_->auth("admin", "987");
        ASSERT_FALSE(ret);
    }
    {
        std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
        auto ret = authenticator_->auth("panda", "123456");
        ASSERT_FALSE(ret);
    }
}
*/

TEST_F(LdapAuthTest, SearchBindAuth) {
    FLAGS_ldap_server = "127.0.0.1";
    FLAGS_ldap_port = 389;
    FLAGS_ldap_scheme = "ldap";

    FLAGS_ldap_prefix = "";
    FLAGS_ldap_suffix = "";

    FLAGS_ldap_basedn = "uid=test2,ou=it,dc=sys,dc=com";

    // Ldap server contains corresponding records
    {
        std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
        auto ret = authenticator_->auth("test2", "passwdtest2");
        ASSERT_TRUE(ret);
    }
    // Ldap server has no corresponding record
    {
        std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
        auto ret = authenticator_->auth("admin", "1234");
        ASSERT_FALSE(ret);
    }
    {
        std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
        auto ret = authenticator_->auth("panda", "123456");
        ASSERT_FALSE(ret);
    }
}

/*
TEST_F(LdapAuthTest, SearchBindAuthWithTLS) {
    FLAGS_ldap_server = "127.0.0.1";
    FLAGS_ldap_port = 389;
    FLAGS_ldap_scheme = "ldap";
    FLAGS_ldap_tls = true;

    FLAGS_ldap_prefix = "";
    FLAGS_ldap_suffix = "";

    FLAGS_ldap_basedn = "uid=test2,ou=it,dc=sys,dc=com";

    // Ldap server contains corresponding records
    {
        std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
        auto ret = authenticator_->auth("test2", "passwdtest2");
        ASSERT_TRUE(ret);
    }
    // Ldap server has no corresponding record
    {
        std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
        auto ret = authenticator_->auth("admin", "1234");
        ASSERT_FALSE(ret);
    }
    {
        std::unique_ptr<Authenticator> authenticator_ = std::make_unique<LdapAuthenticator>();
        auto ret = authenticator_->auth("panda", "123456");
        ASSERT_FALSE(ret);
    }
}
*/

}   // namespace graph
}   // namespace nebula

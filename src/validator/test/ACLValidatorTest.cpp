/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/test/ValidatorTestBase.h"
#include "planner/plan/Admin.h"

namespace nebula {
namespace graph {


class ACLValidatorTest : public ValidatorTestBase {
protected:
    const PlanNode *getPlanRoot(const std::string query) {
        auto qctxStatus = validate(query);
        EXPECT_TRUE(qctxStatus.ok()) << qctxStatus.status();
        auto qctx = std::move(qctxStatus).value();
        return qctx->plan()->root();
    }
};

TEST_F(ACLValidatorTest, Simple) {
    constexpr char user[] = "shylock";
    constexpr char password[] = "123456";
    constexpr char newPassword[] = "654321";
    constexpr char roleTypeName[] = "ADMIN";
    constexpr char space[] = "test_space";
    // create user
    {
        auto root = getPlanRoot(folly::stringPrintf("CREATE USER %s", user));
        std::vector<PlanNode::Kind> expectedTop {
            PlanNode::Kind::kCreateUser,
            PlanNode::Kind::kStart,
        };
        ASSERT_TRUE(verifyPlan(root, expectedTop));

        ASSERT_EQ(root->kind(), PlanNode::Kind::kCreateUser);
        auto createUser = static_cast<const CreateUser *>(root);
        ASSERT_FALSE(createUser->ifNotExist());
        ASSERT_EQ(*createUser->username(), user);
        ASSERT_EQ(*createUser->password(), "");
    }
    {   // if not exists
        auto root = getPlanRoot(folly::stringPrintf("CREATE USER IF NOT EXISTS %s", user));
        std::vector<PlanNode::Kind> expectedTop{
            PlanNode::Kind::kCreateUser,
            PlanNode::Kind::kStart,
        };
        ASSERT_TRUE(verifyPlan(root, expectedTop));

        ASSERT_EQ(root->kind(), PlanNode::Kind::kCreateUser);
        auto createUser = static_cast<const CreateUser *>(root);
        ASSERT_TRUE(createUser->ifNotExist());
        ASSERT_EQ(*createUser->username(), user);
        ASSERT_EQ(*createUser->password(), "");
    }
    {   // with password
        auto root =
            getPlanRoot(folly::stringPrintf("CREATE USER %s WITH PASSWORD \"%s\"", user, password));
        std::vector<PlanNode::Kind> expectedTop {
            PlanNode::Kind::kCreateUser,
            PlanNode::Kind::kStart,
        };
        ASSERT_TRUE(verifyPlan(root, expectedTop));

        ASSERT_EQ(root->kind(), PlanNode::Kind::kCreateUser);
        auto createUser = static_cast<const CreateUser*>(root);
        ASSERT_FALSE(createUser->ifNotExist());
        ASSERT_EQ(*createUser->username(), user);
        ASSERT_EQ(*createUser->password(), password);
    }

    // drop user
    {
        auto root = getPlanRoot(folly::stringPrintf("DROP USER %s", user));
        std::vector<PlanNode::Kind> expectedTop {
            PlanNode::Kind::kDropUser,
            PlanNode::Kind::kStart,
        };
        ASSERT_TRUE(verifyPlan(root, expectedTop));

        ASSERT_EQ(root->kind(), PlanNode::Kind::kDropUser);
        auto dropUser = static_cast<const DropUser*>(root);
        ASSERT_FALSE(dropUser->ifExist());
        ASSERT_EQ(*dropUser->username(), user);
    }
    {  // if exits
        auto root = getPlanRoot(folly::stringPrintf("DROP USER IF EXISTS %s", user));
        std::vector<PlanNode::Kind> expectedTop {
            PlanNode::Kind::kDropUser,
            PlanNode::Kind::kStart,
        };
        ASSERT_TRUE(verifyPlan(root, expectedTop));

        ASSERT_EQ(root->kind(), PlanNode::Kind::kDropUser);
        auto dropUser = static_cast<const DropUser*>(root);
        ASSERT_TRUE(dropUser->ifExist());
        ASSERT_EQ(*dropUser->username(), user);
    }

    // update user
    {
        auto root =
            getPlanRoot(folly::stringPrintf("ALTER USER %s WITH PASSWORD \"%s\"", user, password));
        std::vector<PlanNode::Kind> expectedTop{
            PlanNode::Kind::kUpdateUser,
            PlanNode::Kind::kStart,
        };
        ASSERT_TRUE(verifyPlan(root, expectedTop));

        ASSERT_EQ(root->kind(), PlanNode::Kind::kUpdateUser);
        auto updateUser = static_cast<const UpdateUser*>(root);
        ASSERT_EQ(*updateUser->username(), user);
        ASSERT_EQ(*updateUser->password(), password);
    }

    // show users
    {
        auto root = getPlanRoot("SHOW USERS");
        std::vector<PlanNode::Kind> expectedTop {
            PlanNode::Kind::kListUsers,
            PlanNode::Kind::kStart,
        };
        ASSERT_TRUE(verifyPlan(root, expectedTop));
    }

    // change password
    {
        auto root = getPlanRoot(folly::stringPrintf(
            "CHANGE PASSWORD %s FROM \"%s\" TO \"%s\"", user, password, newPassword));

        std::vector<PlanNode::Kind> expectedTop {
            PlanNode::Kind::kChangePassword,
            PlanNode::Kind::kStart,
        };
        ASSERT_TRUE(verifyPlan(root, expectedTop));

        ASSERT_EQ(root->kind(), PlanNode::Kind::kChangePassword);
        auto changePassword = static_cast<const ChangePassword*>(root);
        ASSERT_EQ(*changePassword->username(), user);
        ASSERT_EQ(*changePassword->password(), password);
        ASSERT_EQ(*changePassword->newPassword(), newPassword);
    }

    // grant role
    {
        auto root = getPlanRoot(
            folly::stringPrintf("GRANT ROLE %s ON %s TO %s", roleTypeName, space, user));

        std::vector<PlanNode::Kind> expectedTop {
            PlanNode::Kind::kGrantRole,
            PlanNode::Kind::kStart,
        };
        ASSERT_TRUE(verifyPlan(root, expectedTop));

        ASSERT_EQ(root->kind(), PlanNode::Kind::kGrantRole);
        auto grantRole = static_cast<const GrantRole*>(root);
        ASSERT_EQ(*grantRole->username(), user);
        ASSERT_EQ(*grantRole->spaceName(), space);
        ASSERT_EQ(grantRole->role(), meta::cpp2::RoleType::ADMIN);
    }

    // revoke role
    {
        auto root = getPlanRoot(
            folly::stringPrintf("REVOKE ROLE %s ON %s FROM %s", roleTypeName, space, user));

        std::vector<PlanNode::Kind> expectedTop {
            PlanNode::Kind::kRevokeRole,
            PlanNode::Kind::kStart,
        };
        ASSERT_TRUE(verifyPlan(root, expectedTop));

        ASSERT_EQ(root->kind(), PlanNode::Kind::kRevokeRole);
        auto revokeRole = static_cast<const RevokeRole*>(root);
        ASSERT_EQ(*revokeRole->username(), user);
        ASSERT_EQ(*revokeRole->spaceName(), space);
        ASSERT_EQ(revokeRole->role(), meta::cpp2::RoleType::ADMIN);
    }

    // show roles in space
    {
        auto root = getPlanRoot(folly::stringPrintf("SHOW ROLES IN %s", space));

        std::vector<PlanNode::Kind> expectedTop {
            PlanNode::Kind::kListRoles,
            PlanNode::Kind::kStart,
        };
        ASSERT_TRUE(verifyPlan(root, expectedTop));

        ASSERT_EQ(root->kind(), PlanNode::Kind::kListRoles);
        auto showRoles = static_cast<const ListRoles*>(root);
        ASSERT_EQ(showRoles->space(), 1);
    }
}

}  // namespace graph
}  // namespace nebula

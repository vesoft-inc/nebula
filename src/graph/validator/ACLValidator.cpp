/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/clients/meta/MetaClient.h"

#include "planner/plan/Admin.h"
#include "service/PermissionManager.h"
#include "util/SchemaUtil.h"
#include "validator/ACLValidator.h"

namespace nebula {
namespace graph {

static std::size_t kUsernameMaxLength = 16;
static std::size_t kPasswordMaxLength = 24;

// create user
Status CreateUserValidator::validateImpl() {
    const auto *sentence = static_cast<const CreateUserSentence *>(sentence_);
    if (sentence->getAccount()->size() > kUsernameMaxLength) {
        return Status::SemanticError("Username exceed maximum length %ld characters.",
                                     kUsernameMaxLength);
    }
    if (sentence->getPassword()->size() > kPasswordMaxLength) {
        return Status::SemanticError("Password exceed maximum length %ld characters.",
                                     kPasswordMaxLength);
    }
    return Status::OK();
}

Status CreateUserValidator::toPlan() {
    auto sentence = static_cast<CreateUserSentence *>(sentence_);
    return genSingleNodePlan<CreateUser>(
        sentence->getAccount(), sentence->getPassword(), sentence->ifNotExists());
}

// drop user
Status DropUserValidator::validateImpl() {
    const auto *sentence = static_cast<const DropUserSentence *>(sentence_);
    if (sentence->getAccount()->size() > kUsernameMaxLength) {
        return Status::SemanticError("Username exceed maximum length %ld characters.",
                                     kUsernameMaxLength);
    }
    return Status::OK();
}

Status DropUserValidator::toPlan() {
    auto sentence = static_cast<DropUserSentence *>(sentence_);
    return genSingleNodePlan<DropUser>(sentence->getAccount(), sentence->ifExists());
}

// update user
Status UpdateUserValidator::validateImpl() {
    const auto *sentence = static_cast<const AlterUserSentence *>(sentence_);
    if (sentence->getAccount()->size() > kUsernameMaxLength) {
        return Status::SemanticError("Username exceed maximum length %ld characters.",
                                     kUsernameMaxLength);
    }
    if (sentence->getPassword()->size() > kPasswordMaxLength) {
        return Status::SemanticError("Password exceed maximum length %ld characters.",
                                     kPasswordMaxLength);
    }
    return Status::OK();
}

Status UpdateUserValidator::toPlan() {
    auto sentence = static_cast<AlterUserSentence *>(sentence_);
    return genSingleNodePlan<UpdateUser>(sentence->getAccount(), sentence->getPassword());
}

// show users
Status ShowUsersValidator::validateImpl() {
    return Status::OK();
}

Status ShowUsersValidator::toPlan() {
    return genSingleNodePlan<ListUsers>();
}

// change password
Status ChangePasswordValidator::validateImpl() {
    const auto *sentence = static_cast<const ChangePasswordSentence *>(sentence_);
    if (sentence->getAccount()->size() > kUsernameMaxLength) {
        return Status::SemanticError("Username exceed maximum length %ld characters.",
                                     kUsernameMaxLength);
    }
    if (sentence->getOldPwd()->size() > kPasswordMaxLength) {
        return Status::SemanticError("Old password exceed maximum length %ld characters.",
                                     kPasswordMaxLength);
    }
    if (sentence->getNewPwd()->size() > kPasswordMaxLength) {
        return Status::SemanticError("New password exceed maximum length %ld characters.",
                                     kPasswordMaxLength);
    }
    return Status::OK();
}

Status ChangePasswordValidator::toPlan() {
    auto sentence = static_cast<ChangePasswordSentence *>(sentence_);
    return genSingleNodePlan<ChangePassword>(
        sentence->getAccount(), sentence->getOldPwd(), sentence->getNewPwd());
}

// grant role
Status GrantRoleValidator::validateImpl() {
    const auto *sentence = static_cast<const GrantSentence *>(sentence_);
    if (sentence->getAccount()->size() > kUsernameMaxLength) {
        return Status::SemanticError("Username exceed maximum length %ld characters.",
                                     kUsernameMaxLength);
    }
    return Status::OK();
}

Status GrantRoleValidator::toPlan() {
    auto sentence = static_cast<GrantSentence *>(sentence_);
    return genSingleNodePlan<GrantRole>(sentence->getAccount(),
                                        sentence->getAclItemClause()->getSpaceName(),
                                        sentence->getAclItemClause()->getRoleType());
}

// revoke role
Status RevokeRoleValidator::validateImpl() {
    const auto *sentence = static_cast<const RevokeSentence *>(sentence_);
    if (sentence->getAccount()->size() > kUsernameMaxLength) {
        return Status::SemanticError("Username exceed maximum length %ld characters.",
                                     kUsernameMaxLength);
    }
    return Status::OK();
}

Status RevokeRoleValidator::toPlan() {
    auto sentence = static_cast<RevokeSentence *>(sentence_);
    return genSingleNodePlan<RevokeRole>(sentence->getAccount(),
                                         sentence->getAclItemClause()->getSpaceName(),
                                         sentence->getAclItemClause()->getRoleType());
}

// show roles in space
Status ShowRolesInSpaceValidator::validateImpl() {
    auto sentence = static_cast<ShowRolesSentence *>(sentence_);
    auto spaceIdResult = qctx_->schemaMng()->toGraphSpaceID(*sentence->name());
    NG_RETURN_IF_ERROR(spaceIdResult);
    targetSpaceId_ = spaceIdResult.value();
    return Status::OK();
}

Status ShowRolesInSpaceValidator::checkPermission() {
    return PermissionManager::canReadSpace(qctx_->rctx()->session(), targetSpaceId_);
}

Status ShowRolesInSpaceValidator::toPlan() {
    return genSingleNodePlan<ListRoles>(targetSpaceId_);
}

}   // namespace graph
}   // namespace nebula

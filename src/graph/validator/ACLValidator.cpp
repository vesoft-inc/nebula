/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/ACLValidator.h"

#include "common/base/Status.h"
#include "graph/planner/plan/Admin.h"
#include "graph/service/PermissionManager.h"
#include "graph/validator/Validator.h"

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

Status DropUserValidator::checkPermission() {
  auto r = Validator::checkPermission();
  NG_RETURN_IF_ERROR(r);
  const auto *sentence = static_cast<const DropUserSentence *>(sentence_);
  if (*sentence->getAccount() == "root") {
    return Status::SemanticError("Can't drop root user.");
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
  const std::string *account = sentence->getAccount();
  if (account->size() > kUsernameMaxLength) {
    return Status::SemanticError("Username exceed maximum length %ld characters.",
                                 kUsernameMaxLength);
  }
  auto metaClient = qctx_->getMetaClient();
  auto roles = metaClient->getRolesByUserFromCache(*account);
  for (const auto &role : roles) {
    if (role.get_role_type() == meta::cpp2::RoleType::GOD) {
      return Status::SemanticError("User '%s' is GOD, cannot be granted.", account->c_str());
    }
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

// describe user
Status DescribeUserValidator::validateImpl() {
  auto sentence = static_cast<DescribeUserSentence *>(sentence_);
  if (sentence->account()->size() > kUsernameMaxLength) {
    return Status::SemanticError("Username exceed maximum length %ld characters.",
                                 kUsernameMaxLength);
  }
  if (!inputs_.empty()) {
    return Status::SemanticError("Show queries sentence do not support input");
  }
  outputs_.emplace_back("role", Value::Type::STRING);
  outputs_.emplace_back("space", Value::Type::STRING);
  return Status::OK();
}

Status DescribeUserValidator::checkPermission() {
  auto sentence = static_cast<DescribeUserSentence *>(sentence_);
  return PermissionManager::canReadUser(qctx_->rctx()->session(), *sentence->account());
}

Status DescribeUserValidator::toPlan() {
  auto sentence = static_cast<DescribeUserSentence *>(sentence_);
  return genSingleNodePlan<DescribeUser>(sentence->account());
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

}  // namespace graph
}  // namespace nebula

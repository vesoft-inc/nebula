/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/PrivilegeExecutor.h"

namespace nebula {
namespace graph {

GrantExecutor::GrantExecutor(Sentence *sentence,
                             ExecutionContext *ectx)
    : Executor(ectx, "grant") {
    sentence_ = static_cast<GrantSentence*>(sentence);
}

Status GrantExecutor::prepare() {
    return Status::OK();
}

void GrantExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto* account = sentence_->getAccount();
    auto* aclItem = sentence_->getAclItemClause();

    nebula::cpp2::RoleItem roleItem;
    if (aclItem->getRoleType() == RoleTypeClause::RoleType::GOD) {
        doError(Status::PermissionError("Permission denied"));
        return;
    }
    auto spaceRet = mc->getSpaceIdByNameFromCache(*aclItem->getSpaceName());
    if (!spaceRet.ok()) {
        doError(spaceRet.status());
        return;
    }
    /**
     * Permission check.
     */
    auto *session = ectx()->rctx()->session();
    auto role = session->toRole(PrivilegeUtils::toRoleType(aclItem->getRoleType()));
    auto rst = permission::PermissionManager::canWriteRole(session,
                                                           role,
                                                           spaceRet.value(),
                                                           *account);
    if (!rst) {
        doError(Status::PermissionError("Permission denied"));
        return;
    }

    roleItem.set_user(*account);
    roleItem.set_space_id(spaceRet.value());
    roleItem.set_role_type(PrivilegeUtils::toRoleType(aclItem->getRoleType()));
    auto future = mc->grantToUser(roleItem);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(Status::Error("Grant failed: %s.",
                                  resp.status().toString().c_str()));
            return;
        }
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Grant exception: %s.",
                                       e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

RevokeExecutor::RevokeExecutor(Sentence *sentence,
                               ExecutionContext *ectx)
    : Executor(ectx, "revoke") {
    sentence_ = static_cast<RevokeSentence*>(sentence);
}

Status RevokeExecutor::prepare() {
    return Status::OK();
}

void RevokeExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto* account = sentence_->getAccount();
    auto* aclItem = sentence_->getAclItemClause();

    if (aclItem->getRoleType() == RoleTypeClause::RoleType::GOD) {
        doError(Status::PermissionError("Permission denied"));
        return;
    }

    auto spaceRet = mc->getSpaceIdByNameFromCache(*aclItem->getSpaceName());
    if (!spaceRet.ok()) {
        doError(spaceRet.status());
        return;
    }

    /**
     * Permission check.
     */
    auto *session = ectx()->rctx()->session();
    auto role = session->toRole(PrivilegeUtils::toRoleType(aclItem->getRoleType()));
    auto rst = permission::PermissionManager::canWriteRole(session,
                                                           role,
                                                           spaceRet.value(),
                                                           *account);
    if (!rst) {
        doError(Status::PermissionError("Permission denied"));
        return;
    }

    nebula::cpp2::RoleItem roleItem;
    roleItem.set_user(*account);
    roleItem.set_space_id(spaceRet.value());
    roleItem.set_role_type(PrivilegeUtils::toRoleType(aclItem->getRoleType()));
    auto future = mc->revokeFromUser(roleItem);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(Status::Error("Revoke failed: %s.",
                                  resp.status().toString().c_str()));
            return;
        }
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Revoke exception: %s.",
                                       e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula

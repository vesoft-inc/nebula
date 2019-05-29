/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/UserExecutor.h"
#include "base/MD5Utils.h"

namespace nebula {
namespace graph {

CreateUserExecutor::CreateUserExecutor(Sentence *sentence,
                                       ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<CreateUserSentence*>(sentence);
}

Status CreateUserExecutor::prepare() {
    password_ = sentence_->getPassword();
    missingOk_ = sentence_->getMissingOk();
    userItem_.set_account(sentence_->getAccount()->data());
    auto opts = sentence_->getOpts();
    for (auto &opt : opts) {
        switch (opt->getOptType()) {
            case WithUserOptItem::OptionType::LOCK:
            {
                userItem_.set_is_lock(WithUserOptItem::asBool(opt->getOptVal()));
                break;
            }
            case WithUserOptItem::OptionType::MAX_QUERIES_PER_HOUR:
            {
                userItem_.set_max_queries_per_hour(WithUserOptItem::asInt(opt->getOptVal()));
                break;
            }
            case WithUserOptItem::OptionType::MAX_UPDATES_PER_HOUR:
            {
                userItem_.set_max_updates_per_hour(WithUserOptItem::asInt(opt->getOptVal()));
                break;
            }
            case WithUserOptItem::OptionType::MAX_CONNECTIONS_PER_HOUR:
            {
                userItem_.set_max_connections_per_hour(WithUserOptItem::asInt(opt->getOptVal()));
                break;
            }
            case WithUserOptItem::OptionType::MAX_USER_CONNECTIONS:
            {
                userItem_.set_max_user_connections(WithUserOptItem::asInt(opt->getOptVal()));
                break;
            }
        }
    }
    return Status::OK();
}

void CreateUserExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto future = mc->createUser(userItem_, MD5Utils::md5Encode(*password_), missingOk_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(resp.status());
            return;
        }

        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}


DropUserExecutor::DropUserExecutor(Sentence *sentence,
                                   ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DropUserSentence*>(sentence);
}

Status DropUserExecutor::prepare() {
    account_ = sentence_->getAccount();
    missingOk_ = sentence_->getMissingOk();
    return Status::OK();
}

void DropUserExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto future = mc->dropUser(*account_, missingOk_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(resp.status());
            return;
        }

        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}


AlterUserExecutor::AlterUserExecutor(Sentence *sentence,
                                   ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<AlterUserSentence*>(sentence);
}

Status AlterUserExecutor::prepare() {
    userItem_.set_account(sentence_->getAccount()->data());
    auto opts = sentence_->getOpts();
    for (auto &opt : opts) {
        switch (opt->getOptType()) {
            case WithUserOptItem::OptionType::LOCK:
            {
                userItem_.__isset.is_lock = true;
                userItem_.set_is_lock(WithUserOptItem::asBool(opt->getOptVal()));
                break;
            }
            case WithUserOptItem::OptionType::MAX_QUERIES_PER_HOUR:
            {
                userItem_.__isset.max_queries_per_hour = true;
                userItem_.set_max_queries_per_hour(WithUserOptItem::asInt(opt->getOptVal()));
                break;
            }
            case WithUserOptItem::OptionType::MAX_UPDATES_PER_HOUR:
            {
                userItem_.__isset.max_updates_per_hour = true;
                userItem_.set_max_updates_per_hour(WithUserOptItem::asInt(opt->getOptVal()));
                break;
            }
            case WithUserOptItem::OptionType::MAX_CONNECTIONS_PER_HOUR:
            {
                userItem_.__isset.max_connections_per_hour = true;
                userItem_.set_max_connections_per_hour(WithUserOptItem::asInt(opt->getOptVal()));
                break;
            }
            case WithUserOptItem::OptionType::MAX_USER_CONNECTIONS:
            {
                userItem_.__isset.max_user_connections = true;
                userItem_.set_max_user_connections(WithUserOptItem::asInt(opt->getOptVal()));
                break;
            }
        }
    }
    return Status::OK();
}

void AlterUserExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto future = mc->alterUser(userItem_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(resp.status());
            return;
        }

        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}


GrantExecutor::GrantExecutor(Sentence *sentence,
                                   ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<GrantSentence*>(sentence);
}

Status GrantExecutor::prepare() {
    aclItem_ = sentence_->getAclItemClause();
    type_ = toRole(aclItem_->getRoleType());
    const auto& space = aclItem_->getSpaceName();
    const auto& user = sentence_->getAccount();
    auto spaceRet = ectx()->getMetaClient()->getSpaceIdByNameFromCache(space->data());
    if (!spaceRet.ok()) {
        return Status::Error("Space not found : '%s'", space);
    }
    auto userRet = ectx()->getMetaClient()->getUserIdByNameFromCache(user->data());
    if (!userRet.ok()) {
        return Status::Error("User not found : '%s'", user);
    }
    roleItem_.set_space_id(spaceRet.value());
    roleItem_.set_user_id(userRet.value());
    roleItem_.set_role_type(type_);
    return Status::OK();
}

void GrantExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto future = mc->grantToUser(roleItem_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(resp.status());
            return;
        }

        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}


RevokeExecutor::RevokeExecutor(Sentence *sentence,
                                   ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<RevokeSentence*>(sentence);
}

Status RevokeExecutor::prepare() {
    aclItem_ = sentence_->getAclItemClause();
    type_ = toRole(aclItem_->getRoleType());
    const auto& space = aclItem_->getSpaceName();
    const auto& user = sentence_->getAccount();
    auto spaceRet = ectx()->getMetaClient()->getSpaceIdByNameFromCache(space->data());
    if (!spaceRet.ok()) {
        return Status::Error("Space not found : '%s'", space);
    }
    auto userRet = ectx()->getMetaClient()->getUserIdByNameFromCache(user->data());
    if (!userRet.ok()) {
        return Status::Error("User not found : '%s'", user);
    }
    roleItem_.set_space_id(spaceRet.value());
    roleItem_.set_user_id(userRet.value());
    roleItem_.set_role_type(type_);
    return Status::OK();
}

void RevokeExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto future = mc->revokeFromUser(roleItem_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(resp.status());
            return;
        }

        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}


ChangePasswordExecutor::ChangePasswordExecutor(Sentence *sentence,
                                   ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<ChangePasswordSentence*>(sentence);
}

Status ChangePasswordExecutor::prepare() {
    account_ = sentence_->getAccount();
    newPassword_ = sentence_->getNewPwd();
    oldPassword_ = sentence_->getOldPwd();
    return Status::OK();
}

void ChangePasswordExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    const auto user = ectx()->rctx()->session()->user();
    auto verifyNeed = mc->checkIsGodUserInCache(user);
    auto future = mc->changePassword(*account_,
                                     MD5Utils::md5Encode(*newPassword_),
                                     MD5Utils::md5Encode(*oldPassword_),
                                     verifyNeed);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(resp.status());
            return;
        }

        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}
}   // namespace graph
}   // namespace nebula



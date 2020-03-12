/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/encryption/MD5Utils.h"
#include "graph/UserExecutor.h"

namespace nebula {
namespace graph {

CreateUserExecutor::CreateUserExecutor(Sentence *sentence,
                                       ExecutionContext *ectx)
    : Executor(ectx, "create_user") {
    sentence_ = static_cast<CreateUserSentence*>(sentence);
}

Status CreateUserExecutor::prepare() {
    return Status::OK();
}

void CreateUserExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto* account = sentence_->getAccount();
    auto* password = sentence_->getPassword();
    auto future = mc->createUser(*account,
                                 encryption::MD5Utils::md5Encode(*password),
                                 sentence_->ifNotExists());
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            onError_(resp.status());
            return;
        }
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

DropUserExecutor::DropUserExecutor(Sentence *sentence,
                                   ExecutionContext *ectx)
    : Executor(ectx, "drop_user") {
    sentence_ = static_cast<DropUserSentence*>(sentence);
}

Status DropUserExecutor::prepare() {
    return Status::OK();
}

void DropUserExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto *account = sentence_->getAccount();
    auto future = mc->dropUser(*account, sentence_->ifExists());
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(Status::Error("Drop user failed: %s.", resp.status().toString().c_str()));
            return;
        }
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Drop user exception: %s.",
                                       e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

AlterUserExecutor::AlterUserExecutor(Sentence *sentence,
                                     ExecutionContext *ectx)
    : Executor(ectx, "alter_user") {
    sentence_ = static_cast<AlterUserSentence*>(sentence);
}

Status AlterUserExecutor::prepare() {
    return Status::OK();
}

void AlterUserExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto* account = sentence_->getAccount();
    auto* password = sentence_->getPassword();
    auto future = mc->alterUser(*account, encryption::MD5Utils::md5Encode(*password));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            onError_(resp.status());
            return;
        }
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

ChangePasswordExecutor::ChangePasswordExecutor(Sentence *sentence,
                                               ExecutionContext *ectx)
    : Executor(ectx, "change_password") {
    sentence_ = static_cast<ChangePasswordSentence*>(sentence);
}

Status ChangePasswordExecutor::prepare() {
    return Status::OK();
}

void ChangePasswordExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto* account = sentence_->getAccount();
    auto newPwd = encryption::MD5Utils::md5Encode(*sentence_->getNewPwd());
    auto oldPwd = encryption::MD5Utils::md5Encode(*sentence_->getOldPwd());
    auto future = mc->changePassword(*account, newPwd, oldPwd);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            onError_(resp.status());
            return;
        }
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula

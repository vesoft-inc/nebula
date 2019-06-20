/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PARSER_USEREXECUTOR_H_
#define PARSER_USEREXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class CreateUserExecutor final : public Executor {
public:
    CreateUserExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "CreateUserExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    CreateUserSentence                           *sentence_{nullptr};
    meta::cpp2::UserItem                          userItem_{apache::thrift::FRAGILE,
                                                            "", false, 0, 0, 0, 0};
    const std::string                            *password_{nullptr};
    bool                                          missingOk_{false};
};


class DropUserExecutor final : public Executor {
public:
    DropUserExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "DropUserExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    DropUserSentence                             *sentence_{nullptr};
    const std::string                            *account_{nullptr};
    bool                                          missingOk_{false};
};


class AlterUserExecutor final : public Executor {
public:
    AlterUserExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "AlterUserExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    AlterUserSentence                            *sentence_{nullptr};
    meta::cpp2::UserItem                          userItem_;
};


class GrantExecutor final : public Executor {
public:
    GrantExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "GrantExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    GrantSentence                                *sentence_{nullptr};
    AclItemClause                                *aclItem_{nullptr};
    meta::cpp2::RoleType                          type_;
    meta::cpp2::RoleItem                          roleItem_;
};


class RevokeExecutor final : public Executor {
public:
    RevokeExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "RevokeExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    RevokeSentence                               *sentence_{nullptr};
    AclItemClause                                *aclItem_{nullptr};
    meta::cpp2::RoleType                          type_;
    meta::cpp2::RoleItem                          roleItem_;
};


class ChangePasswordExecutor final : public Executor {
public:
    ChangePasswordExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "ChangePasswordExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    ChangePasswordSentence                       *sentence_{nullptr};
    const std::string                            *account_{nullptr};
    const std::string                            *newPassword_{nullptr};
    const std::string                            *oldPassword_{nullptr};
};

}   // namespace graph
}   // namespace nebula
#endif  // PARSER_USEREXECUTOR_H_


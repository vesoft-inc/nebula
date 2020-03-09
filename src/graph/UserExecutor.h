/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef GRAPH_USEREXECUTOR_H_
#define GRAPH_USEREXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"
#include "meta/SchemaManager.h"

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
    CreateUserSentence                          *sentence_{nullptr};
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
    DropUserSentence                            *sentence_{nullptr};
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
    AlterUserSentence                           *sentence_{nullptr};
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
    ChangePasswordSentence                           *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula
#endif   // GRAPH_USEREXECUTOR_H_

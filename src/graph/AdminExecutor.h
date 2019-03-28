/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_ADMINEXECUTOR_H_
#define GRAPH_ADMINEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class ShowExecutor final : public Executor {
public:
    ShowExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "ShowExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    ShowSentence            *sentence_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>  resp_;
};


class AddHostsExecutor final : public Executor {
public:
    AddHostsExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "AddHostsExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    AddHostsSentence        *sentence_{nullptr};
    std::vector<HostAddr>    host_;
};


class RemoveHostsExecutor final : public Executor {
public:
    RemoveHostsExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "RemoveHostsExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    RemoveHostsSentence     *sentence_{nullptr};
    std::vector<HostAddr>    host_;
};


class CreateSpaceExecutor final : public Executor {
public:
    CreateSpaceExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "CreateSpaceExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    CreateSpaceSentence     *sentence_{nullptr};
    std::string             *spaceName_{nullptr};
    int32_t                  partNum_{0};
    int32_t                  replicaFactor_{0};
};


class DropSpaceExecutor final : public Executor {
public:
    DropSpaceExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "DropSpaceExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    DropSpaceSentence       *sentence_{nullptr};
    std::string             *spaceName_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_ADMINEXECUTOR_H_

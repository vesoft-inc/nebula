/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_INSTALLPLUGINEXECUTOR_H_
#define GRAPH_INSTALLPLUGINEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class InstallPluginExecutor final : public Executor {
public:
    InstallPluginExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "InstallPluginExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    InstallPluginSentence                   *sentence_{nullptr};
    const std::string                       *pluginName_{nullptr};
    const std::string                       *soName_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_INSTALLPLUGINEXECUTOR_H_

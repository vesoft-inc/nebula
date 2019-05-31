/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_CONFIGEXECUTOR_H_
#define GRAPH_CONFIGEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class ConfigExecutor final : public Executor {
public:
    ConfigExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "ConfigExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

    void showVariables();
    void setVariables();
    void getVariables();
    void declareVariables();

private:
    std::vector<cpp2::ColumnValue> genRow(const meta::ConfigItem& item);
    meta::cpp2::ConfigModule StringToConfigModule(std::string module);
    std::string ConfigModuleToString(meta::cpp2::ConfigModule module);

    ConfigSentence                           *sentence_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>  resp_;
    ConfigRowItem                            *configItem_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_CONFIGEXECUTOR_H_

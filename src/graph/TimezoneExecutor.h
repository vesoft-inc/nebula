/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#ifndef GRAPH_TIMEZONEEXECUTOR_H
#define GRAPH_TIMEZONEEXECUTOR_H

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class TimezoneExecutor final : public Executor {
public:
    TimezoneExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "TimezoneExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void getTimezone();

    void setTimezone();

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    TimezoneSentence                          *sentence_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>  resp_;
    nebula::Timezone                          timezone_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_TIMEZONEEXECUTOR_H

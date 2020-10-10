/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_SETEXECUTOR_H_
#define GRAPH_SETEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "meta/SchemaProviderIf.h"
#include <boost/thread/latch.hpp>

namespace nebula {
namespace graph {

class SetExecutor final : public TraverseExecutor {
public:
    SetExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "SetExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void feedResult(std::unique_ptr<InterimResult> result) override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    void setLeft();

    void setRight();

    void finishExecution(std::unique_ptr<InterimResult> result);

    void finishExecution(std::vector<cpp2::RowValue> leftRows);

    void doUnion();

    void doIntersect();

    void doMinus();

    Status checkSchema();

    Status doCasting(std::vector<cpp2::RowValue> &rows) const;

    void doDistinct(std::vector<cpp2::RowValue> &rows) const;

    void onEmptyInputs();

private:
    SetSentence                                                *sentence_{nullptr};
    std::unique_ptr<TraverseExecutor>                           left_;
    std::unique_ptr<TraverseExecutor>                           right_;
    std::unique_ptr<InterimResult>                              leftResult_;
    std::unique_ptr<InterimResult>                              rightResult_;
    std::vector<folly::Future<folly::Unit>>                     futures_;
    Status                                                      leftS_;
    Status                                                      rightS_;
    std::vector<std::pair<uint64_t, nebula::cpp2::ValueType>>   castingMap_;
    std::vector<std::string>                                    colNames_;
    std::shared_ptr<const meta::SchemaProviderIf>               resultSchema_;
    std::unique_ptr<cpp2::ExecutionResponse>                    resp_;
    bool                                                        hasFeedResult_{false};
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_SETEXECUTOR_H_

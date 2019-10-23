/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_GROUPBYEXECUTOR_H_
#define GRAPH_GROUPBYEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"

namespace nebula {
namespace graph {

class GroupByExecutor final : public TraverseExecutor {
public:
    GroupByExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "GroupByExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void feedResult(std::unique_ptr<InterimResult> result) override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    Status prepareGroup();
    Status prepareYield();
    Status buildIndex();

    Status groupingData();
    void generateOutputSchema();

    std::vector<std::string> getResultColumnNames() const;
    std::unique_ptr<InterimResult> setupInterimResult();

    cpp2::ColumnValue toColumnValue(const VariantType& value);


private:
    GroupBySentence                                          *sentence_{nullptr};
    std::vector<cpp2::RowValue>                               rows_;
    std::shared_ptr<const meta::SchemaProviderIf>             schema_{nullptr};

    // index of input data and YieldColumn
    using ColumnIndexs = std::vector<std::pair<YieldColumn*, int64_t>>;

    ColumnIndexs                                               groupCols_;
    ColumnIndexs                                               yieldCols_;
    std::shared_ptr<SchemaWriter>                              resultSchema_{nullptr};
    std::unique_ptr<ExpressionContext>                         expCtx_;
    // key: alias , value input name
    std::unordered_map<std::string, std::string>               aliases_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_GROUPBYEXECUTOR_H

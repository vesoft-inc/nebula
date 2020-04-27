/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_FETCHEXECUTOR_H_
#define GRAPH_FETCHEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {
class FetchExecutor : public TraverseExecutor {
public:
    explicit FetchExecutor(ExecutionContext *ectx,
                           const std::string &statsName) : TraverseExecutor(ectx, statsName) {}

    void setupResponse(cpp2::ExecutionResponse &resp) override;

protected:
    Status prepareYield();

    void setupColumns();

    void onEmptyInputs();

    Status getOutputSchema(
            meta::SchemaProviderIf *schema,
            const RowReader *reader,
            SchemaWriter *outputSchema) const;

    void finishExecution(std::unique_ptr<RowSetWriter> rsWriter);

    void doEmptyResp();

protected:
    GraphSpaceID                                    spaceId_{INT_MIN};
    std::unique_ptr<ExpressionContext>              expCtx_;
    std::string                                    *labelName_{nullptr};
    std::shared_ptr<const meta::SchemaProviderIf>   labelSchema_;
    YieldClause                                    *yieldClause_{nullptr};
    std::vector<YieldColumn*>                       yields_;
    std::unique_ptr<YieldColumns>                   yieldColsHolder_;
    bool                                            distinct_{false};
    std::vector<std::string>                        resultColNames_;
    std::vector<std::string>                        returnColNames_;
    std::unique_ptr<cpp2::ExecutionResponse>        resp_;
    std::vector<nebula::cpp2::SupportedType>        colTypes_;
};
}  // namespace graph
}  // namespace nebula
#endif

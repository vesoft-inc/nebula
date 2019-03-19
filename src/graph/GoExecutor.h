/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_GOEXECUTOR_H_
#define GRAPH_GOEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "storage/client/StorageClient.h"

namespace nebula {

namespace storage {
namespace cpp2 {
class QueryResponse;
}   // namespace cpp2
}   // namespace storage

namespace graph {

class GoExecutor final : public TraverseExecutor {
public:
    GoExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "GoExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void feedResult(std::unique_ptr<IntermResult> result) override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    /**
     * To do some preparing works on the clauses
     */
    Status prepareStep();

    Status prepareFrom();

    Status prepareOver();

    Status prepareWhere();

    Status prepareYield();

    Status prepareNeededProps();

    /**
     * To check if this is the final step.
     */
    bool isFinalStep() const {
        return curStep_ == steps_;
    }

    /**
     * To check if `UPTO' is specified.
     * If so, we are supposed to apply the filter in each step.
     */
    bool isUpto() const {
        return upto_;
    }

    /**
     * To check if `REVERSELY' is specified.
     * If so, we step out in a reverse manner.
     */
    bool isReversely() const {
        return reversely_;
    }

    /**
     * To obtain the source ids from various places,
     * such as the literal id list, inputs from the pipeline or results of variable.
     */
    Status setupStarts();

    /**
     * To step out for one step.
     */
    void stepOut();

    using RpcResponse = storage::StorageRpcResponse<storage::cpp2::QueryResponse>;
    /**
     * Callback invoked upon the response of stepping out arrives.
     */
    void onStepOutResponse(RpcResponse &&rpcResp);

    /**
     * Callback invoked when the stepping out action reaches the dead end.
     */
    void onEmptyInputs();

    /**
     * Callback invoked upon the response of retrieving terminal vertex arrives.
     */
    void onVertexProps(RpcResponse &&rpcResp);

    std::vector<storage::cpp2::PropDef> getStepOutProps() const;
    std::vector<storage::cpp2::PropDef> getDstProps() const;

    void fetchVertexProps(std::vector<VertexID> ids, RpcResponse &&rpcResp);

    /**
     * To retrieve or generate the column names for the execution result.
     */
    std::vector<std::string> getResultColumnNames() const;

    /**
     * To retrieve the dst ids from a stepping out response.
     */
    std::vector<VertexID> getDstIdsFromResp(RpcResponse &rpcResp) const;

    /**
     * To setup an intermediate representation of the execution result,
     * which is about to be piped to the next executor.
     */
    std::unique_ptr<IntermResult> setupIntermResult(RpcResponse && rpcResp);

    /**
     * To setup the header of the execution result, i.e. the column names.
     */
    void setupResponseHeader(cpp2::ExecutionResponse &resp) const;

    /**
     * To setup the body of the execution result.
     */
    void setupResponseBody(RpcResponse &rpcResp, cpp2::ExecutionResponse &resp) const;

    /**
     * To iterate on the final data collection, and evaluate the filter and yield columns.
     * For each row that matches the filter, `cb' would be invoked.
     */
    using Callback = std::function<void(std::vector<VariantType>)>;
    void processFinalResult(RpcResponse &rpcResp, Callback cb) const;

    /**
     * A container to hold the mapping from vertex id to its properties, used for lookups
     * during the final evaluation process.
     */
    class VertexHolder final {
    public:
        VariantType get(VertexID id, const std::string &prop) const;
        void add(const storage::cpp2::QueryResponse &resp);

    private:
        std::shared_ptr<ResultSchemaProvider>       schema_;
        std::unordered_map<VertexID, std::string>   data_;
    };

private:
    GoSentence                                 *sentence_{nullptr};
    uint32_t                                    steps_{1};
    uint32_t                                    curStep_{1};
    bool                                        upto_{false};
    bool                                        reversely_{false};
    EdgeType                                    edge_;
    std::string                                *varname_{nullptr};
    std::string                                *colname_{nullptr};
    Expression                                 *filter_{nullptr};
    std::vector<YieldColumn*>                   yields_;
    std::unique_ptr<IntermResult>               inputs_;
    std::unique_ptr<ExpressionContext>          expctx_;
    std::vector<VertexID>                       starts_;
    std::unique_ptr<VertexHolder>               vertexHolder_;
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_GOEXECUTOR_H_

/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_GOEXECUTOR_H_
#define GRAPH_GOEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "storage/client/StorageClient.h"

DECLARE_bool(filter_pushdown);

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

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    /**
     * To do some preparing works on the clauses
     */
    Status prepareClauses();

    Status prepareStep();

    Status prepareFrom();

    Status prepareOver();

    Status prepareWhere();

    Status prepareYield();

    Status prepareNeededProps();

    Status prepareDistinct();

    Status prepareOverAll();

    Status addToEdgeTypes(EdgeType type);

    Status checkNeededProps();

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

    StatusOr<std::vector<storage::cpp2::PropDef>> getStepOutProps();
    StatusOr<std::vector<storage::cpp2::PropDef>> getDstProps();

    void fetchVertexProps(std::vector<VertexID> ids, RpcResponse &&rpcResp);

    void maybeFinishExecution(RpcResponse &&rpcResp);

    /**
     * To retrieve or generate the column names for the execution result.
     */
    std::vector<std::string> getResultColumnNames() const;

    /**
     * To retrieve the dst ids from a stepping out response.
     */
    StatusOr<std::vector<VertexID>> getDstIdsFromResp(RpcResponse &rpcResp) const;

    /**
     * get the edgeName when over all edges
     */
    std::vector<std::string> getEdgeNames() const;
    /**
     * All required data have arrived, finish the execution.
     */
    void finishExecution(RpcResponse &&rpcResp);

    /**
     * To setup an intermediate representation of the execution result,
     * which is about to be piped to the next executor.
     */
    bool setupInterimResult(RpcResponse &&rpcResp, std::unique_ptr<InterimResult> &result);

    /**
     * To setup the header of the execution result, i.e. the column names.
     */
    void setupResponseHeader(cpp2::ExecutionResponse &resp) const;

    /**
     * To setup the body of the execution result.
     */
    bool setupResponseBody(RpcResponse &rpcResp, cpp2::ExecutionResponse &resp) const;

    /**
     * To iterate on the final data collection, and evaluate the filter and yield columns.
     * For each row that matches the filter, `cb' would be invoked.
     */
    using Callback = std::function<Status(std::vector<VariantType>,
                                          const std::vector<nebula::cpp2::SupportedType>&)>;

    bool processFinalResult(RpcResponse &rpcResp, Callback cb) const;

    StatusOr<std::vector<cpp2::RowValue>> toThriftResponse(RpcResponse&& resp);

    /**
     * A container to hold the mapping from vertex id to its properties, used for lookups
     * during the final evaluation process.
     */
    class VertexHolder final {
    public:
        OptVariantType getDefaultProp(TagID tid, const std::string &prop) const;
        OptVariantType get(VertexID id, TagID tid, const std::string &prop) const;
        void add(const storage::cpp2::QueryResponse &resp);
        nebula::cpp2::SupportedType getDefaultPropType(TagID tid, const std::string &prop) const;
        nebula::cpp2::SupportedType getType(VertexID id, TagID tid, const std::string &prop);

    private:
        using VData = std::tuple<std::shared_ptr<ResultSchemaProvider>, std::string>;
        std::unordered_map<VertexID, std::unordered_map<TagID, VData>> data_;
    };

    class VertexBackTracker final {
    public:
        void add(VertexID src, VertexID dst) {
            VertexID value = src;
            auto iter = mapping_.find(src);
            if (iter != mapping_.end()) {
                value = iter->second;
            }
            mapping_[dst] = value;
        }

        VertexID get(VertexID id) {
            auto iter = mapping_.find(id);
            DCHECK(iter != mapping_.end());
            return iter->second;
        }

    private:
         std::unordered_map<VertexID, VertexID>     mapping_;
    };

    OptVariantType getPropFromInterim(VertexID id, const std::string &prop) const;

    enum FromType {
        kInstantExpr,
        kVariable,
        kPipe,
    };

private:
    GoSentence                                 *sentence_{nullptr};
    FromType                                    fromType_{kInstantExpr};
    uint32_t                                    steps_{1};
    uint32_t                                    curStep_{1};
    bool                                        upto_{false};
    OverClause::Direction                       direction_{OverClause::Direction::kForward};
    std::vector<EdgeType>                       edgeTypes_;
    std::string                                *varname_{nullptr};
    std::string                                *colname_{nullptr};
    std::unique_ptr<WhereWrapper>               whereWrapper_;
    std::vector<YieldColumn*>                   yields_;
    std::unique_ptr<YieldClauseWrapper>         yieldClauseWrapper_;
    bool                                        distinct_{false};
    bool                                        distinctPushDown_{false};
    using InterimIndex = InterimResult::InterimResultIndex;
    std::unique_ptr<InterimIndex>               index_;
    std::unique_ptr<ExpressionContext>          expCtx_;
    std::vector<VertexID>                       starts_;
    std::unique_ptr<VertexHolder>               vertexHolder_;
    std::unique_ptr<VertexBackTracker>          backTracker_;
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
    // The name of Tag or Edge, index of prop in data
    using SchemaPropIndex = std::unordered_map<std::pair<std::string, std::string>, int64_t>;
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_GOEXECUTOR_H_

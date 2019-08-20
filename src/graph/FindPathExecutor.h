/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_FINDPATHEXECUTOR_H_
#define GRAPH_FINDPATHEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {
class FindPathExecutor final : public TraverseExecutor {
public:
    FindPathExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "FindPathExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    class Vertices {
    public:
        std::string             *colname_{nullptr};
        std::string             *varname_{nullptr};
        std::vector<VertexID>    vids_;
    };

    class Over {
    public:
        std::string *edge_{nullptr};
        EdgeType     edgeType_{INT_MIN};
        bool         reversely_{false};
    };

    class Step {
    public:
        uint32_t steps_{0};
        bool     upto_{false};
    };

    class Where {
    public:
        Expression *filter_{nullptr};
    };

    Status prepareFrom();

    Status prepareTo();

    void prepareVids(Expression *expr, Vertices &vertices);

    Status prepareOver();

    Status prepareStep();

    Status prepareWhere();

    void addGoFromFTask(std::vector<VertexID> &&fromVids,
                        folly::Promise<std::vector<VertexID>> &proF);

    void addGoFromTTask(std::vector<VertexID> &&toVids,
                        folly::Promise<std::vector<VertexID>> &proT);

    void findPath(std::vector<folly::Try<std::vector<VertexID>>> &&result);

    Status setupVids();

    Status setupVidsFromRef(Vertices &vertices);

    Status setupVidsFromExpr(std::vector<Expression*> &&vidList, Vertices &vertices);

    Status goFromF(std::vector<VertexID> vids,
                                  std::vector<storage::cpp2::PropDef> props,
                                  std::vector<VertexID> &frontiers);

    StatusOr<std::vector<VertexID>>
    doFilter(storage::StorageRpcResponse<storage::cpp2::QueryResponse> &&result,
         Expression *filter);

    Status goFromT(std::vector<VertexID> vids, std::vector<VertexID> &frontiers);

    StatusOr<std::vector<storage::cpp2::PropDef>> getStepOutProps(std::string reserve);

    StatusOr<std::vector<storage::cpp2::PropDef>> getDstProps();

private:
    FindPathSentence                           *sentence_{nullptr};
    std::unique_ptr<ExpressionContext>          expCtx_;
    GraphSpaceID                                spaceId_{INT_MIN};
    Vertices                                    from_;
    Vertices                                    to_;
    Over                                        over_;
    Step                                        step_;
    Where                                       where_;
    std::unique_ptr<InterimResult>              inputs_;
    std::unordered_set<VertexID>                visitedFrom_;
    std::unordered_set<VertexID>                visitedTo_;
    std::map<VertexID, std::string>             pathFrom_;
    std::map<VertexID, std::string>             pathTo_;
    std::vector<std::string>                    finalPath_;
    bool                                        stop_{false};
    using SchemaPropIndex = std::unordered_map<std::pair<std::string, std::string>, int64_t>;
    SchemaPropIndex                             srcTagProps_;
    SchemaPropIndex                             dstTagProps_;
};
}  // namespace graph
}  // namespace nebula
#endif

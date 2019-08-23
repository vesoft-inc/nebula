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
#include "common/concurrent/Barrier.h"

namespace nebula {
namespace graph {

using SchemaProps = std::unordered_map<std::string, std::vector<std::string>>;
const std::vector<std::string> kReserveProps_ = {"_dst", "_type", "_rank"};
using Neighbor = std::tuple<VertexID, EdgeType, EdgeRanking>;
using Neighbors = std::vector<Neighbor>;
using Frontiers =
        std::vector<
                    std::pair<
                              VertexID, /* start */
                              Neighbors /* frontiers of vertex*/
                             >
                   >;

enum class VisitedBy : char {
    FROM,
    TO,
};
class FindPathExecutor final : public TraverseExecutor {
public:
    FindPathExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "FindPathExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void feedResult(std::unique_ptr<InterimResult> result) override {
        inputs_ = std::move(result);
    }

private:
    struct Vertices {
        std::string             *colname_{nullptr};
        std::string             *varname_{nullptr};
        std::vector<VertexID>    vids_;
    };

    struct Over {
        std::string *edge_{nullptr};
        EdgeType     edgeType_{INT_MIN};
        bool         reversely_{false};
    };

    struct Step {
        uint32_t steps_{0};
        bool     upto_{false};
    };

    struct Where {
        Expression *filter_{nullptr};
    };


    Status prepareFrom();

    Status prepareTo();

    void prepareVids(Expression *expr, Vertices &vertices);

    Status prepareOver();

    Status prepareStep();

    Status prepareWhere();

    void getFromFrontiers(std::vector<storage::cpp2::PropDef> props);

    void getToFrontiers(std::vector<storage::cpp2::PropDef> props);

    void findPath();

    Status setupVids();

    Status setupVidsFromRef(Vertices &vertices);

    Status setupVidsFromExpr(std::vector<Expression*> &&vidList, Vertices &vertices);

    bool foundAllDest();

    Status doFilter(
            storage::StorageRpcResponse<storage::cpp2::QueryResponse> &&result,
            Expression *filter,
            bool isOutBound,
            Frontiers &frontiers);

    StatusOr<std::vector<storage::cpp2::PropDef>> getStepOutProps(bool reversely);

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
    bool                                        shortest_{false};
    std::unique_ptr<InterimResult>              inputs_;
    using SchemaPropIndex = std::unordered_map<std::pair<std::string, std::string>, int64_t>;
    SchemaPropIndex                             srcTagProps_;
    SchemaPropIndex                             dstTagProps_;
    concurrent::Barrier                         barrier_;
    Status                                      fStatus_;
    Status                                      tStatus_;
    // visited vertices
    std::unordered_set<VertexID>                visitedFrom_;
    std::unordered_set<VertexID>                visitedTo_;
    // next step starting vertices
    std::vector<VertexID>                       fromVids_;
    std::vector<VertexID>                       toVids_;
    // frontiers of vertices
    std::pair<VisitedBy, Frontiers>             fromFrontiers_;
    std::pair<VisitedBy, Frontiers>             toFrontiers_;
    using StepOut = std::tuple<VertexID, EdgeType, EdgeRanking>;
    using Path = std::vector<StepOut>;
    // interim path
    std::multimap<VertexID, Path>        pathFrom_;
    std::multimap<VertexID, Path>        pathTo_;
    // final path(shortest or all)
    std::vector<Path>                    finalPath_;
    bool                                        stop_{false};
};
}  // namespace graph
}  // namespace nebula
#endif

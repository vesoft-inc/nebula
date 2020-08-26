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
const std::vector<std::string> kReserveProps_ = {"_type", "_rank"};
using Neighbor = std::tuple<VertexID, EdgeType, EdgeRanking>; /* dst, type, rank*/
using Neighbors = std::vector<Neighbor>;
using Frontiers =
        std::vector<
                    std::pair<
                              VertexID, /* start */
                              Neighbors /* frontiers of vertex*/
                             >
                   >;

using StepOut = std::tuple<VertexID, EdgeType, EdgeRanking>; /* src, type, rank*/
using Path = std::list<StepOut*>;
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

    void setupResponse(cpp2::ExecutionResponse &resp) override;

    static std::string buildPathString(const Path &path);

    cpp2::RowValue buildPathRow(const Path &path);

private:
    Status prepareClauses();

    // Do some prepare work that can not do in prepare()
    Status beforeExecute();

    Status prepareOver();

    Status prepareOverAll();

    void getNeighborsAndFindPath();

    bool isFinalStep() {
        return currentStep_ == steps_;
    }

    void getFromFrontiers(std::vector<storage::cpp2::PropDef> props);

    void getToFrontiers(std::vector<storage::cpp2::PropDef> props);

    void findPath();

    inline void meetOddPath(VertexID src, VertexID dst, Neighbor &neighbor);

    inline void meetEvenPath(VertexID intersectId);

    inline void updatePath(
            VertexID &src,
            std::multimap<VertexID, Path> &pathToSrc,
            Neighbor &neighbor,
            std::multimap<VertexID, Path> &pathToNeighbor,
            VisitedBy visitedBy);

    Status setupVids();

    Status setupVidsFromRef(Clause::Vertices &vertices);

    Status doFilter(
            storage::StorageRpcResponse<storage::cpp2::QueryResponse> &&result,
            Expression *filter,
            bool isOutBound,
            Frontiers &frontiers);

    StatusOr<std::vector<storage::cpp2::PropDef>> getStepOutProps(bool reversely);

    StatusOr<std::vector<storage::cpp2::PropDef>> getDstProps();

private:
    FindPathSentence                               *sentence_{nullptr};
    std::unique_ptr<ExpressionContext>              expCtx_;
    GraphSpaceID                                    spaceId_{INT_MIN};
    Clause::Vertices                                from_;
    Clause::Vertices                                to_;
    Clause::Over                                    over_;
    Clause::Step                                    step_;
    Clause::Where                                   where_;
    bool                                            shortest_{false};
    using SchemaPropIndex = std::unordered_map<std::pair<std::string, std::string>, int64_t>;
    SchemaPropIndex                                 srcTagProps_;
    SchemaPropIndex                                 dstTagProps_;
    std::unordered_map<EdgeType, std::string>       edgeTypeNameMap_;
    std::unique_ptr<folly::Promise<folly::Unit>>    fPro_;
    std::unique_ptr<folly::Promise<folly::Unit>>    tPro_;
    Status                                          fStatus_;
    Status                                          tStatus_;
    std::unordered_set<VertexID>                    targetNotFound_;
    using StepOutHolder = std::unordered_set<std::unique_ptr<StepOut>>;
    StepOutHolder                                   stepOutHolder_;
    // next step starting vertices
    std::unordered_set<VertexID>                    visitedFrom_;
    std::unordered_set<VertexID>                    visitedTo_;
    // next step starting vertices
    std::vector<VertexID>                           fromVids_;
    std::vector<VertexID>                           toVids_;
    // frontiers of vertices
    std::pair<VisitedBy, Frontiers>                 fromFrontiers_;
    std::pair<VisitedBy, Frontiers>                 toFrontiers_;
    // interim path
    std::multimap<VertexID, Path>                   pathFrom_;
    std::multimap<VertexID, Path>                   pathTo_;
    // final path(shortest or all)
    std::multimap<VertexID, Path>                   finalPath_;
    uint64_t                                        currentStep_{1};
    uint64_t                                        steps_{0};
};
}  // namespace graph
}  // namespace nebula
#endif

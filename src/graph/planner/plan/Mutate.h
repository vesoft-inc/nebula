/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLAN_MUTATE_H_
#define PLANNER_PLAN_MUTATE_H_

#include "context/QueryContext.h"
#include "planner/plan/Query.h"
#include "parser/TraverseSentences.h"

/**
 * All mutate-related nodes would put in this file.
 */
namespace nebula {
namespace graph {
class InsertVertices final : public SingleDependencyNode {
public:
    static InsertVertices* make(QueryContext* qctx,
                                PlanNode* input,
                                GraphSpaceID spaceId,
                                std::vector<storage::cpp2::NewVertex> vertices,
                                std::unordered_map<TagID, std::vector<std::string>> tagPropNames,
                                bool ifNotExists) {
        return qctx->objPool()->add(new InsertVertices(qctx,
                                                       input,
                                                       spaceId,
                                                       std::move(vertices),
                                                       std::move(tagPropNames),
                                                       ifNotExists));
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

    const std::vector<storage::cpp2::NewVertex>& getVertices() const {
        return vertices_;
    }

    const std::unordered_map<TagID, std::vector<std::string>>& getPropNames() const {
        return tagPropNames_;
    }

    GraphSpaceID getSpace() const {
        return spaceId_;
    }

    bool getIfNotExists() const {
        return ifNotExists_;
    }

private:
    InsertVertices(QueryContext* qctx,
                   PlanNode* input,
                   GraphSpaceID spaceId,
                   std::vector<storage::cpp2::NewVertex> vertices,
                   std::unordered_map<TagID, std::vector<std::string>> tagPropNames,
                   bool ifNotExists)
        : SingleDependencyNode(qctx, Kind::kInsertVertices, input),
          spaceId_(spaceId),
          vertices_(std::move(vertices)),
          tagPropNames_(std::move(tagPropNames)),
          ifNotExists_(ifNotExists) {}

private:
    GraphSpaceID spaceId_{-1};
    std::vector<storage::cpp2::NewVertex> vertices_;
    std::unordered_map<TagID, std::vector<std::string>> tagPropNames_;
    bool ifNotExists_{false};
};

class InsertEdges final : public SingleDependencyNode {
public:
    static InsertEdges* make(QueryContext* qctx,
                             PlanNode* input,
                             GraphSpaceID spaceId,
                             std::vector<storage::cpp2::NewEdge> edges,
                             std::vector<std::string> propNames,
                             bool ifNotExists,
                             bool useChainInsert = false) {
        return qctx->objPool()->add(new InsertEdges(qctx,
                                                    input,
                                                    spaceId,
                                                    std::move(edges),
                                                    std::move(propNames),
                                                    ifNotExists,
                                                    useChainInsert));
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

    const std::vector<std::string>& getPropNames() const {
        return propNames_;
    }

    const std::vector<storage::cpp2::NewEdge>& getEdges() const {
        return edges_;
    }

    bool getIfNotExists() const {
        return ifNotExists_;
    }

    GraphSpaceID getSpace() const {
        return spaceId_;
    }

    bool useChainInsert() const {
        return useChainInsert_;
    }

private:
    InsertEdges(QueryContext* qctx,
                PlanNode* input,
                GraphSpaceID spaceId,
                std::vector<storage::cpp2::NewEdge> edges,
                std::vector<std::string> propNames,
                bool ifNotExists,
                bool useChainInsert)
        : SingleDependencyNode(qctx, Kind::kInsertEdges, input),
          spaceId_(spaceId),
          edges_(std::move(edges)),
          propNames_(std::move(propNames)),
          ifNotExists_(ifNotExists),
          useChainInsert_(useChainInsert) {}

private:
    GraphSpaceID spaceId_{-1};
    std::vector<storage::cpp2::NewEdge> edges_;
    std::vector<std::string> propNames_;
    bool ifNotExists_{false};
    // if this enabled, add edge request will only sent to
    // outbound edges. (toss)
    bool useChainInsert_{false};
};

class Update : public SingleDependencyNode {
public:
    bool getInsertable() const {
        return insertable_;
    }

    const std::vector<std::string>& getReturnProps() const {
        return returnProps_;
    }

    const std::string getCondition() const {
        return condition_;
    }

    const std::vector<std::string>& getYieldNames() const {
        return yieldNames_;
    }

    GraphSpaceID getSpaceId() const {
        return spaceId_;
    }

    const std::vector<storage::cpp2::UpdatedProp>& getUpdatedProps() const {
        return updatedProps_;
    }

    const std::string& getName() const {
        return schemaName_;
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

protected:
    Update(QueryContext* qctx,
           Kind kind,
           PlanNode* input,
           GraphSpaceID spaceId,
           std::string name,
           bool insertable,
           std::vector<storage::cpp2::UpdatedProp> updatedProps,
           std::vector<std::string> returnProps,
           std::string condition,
           std::vector<std::string> yieldNames)
        : SingleDependencyNode(qctx, kind, input),
          spaceId_(spaceId),
          schemaName_(std::move(name)),
          insertable_(insertable),
          updatedProps_(std::move(updatedProps)),
          returnProps_(std::move(returnProps)),
          condition_(std::move(condition)),
          yieldNames_(std::move(yieldNames)) {}

protected:
    GraphSpaceID spaceId_{-1};
    std::string schemaName_;
    bool insertable_;
    std::vector<storage::cpp2::UpdatedProp> updatedProps_;
    std::vector<std::string> returnProps_;
    std::string condition_;
    std::vector<std::string> yieldNames_;
};

class UpdateVertex final : public Update {
public:
    static UpdateVertex* make(QueryContext* qctx,
                              PlanNode* input,
                              GraphSpaceID spaceId,
                              std::string name,
                              Value vId,
                              TagID tagId,
                              bool insertable,
                              std::vector<storage::cpp2::UpdatedProp> updatedProps,
                              std::vector<std::string> returnProps,
                              std::string condition,
                              std::vector<std::string> yieldNames) {
        return qctx->objPool()->add(new UpdateVertex(qctx,
                                                     input,
                                                     spaceId,
                                                     std::move(name),
                                                     std::move(vId),
                                                     tagId,
                                                     insertable,
                                                     std::move(updatedProps),
                                                     std::move(returnProps),
                                                     std::move(condition),
                                                     std::move(yieldNames)));
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

    const Value& getVId() const {
        return vId_;
    }

    TagID getTagId() const {
        return tagId_;
    }

private:
    UpdateVertex(QueryContext* qctx,
                 PlanNode* input,
                 GraphSpaceID spaceId,
                 std::string name,
                 Value vId,
                 TagID tagId,
                 bool insertable,
                 std::vector<storage::cpp2::UpdatedProp> updatedProps,
                 std::vector<std::string> returnProps,
                 std::string condition,
                 std::vector<std::string> yieldNames)
        : Update(qctx,
                 Kind::kUpdateVertex,
                 input,
                 spaceId,
                 std::move(name),
                 insertable,
                 std::move(updatedProps),
                 std::move(returnProps),
                 std::move(condition),
                 std::move(yieldNames)),
          vId_(std::move(vId)),
          tagId_(tagId) {}

private:
    Value vId_;
    TagID tagId_{-1};
};

class UpdateEdge final : public Update {
public:
    static UpdateEdge* make(QueryContext* qctx,
                            PlanNode* input,
                            GraphSpaceID spaceId,
                            std::string name,
                            Value srcId,
                            Value dstId,
                            EdgeType edgeType,
                            int64_t rank,
                            bool insertable,
                            std::vector<storage::cpp2::UpdatedProp> updatedProps,
                            std::vector<std::string> returnProps,
                            std::string condition,
                            std::vector<std::string> yieldNames) {
        return qctx->objPool()->add(new UpdateEdge(qctx,
                                                   input,
                                                   spaceId,
                                                   std::move(name),
                                                   std::move(srcId),
                                                   std::move(dstId),
                                                   edgeType,
                                                   rank,
                                                   insertable,
                                                   std::move(updatedProps),
                                                   std::move(returnProps),
                                                   std::move(condition),
                                                   std::move(yieldNames)));
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

    const Value& getSrcId() const {
        return srcId_;
    }

    const Value& getDstId() const {
        return dstId_;
    }

    int64_t getRank() const {
        return rank_;
    }

    int64_t getEdgeType() const {
        return edgeType_;
    }

    const std::vector<storage::cpp2::UpdatedProp>& getUpdatedProps() const {
        return updatedProps_;
    }

private:
    UpdateEdge(QueryContext* qctx,
               PlanNode* input,
               GraphSpaceID spaceId,
               std::string name,
               Value srcId,
               Value dstId,
               EdgeType edgeType,
               int64_t rank,
               bool insertable,
               std::vector<storage::cpp2::UpdatedProp> updatedProps,
               std::vector<std::string> returnProps,
               std::string condition,
               std::vector<std::string> yieldNames)
        : Update(qctx,
                 Kind::kUpdateEdge,
                 input,
                 spaceId,
                 std::move(name),
                 insertable,
                 std::move(updatedProps),
                 std::move(returnProps),
                 std::move(condition),
                 std::move(yieldNames)),
          srcId_(std::move(srcId)),
          dstId_(std::move(dstId)),
          rank_(rank),
          edgeType_(edgeType) {}

private:
    Value srcId_;
    Value dstId_;
    int64_t rank_{0};
    EdgeType edgeType_{-1};
};

class DeleteVertices final : public SingleInputNode {
public:
    static DeleteVertices* make(QueryContext* qctx,
                                PlanNode* input,
                                GraphSpaceID spaceId,
                                Expression* vidRef_) {
        return qctx->objPool()->add(
            new DeleteVertices(qctx, input, spaceId, vidRef_));
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

    GraphSpaceID getSpace() const {
        return space_;
    }

    Expression* getVidRef() const {
        return vidRef_;
    }

private:
    DeleteVertices(QueryContext* qctx,
                   PlanNode* input,
                   GraphSpaceID spaceId,
                   Expression* vidRef)
        : SingleInputNode(qctx, Kind::kDeleteVertices, input),
          space_(spaceId),
          vidRef_(vidRef) {}

private:
    GraphSpaceID space_;
    Expression* vidRef_{nullptr};
};

class DeleteEdges final : public SingleInputNode {
public:
    static DeleteEdges* make(QueryContext* qctx,
                             PlanNode* input,
                             GraphSpaceID spaceId,
                             EdgeKeyRef *edgeKeyRef) {
        return qctx->objPool()->add(new DeleteEdges(qctx, input, spaceId, edgeKeyRef));
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

    GraphSpaceID getSpace() const {
        return space_;
    }

    EdgeKeyRef* edgeKeyRef() const {
        return edgeKeyRef_;
    }

private:
    DeleteEdges(QueryContext* qctx,
                PlanNode* input,
                GraphSpaceID spaceId,
                EdgeKeyRef *edgeKeyRef)
        : SingleInputNode(qctx, Kind::kDeleteEdges, input)
        , space_(spaceId)
        , edgeKeyRef_(edgeKeyRef) {}

private:
    GraphSpaceID space_{-1};
    EdgeKeyRef  *edgeKeyRef_{nullptr};
};

}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLAN_MUTATE_H_

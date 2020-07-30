/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MUTATE_H_
#define PLANNER_MUTATE_H_

#include "common/interface/gen-cpp2/storage_types.h"

#include "planner/Query.h"
#include "parser/TraverseSentences.h"

/**
 * All mutate-related nodes would put in this file.
 */
namespace nebula {
namespace graph {
class InsertVertices final : public SingleInputNode {
public:
    static InsertVertices* make(
            ExecutionPlan* plan,
            PlanNode* input,
            GraphSpaceID spaceId,
            std::vector<storage::cpp2::NewVertex> vertices,
            std::unordered_map<TagID, std::vector<std::string>> tagPropNames,
            bool overwritable) {
        return new InsertVertices(plan,
                                  input,
                                  spaceId,
                                  std::move(vertices),
                                  std::move(tagPropNames),
                                  overwritable);
    }

    std::string explain() const override {
        return "InsertVertices";
    }

    const std::vector<storage::cpp2::NewVertex>& getVertices() const {
        return vertices_;
    }

    const std::unordered_map<TagID, std::vector<std::string>>& getPropNames() const {
        return tagPropNames_;
    }

    bool getOverwritable() const {
        return overwritable_;
    }

    GraphSpaceID getSpace() const {
        return spaceId_;
    }

private:
    InsertVertices(ExecutionPlan* plan,
                   PlanNode* input,
                   GraphSpaceID spaceId,
                   std::vector<storage::cpp2::NewVertex> vertices,
                   std::unordered_map<TagID, std::vector<std::string>> tagPropNames,
                   bool overwritable)
        : SingleInputNode(plan, Kind::kInsertVertices, input)
        , spaceId_(spaceId)
        , vertices_(std::move(vertices))
        , tagPropNames_(std::move(tagPropNames))
        , overwritable_(overwritable) {
    }

private:
    GraphSpaceID                                               spaceId_{-1};
    std::vector<storage::cpp2::NewVertex>                      vertices_;
    std::unordered_map<TagID, std::vector<std::string>>        tagPropNames_;
    bool                                                       overwritable_;
};

class InsertEdges final : public SingleInputNode {
public:
    static InsertEdges* make(ExecutionPlan* plan,
                             PlanNode* input,
                             GraphSpaceID spaceId,
                             std::vector<storage::cpp2::NewEdge> edges,
                             std::vector<std::string> propNames,
                             bool overwritable) {
        return new InsertEdges(plan,
                               input,
                               spaceId,
                               std::move(edges),
                               std::move(propNames),
                               overwritable);
    }

    std::string explain() const override {
        return "InsertEdges";
    }

    const std::vector<std::string>& getPropNames() const {
        return propNames_;
    }

    const std::vector<storage::cpp2::NewEdge>& getEdges() const {
        return edges_;
    }

    bool getOverwritable() const {
        return overwritable_;
    }

    GraphSpaceID getSpace() const {
        return spaceId_;
    }

private:
    InsertEdges(ExecutionPlan* plan,
                PlanNode* input,
                GraphSpaceID spaceId,
                std::vector<storage::cpp2::NewEdge> edges,
                std::vector<std::string> propNames,
                bool overwritable)
        : SingleInputNode(plan, Kind::kInsertEdges, input)
        , spaceId_(spaceId)
        , edges_(std::move(edges))
        , propNames_(std::move(propNames))
        , overwritable_(overwritable) {
    }

private:
    GraphSpaceID                               spaceId_{-1};
    std::vector<storage::cpp2::NewEdge>        edges_;
    std::vector<std::string>                   propNames_;
    bool                                       overwritable_;
};

class UpdateVertex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "UpdateVertex";
    }
};

class UpdateEdge final : public SingleInputNode {
public:
    std::string explain() const override {
        return "UpdateEdge";
    }
};

class DeleteVertices final : public SingleInputNode {
public:
    static DeleteVertices* make(ExecutionPlan* plan,
                                PlanNode* input,
                                GraphSpaceID spaceId,
                                Expression* vidRef_) {
        return new DeleteVertices(plan,
                                  input,
                                  spaceId,
                                  vidRef_);
    }

    std::string explain() const override {
        return "DeleteVertices";
    }

    GraphSpaceID getSpace() const {
        return space_;
    }

    Expression* getVidRef() const {
        return vidRef_;
    }

private:
    DeleteVertices(ExecutionPlan* plan,
                   PlanNode* input,
                   GraphSpaceID spaceId,
                   Expression* vidRef)
        : SingleInputNode(plan, Kind::kDeleteVertices, input)
        , space_(spaceId)
        , vidRef_(vidRef) {}

private:
    GraphSpaceID                            space_;
    Expression                             *vidRef_{nullptr};
};

class DeleteEdges final : public SingleInputNode {
public:
    static DeleteEdges* make(ExecutionPlan* plan,
                             PlanNode* input,
                             GraphSpaceID spaceId,
                             std::vector<EdgeKeyRef*> edgeKeyRefs) {
        return new DeleteEdges(plan,
                               input,
                               spaceId,
                               std::move(edgeKeyRefs));
    }

    std::string explain() const override {
        return "DeleteEdges";
    }

    GraphSpaceID getSpace() const {
        return space_;
    }

    const std::vector<EdgeKeyRef*>& getEdgeKeyRefs() const {
        return edgeKeyRefs_;
    }

private:
    DeleteEdges(ExecutionPlan* plan,
                PlanNode* input,
                GraphSpaceID spaceId,
                std::vector<EdgeKeyRef*> edgeKeyRefs)
        : SingleInputNode(plan, Kind::kDeleteEdges, input)
        , space_(spaceId)
        , edgeKeyRefs_(std::move(edgeKeyRefs)) {}

private:
    GraphSpaceID                                   space_{-1};
    std::vector<EdgeKeyRef*>  edgeKeyRefs_;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MUTATE_H_

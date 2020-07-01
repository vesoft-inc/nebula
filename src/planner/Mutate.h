/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MUTATE_H_
#define PLANNER_MUTATE_H_

#include "common/interface/gen-cpp2/storage_types.h"
#include "planner/Query.h"

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

class DeleteVertex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "DeleteVertex";
    }
};

class DeleteEdge final : public SingleInputNode {
public:
    std::string explain() const override {
        return "DeleteEdge";
    }
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MUTATE_H_

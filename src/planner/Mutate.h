/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MUTATE_H_
#define PLANNER_MUTATE_H_

#include "interface/gen-cpp2/storage_types.h"
#include "PlanNode.h"

/**
 * All mutate-related nodes would put in this file.
 */
namespace nebula {
namespace graph {
// TODO: All DDLs, DMLs and DQLs could be used in a single query
// which would make them in a single and big execution plan
class InsertVertices final : public PlanNode {
public:
    static InsertVertices* make(ExecutionPlan* plan,
                                GraphSpaceID spaceId,
                                std::vector<storage::cpp2::NewVertex> vertices,
                                std::unordered_map<TagID, std::vector<std::string>> tagPropNames,
                                bool overwritable) {
        return new InsertVertices(plan,
                                  spaceId,
                                  std::move(vertices),
                                  std::move(tagPropNames),
                                  overwritable);
    }

    std::string explain() const override {
        return "InsertVertices";
    }

    GraphSpaceID space() const {
        return space_;
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

private:
    InsertVertices(ExecutionPlan* plan,
                   GraphSpaceID spaceId,
                   std::vector<storage::cpp2::NewVertex> vertices,
                   std::unordered_map<TagID, std::vector<std::string>> tagPropNames,
                   bool overwritable)
    : PlanNode(plan, Kind::kInsertVertices)
    , space_(spaceId)
    , vertices_(std::move(vertices))
    , tagPropNames_(std::move(tagPropNames))
    , overwritable_(overwritable) {}

private:
    GraphSpaceID                                               space_;
    std::vector<storage::cpp2::NewVertex>                      vertices_;
    std::unordered_map<TagID, std::vector<std::string>>        tagPropNames_;
    bool                                                       overwritable_;
};

class InsertEdges final : public PlanNode {
public:
    static InsertEdges* make(ExecutionPlan* plan,
                             GraphSpaceID spaceId,
                             std::vector<storage::cpp2::NewEdge> edges,
                             std::vector<std::string> propNames,
                             bool overwritable) {
        return new InsertEdges(plan,
                               spaceId,
                               std::move(edges),
                               std::move(propNames),
                               overwritable);
    }

    std::string explain() const override {
        return "InsertEdges";
    }

    GraphSpaceID space() const {
        return space_;
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

private:
    InsertEdges(ExecutionPlan* plan,
                GraphSpaceID spaceId,
                std::vector<storage::cpp2::NewEdge> edges,
                std::vector<std::string> propNames,
                bool overwritable)
    : PlanNode(plan, Kind::kInsertEdges)
    , space_(spaceId)
    , edges_(std::move(edges))
    , propNames_(std::move(propNames))
    , overwritable_(overwritable) {}

private:
    GraphSpaceID                               space_;
    std::vector<storage::cpp2::NewEdge>        edges_;
    std::vector<std::string>                   propNames_;
    bool                                       overwritable_;
};

class UpdateVertex final : public PlanNode {
public:
    std::string explain() const override {
        return "UpdateVertex";
    }
};

class UpdateEdge final : public PlanNode {
public:
    std::string explain() const override {
        return "UpdateEdge";
    }
};

class DeleteVertex final : public PlanNode {
public:
    std::string explain() const override {
        return "DeleteVertex";
    }
};

class DeleteEdge final : public PlanNode {
public:
    std::string explain() const override {
        return "DeleteEdge";
    }
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MUTATE_H_

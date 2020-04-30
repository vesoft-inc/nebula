/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MAINTAIN_H_
#define PLANNER_MAINTAIN_H_

#include "PlanNode.h"
#include "interface/gen-cpp2/meta_types.h"
#include "clients/meta/MetaClient.h"

namespace nebula {
namespace graph {
// TODO: All DDLs, DMLs and DQLs could be used in a single query
// which would make them in a single and big execution plan
class SchemaNode : public PlanNode {
public:
    GraphSpaceID space() const {
        return space_;
    }

protected:
    SchemaNode(ExecutionPlan* plan, Kind kind, const GraphSpaceID space)
        : PlanNode(plan, kind), space_(space) {}

protected:
    GraphSpaceID        space_;
};

class CreateSchemaNode : public SchemaNode {
protected:
    CreateSchemaNode(ExecutionPlan* plan,
                     Kind kind,
                     GraphSpaceID space,
                     std::string name,
                     meta::cpp2::Schema schema,
                     bool ifNotExists)
        : SchemaNode(plan, kind, space)
        , name_(std::move(name))
        , schema_(std::move(schema))
        , ifNotExists_(ifNotExists) {}

public:
    const std::string& getName() const {
        return name_;
    }

    const meta::cpp2::Schema& getSchema() const {
        return schema_;
    }

    bool getIfNotExists() const {
        return ifNotExists_;
    }

protected:
    std::string            name_;
    meta::cpp2::Schema     schema_;
    bool                   ifNotExists_;
};

class CreateTag final : public CreateSchemaNode {
public:
    static CreateTag* make(ExecutionPlan* plan,
                           GraphSpaceID space,
                           std::string tagName,
                           meta::cpp2::Schema schema,
                           bool ifNotExists) {
    return new CreateTag(plan,
                         space,
                         std::move(tagName),
                         std::move(schema),
                         ifNotExists);
    }

    std::string explain() const override {
        return "CreateTag";
    }

private:
    CreateTag(ExecutionPlan* plan,
              GraphSpaceID space,
              std::string tagName,
              meta::cpp2::Schema schema,
              bool ifNotExists)
        : CreateSchemaNode(plan,
                           Kind::kCreateTag,
                           space,
                           std::move(tagName),
                           std::move(schema),
                           ifNotExists) {
        }
};

class CreateEdge final : public CreateSchemaNode {
public:
    static CreateEdge* make(ExecutionPlan* plan,
                            GraphSpaceID space,
                            std::string edgeName,
                            meta::cpp2::Schema schema,
                            bool ifNotExists) {
    return new CreateEdge(plan,
                          space,
                          std::move(edgeName),
                          std::move(schema),
                          ifNotExists);
    }

    std::string explain() const override {
        return "CreateEdge";
    }

private:
    CreateEdge(ExecutionPlan* plan,
               GraphSpaceID space,
               std::string edgeName,
               meta::cpp2::Schema schema,
               bool ifNotExists)
        : CreateSchemaNode(plan,
                           Kind::kCreateEdge,
                           space,
                           std::move(edgeName),
                           std::move(schema),
                           ifNotExists) {
        }
};

class AlterTag final : public PlanNode {
};

class AlterEdge final : public PlanNode {
};

class DescSchema : public PlanNode {
protected:
    DescSchema(ExecutionPlan* plan,
               Kind kind,
               GraphSpaceID space,
               std::string name)
        : PlanNode(plan, kind)
        , space_(space)
        , name_(std::move(name)) {}

public:
    const std::string& getName() const {
        return name_;
    }

    GraphSpaceID getSpaceId() const {
        return space_;
    }

protected:
    GraphSpaceID           space_;
    std::string            name_;
};

class DescTag final : public DescSchema {
public:
    static DescTag* make(ExecutionPlan* plan,
                         GraphSpaceID space,
                         std::string tagName) {
    return new DescTag(plan, space, std::move(tagName));
    }

    std::string explain() const override {
        return "DescTag";
    }

private:
    DescTag(ExecutionPlan* plan,
            GraphSpaceID space,
            std::string tagName)
        : DescSchema(plan, Kind::kDescTag, space, std::move(tagName)) {
        }
};

class DescEdge final : public DescSchema {
public:
    static DescEdge* make(ExecutionPlan* plan,
                          GraphSpaceID space,
                          std::string edgeName) {
    return new DescEdge(plan, space, std::move(edgeName));
    }

    std::string explain() const override {
        return "DescEdge";
    }

private:
    DescEdge(ExecutionPlan* plan,
            GraphSpaceID space,
            std::string edgeName)
        : DescSchema(plan, Kind::kDescEdge, space, std::move(edgeName)) {
        }
};

class DropTag final : public PlanNode {
};

class DropEdge final : public PlanNode {
};

class CreateTagIndex final : public PlanNode {
};

class CreateEdgeIndex final : public PlanNode {
};

class DescribeTagIndex final : public PlanNode {
};

class DescribeEdgeIndex final : public PlanNode {
};

class DropTagIndex final : public PlanNode {
};

class DropEdgeIndex final : public PlanNode {
};

class BuildTagIndex final : public PlanNode {
};

class BuildEdgeIndex final : public PlanNode {
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MAINTAIN_H_

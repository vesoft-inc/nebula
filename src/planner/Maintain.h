/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MAINTAIN_H_
#define PLANNER_MAINTAIN_H_

#include "planner/Query.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/clients/meta/MetaClient.h"

namespace nebula {
namespace graph {
// which would make them in a single and big execution plan
class CreateSchemaNode : public SingleInputNode {
protected:
    CreateSchemaNode(ExecutionPlan* plan,
                     PlanNode* input,
                     Kind kind,
                     std::string name,
                     meta::cpp2::Schema schema,
                     bool ifNotExists)
        : SingleInputNode(plan, kind, input)
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
                           PlanNode* input,
                           std::string tagName,
                           meta::cpp2::Schema schema,
                           bool ifNotExists) {
    return new CreateTag(plan,
                         input,
                         std::move(tagName),
                         std::move(schema),
                         ifNotExists);
    }

    std::string explain() const override {
        return "CreateTag";
    }

private:
    CreateTag(ExecutionPlan* plan,
              PlanNode* input,
              std::string tagName,
              meta::cpp2::Schema schema,
              bool ifNotExists)
        : CreateSchemaNode(plan,
                           input,
                           Kind::kCreateTag,
                           std::move(tagName),
                           std::move(schema),
                           ifNotExists) {
        }
};

class CreateEdge final : public CreateSchemaNode {
public:
    static CreateEdge* make(ExecutionPlan* plan,
                            PlanNode* input,
                            std::string edgeName,
                            meta::cpp2::Schema schema,
                            bool ifNotExists) {
    return new CreateEdge(plan,
                          input,
                          std::move(edgeName),
                          std::move(schema),
                          ifNotExists);
    }

    std::string explain() const override {
        return "CreateEdge";
    }

private:
    CreateEdge(ExecutionPlan* plan,
               PlanNode* input,
               std::string edgeName,
               meta::cpp2::Schema schema,
               bool ifNotExists)
        : CreateSchemaNode(plan,
                           input,
                           Kind::kCreateEdge,
                           std::move(edgeName),
                           std::move(schema),
                           ifNotExists) {
        }
};

class AlterSchemaNode : public SingleInputNode {
protected:
    AlterSchemaNode(ExecutionPlan* plan,
                    Kind kind,
                    PlanNode* input,
                    GraphSpaceID space,
                    std::string name,
                    std::vector<meta::cpp2::AlterSchemaItem> items,
                    meta::cpp2::SchemaProp schemaProp)
        : SingleInputNode(plan, kind, input)
        , space_(space)
        , name_(std::move(name))
        , schemaItems_(std::move(items))
        , schemaProp_(std::move(schemaProp)) {}

public:
    const std::string& getName() const {
        return name_;
    }

    const std::vector<meta::cpp2::AlterSchemaItem>& getSchemaItems() const {
        return schemaItems_;
    }

    const meta::cpp2::SchemaProp& getSchemaProp() const {
        return schemaProp_;
    }

    GraphSpaceID space() const {
        return space_;
    }

protected:
    GraphSpaceID                               space_;
    std::string                                name_;
    std::vector<meta::cpp2::AlterSchemaItem>   schemaItems_;
    meta::cpp2::SchemaProp                     schemaProp_;
};

class AlterTag final : public AlterSchemaNode {
public:
    static AlterTag* make(ExecutionPlan* plan,
                          PlanNode* input,
                          GraphSpaceID space,
                          std::string name,
                          std::vector<meta::cpp2::AlterSchemaItem> items,
                          meta::cpp2::SchemaProp schemaProp) {
        return new AlterTag(plan,
                            input,
                            space,
                            std::move(name),
                            std::move(items),
                            std::move(schemaProp));
    }

    std::string explain() const override {
        return "AlterTag";
    }

private:
    AlterTag(ExecutionPlan* plan,
             PlanNode* input,
             GraphSpaceID space,
             std::string name,
             std::vector<meta::cpp2::AlterSchemaItem> items,
             meta::cpp2::SchemaProp schemaProp)
        : AlterSchemaNode(plan,
                            Kind::kAlterTag,
                            input,
                            space,
                            std::move(name),
                            std::move(items),
                            std::move(schemaProp)) {
    }
};

class AlterEdge final : public AlterSchemaNode {
public:
    static AlterEdge* make(ExecutionPlan* plan,
                           PlanNode* input,
                           GraphSpaceID space,
                           std::string name,
                           std::vector<meta::cpp2::AlterSchemaItem> items,
                           meta::cpp2::SchemaProp schemaProp) {
        return new AlterEdge(plan,
                             input,
                             space,
                             std::move(name),
                             std::move(items),
                             std::move(schemaProp));
    }

    std::string explain() const override {
        return "AlterEdge";
    }

private:
    AlterEdge(ExecutionPlan* plan,
              PlanNode* input,
              GraphSpaceID space,
              std::string name,
              std::vector<meta::cpp2::AlterSchemaItem> items,
              meta::cpp2::SchemaProp schemaProp)
        : AlterSchemaNode(plan,
                            Kind::kAlterEdge,
                            input,
                            space,
                            std::move(name),
                            std::move(items),
                            std::move(schemaProp)) {
    }
};

class DescSchema : public SingleInputNode {
protected:
    DescSchema(ExecutionPlan* plan,
               PlanNode* input,
               Kind kind,
               std::string name)
        : SingleInputNode(plan, kind, input)
        , name_(std::move(name)) {
    }

public:
    const std::string& getName() const {
        return name_;
    }

protected:
    std::string            name_;
};

class DescTag final : public DescSchema {
public:
    static DescTag* make(ExecutionPlan* plan,
                         PlanNode* input,
                         std::string tagName) {
        return new DescTag(plan, input, std::move(tagName));
    }

    std::string explain() const override {
        return "DescTag";
    }

private:
    DescTag(ExecutionPlan* plan,
            PlanNode* input,
            std::string tagName)
        : DescSchema(plan, input, Kind::kDescTag, std::move(tagName)) {
    }
};

class DescEdge final : public DescSchema {
public:
    static DescEdge* make(ExecutionPlan* plan,
                          PlanNode* input,
                          std::string edgeName) {
        return new DescEdge(plan, input, std::move(edgeName));
    }

    std::string explain() const override {
        return "DescEdge";
    }

private:
    DescEdge(ExecutionPlan* plan,
             PlanNode* input,
             std::string edgeName)
        : DescSchema(plan, input, Kind::kDescEdge, std::move(edgeName)) {
    }
};

class DropTag final : public SingleInputNode {
public:
    std::string explain() const override {
        return "DropTag";
    }
};

class DropEdge final : public SingleInputNode {
public:
    std::string explain() const override {
        return "DropEdge";
    }
};

class CreateTagIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "CreateTagIndex";
    }
};

class CreateEdgeIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "CreateEdgeIndex";
    }
};

class DescribeTagIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "DescribeTagIndex";
    }
};

class DescribeEdgeIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "DescribeEdgeIndex";
    }
};

class DropTagIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "DropTagIndex";
    }
};

class DropEdgeIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "DropEdgeIndex";
    }
};

class BuildTagIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "BuildTagIndex";
    }
};

class BuildEdgeIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "BuildEdgeIndex";
    }
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MAINTAIN_H_

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
    CreateSchemaNode(int64_t id,
                     PlanNode* input,
                     Kind kind,
                     std::string name,
                     meta::cpp2::Schema schema,
                     bool ifNotExists)
        : SingleInputNode(id, kind, input)
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

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    std::string            name_;
    meta::cpp2::Schema     schema_;
    bool                   ifNotExists_;
};

class CreateTag final : public CreateSchemaNode {
public:
    static CreateTag* make(QueryContext* qctx,
                           PlanNode* input,
                           std::string tagName,
                           meta::cpp2::Schema schema,
                           bool ifNotExists) {
        return qctx->objPool()->add(new CreateTag(
            qctx->genId(), input, std::move(tagName), std::move(schema), ifNotExists));
    }

private:
    CreateTag(int64_t id,
              PlanNode* input,
              std::string tagName,
              meta::cpp2::Schema schema,
              bool ifNotExists)
        : CreateSchemaNode(id,
                           input,
                           Kind::kCreateTag,
                           std::move(tagName),
                           std::move(schema),
                           ifNotExists) {
        }
};

class CreateEdge final : public CreateSchemaNode {
public:
    static CreateEdge* make(QueryContext* qctx,
                            PlanNode* input,
                            std::string edgeName,
                            meta::cpp2::Schema schema,
                            bool ifNotExists) {
        return qctx->objPool()->add(new CreateEdge(
            qctx->genId(), input, std::move(edgeName), std::move(schema), ifNotExists));
    }

private:
    CreateEdge(int64_t id,
               PlanNode* input,
               std::string edgeName,
               meta::cpp2::Schema schema,
               bool ifNotExists)
        : CreateSchemaNode(id,
                           input,
                           Kind::kCreateEdge,
                           std::move(edgeName),
                           std::move(schema),
                           ifNotExists) {
        }
};

class AlterSchemaNode : public SingleInputNode {
protected:
    AlterSchemaNode(int64_t id,
                    Kind kind,
                    PlanNode* input,
                    GraphSpaceID space,
                    std::string name,
                    std::vector<meta::cpp2::AlterSchemaItem> items,
                    meta::cpp2::SchemaProp schemaProp)
        : SingleInputNode(id, kind, input)
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

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    GraphSpaceID                               space_;
    std::string                                name_;
    std::vector<meta::cpp2::AlterSchemaItem>   schemaItems_;
    meta::cpp2::SchemaProp                     schemaProp_;
};

class AlterTag final : public AlterSchemaNode {
public:
    static AlterTag* make(QueryContext* qctx,
                          PlanNode* input,
                          GraphSpaceID space,
                          std::string name,
                          std::vector<meta::cpp2::AlterSchemaItem> items,
                          meta::cpp2::SchemaProp schemaProp) {
        return qctx->objPool()->add(new AlterTag(
            qctx->genId(), input, space, std::move(name), std::move(items), std::move(schemaProp)));
    }

private:
    AlterTag(int64_t id,
             PlanNode* input,
             GraphSpaceID space,
             std::string name,
             std::vector<meta::cpp2::AlterSchemaItem> items,
             meta::cpp2::SchemaProp schemaProp)
        : AlterSchemaNode(id,
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
    static AlterEdge* make(QueryContext* qctx,
                           PlanNode* input,
                           GraphSpaceID space,
                           std::string name,
                           std::vector<meta::cpp2::AlterSchemaItem> items,
                           meta::cpp2::SchemaProp schemaProp) {
        return qctx->objPool()->add(new AlterEdge(
            qctx->genId(), input, space, std::move(name), std::move(items), std::move(schemaProp)));
    }

private:
    AlterEdge(int64_t id,
              PlanNode* input,
              GraphSpaceID space,
              std::string name,
              std::vector<meta::cpp2::AlterSchemaItem> items,
              meta::cpp2::SchemaProp schemaProp)
        : AlterSchemaNode(id,
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
    DescSchema(int64_t id,
               PlanNode* input,
               Kind kind,
               std::string name)
        : SingleInputNode(id, kind, input)
        , name_(std::move(name)) {
    }

public:
    const std::string& getName() const {
        return name_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    std::string            name_;
};

class DescTag final : public DescSchema {
public:
    static DescTag* make(QueryContext* qctx,
                         PlanNode* input,
                         std::string tagName) {
        return qctx->objPool()->add(new DescTag(qctx->genId(), input, std::move(tagName)));
    }

private:
    DescTag(int64_t id,
            PlanNode* input,
            std::string tagName)
        : DescSchema(id, input, Kind::kDescTag, std::move(tagName)) {
    }
};

class DescEdge final : public DescSchema {
public:
    static DescEdge* make(QueryContext* qctx,
                          PlanNode* input,
                          std::string edgeName) {
        return qctx->objPool()->add(new DescEdge(qctx->genId(), input, std::move(edgeName)));
    }

private:
    DescEdge(int64_t id,
             PlanNode* input,
             std::string edgeName)
        : DescSchema(id, input, Kind::kDescEdge, std::move(edgeName)) {
    }
};

class ShowCreateTag final : public DescSchema {
public:
    static ShowCreateTag* make(QueryContext* qctx,
                               PlanNode* input,
                               std::string name) {
        return qctx->objPool()->add(new ShowCreateTag(qctx->genId(), input, std::move(name)));
    }

private:
    ShowCreateTag(int64_t id,
                  PlanNode* input,
                  std::string name)
        : DescSchema(id, input, Kind::kShowCreateTag, std::move(name)) {
    }
};

class ShowCreateEdge final : public DescSchema {
public:
    static ShowCreateEdge* make(QueryContext* qctx,
                                PlanNode* input,
                                std::string name) {
        return qctx->objPool()->add(new ShowCreateEdge(qctx->genId(), input, std::move(name)));
    }

private:
    ShowCreateEdge(int64_t id,
                   PlanNode* input,
                   std::string name)
        : DescSchema(id, input, Kind::kShowCreateEdge, std::move(name)) {
    }
};

class ShowTags final : public SingleInputNode {
public:
    static ShowTags* make(QueryContext* qctx,
                          PlanNode* input) {
        return qctx->objPool()->add(new ShowTags(qctx->genId(), input));
    }

private:
    ShowTags(int64_t id,
             PlanNode* input)
        : SingleInputNode(id, Kind::kShowTags, input) {
    }
};

class ShowEdges final : public SingleInputNode {
public:
    static ShowEdges* make(QueryContext* qctx,
                           PlanNode* input) {
        return qctx->objPool()->add(new ShowEdges(qctx->genId(), input));
    }

private:
    ShowEdges(int64_t id,
              PlanNode* input)
        : SingleInputNode(id, Kind::kShowEdges, input) {
    }
};

class DropSchema : public SingleInputNode {
protected:
    DropSchema(int64_t id,
               Kind kind,
               PlanNode* input,
               std::string name,
               bool ifExists)
        : SingleInputNode(id, kind, input)
        , name_(std::move(name))
        , ifExists_(ifExists) {}

public:
    const std::string& getName() const {
        return name_;
    }

    GraphSpaceID getIfExists() const {
        return ifExists_;
    }


    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    std::string            name_;
    bool                   ifExists_;
};

class DropTag final : public DropSchema {
public:
    static DropTag* make(QueryContext* qctx,
                         PlanNode* input,
                         std::string name,
                         bool ifExists) {
        return qctx->objPool()->add(new DropTag(qctx->genId(), input, std::move(name), ifExists));
    }

private:
    DropTag(int64_t id,
            PlanNode* input,
            std::string name,
            bool ifExists)
        : DropSchema(id, Kind::kDropTag, input, std::move(name), ifExists) {
    }
};

class DropEdge final : public DropSchema {
public:
    static DropEdge* make(QueryContext* qctx,
                          PlanNode* input,
                          std::string name,
                          bool ifExists) {
        return qctx->objPool()->add(new DropEdge(qctx->genId(), input, std::move(name), ifExists));
    }

private:
    DropEdge(int64_t id,
             PlanNode* input,
             std::string name,
             bool ifExists)
        : DropSchema(id, Kind::kDropEdge, input, std::move(name), ifExists) {
    }
};

class CreateTagIndex final : public SingleInputNode {
public:
};

class CreateEdgeIndex final : public SingleInputNode {
public:
};

class DescribeTagIndex final : public SingleInputNode {
public:
};

class DescribeEdgeIndex final : public SingleInputNode {
public:
};

class DropTagIndex final : public SingleInputNode {
public:
};

class DropEdgeIndex final : public SingleInputNode {
public:
};

class BuildTagIndex final : public SingleInputNode {
public:
};

class BuildEdgeIndex final : public SingleInputNode {
public:
};

}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MAINTAIN_H_

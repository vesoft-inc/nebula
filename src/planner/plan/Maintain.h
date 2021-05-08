/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLAN_MAINTAIN_H_
#define PLANNER_PLAN_MAINTAIN_H_

#include "common/interface/gen-cpp2/meta_types.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

// which would make them in a single and big execution plan
class CreateSchemaNode : public SingleInputNode {
protected:
    CreateSchemaNode(QueryContext* qctx,
                     PlanNode* input,
                     Kind kind,
                     std::string name,
                     meta::cpp2::Schema schema,
                     bool ifNotExists)
        : SingleInputNode(qctx, kind, input),
          name_(std::move(name)),
          schema_(std::move(schema)),
          ifNotExists_(ifNotExists) {}

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

    std::unique_ptr<PlanNodeDescription> explain() const override;

protected:
    std::string name_;
    meta::cpp2::Schema schema_;
    bool ifNotExists_;
};

class CreateTag final : public CreateSchemaNode {
public:
    static CreateTag* make(QueryContext* qctx,
                           PlanNode* input,
                           std::string tagName,
                           meta::cpp2::Schema schema,
                           bool ifNotExists) {
        return qctx->objPool()->add(
            new CreateTag(qctx, input, std::move(tagName), std::move(schema), ifNotExists));
    }

private:
    CreateTag(QueryContext* qctx,
              PlanNode* input,
              std::string tagName,
              meta::cpp2::Schema schema,
              bool ifNotExists)
        : CreateSchemaNode(qctx,
                           input,
                           Kind::kCreateTag,
                           std::move(tagName),
                           std::move(schema),
                           ifNotExists) {}
};

class CreateEdge final : public CreateSchemaNode {
public:
    static CreateEdge* make(QueryContext* qctx,
                            PlanNode* input,
                            std::string edgeName,
                            meta::cpp2::Schema schema,
                            bool ifNotExists) {
        return qctx->objPool()->add(
            new CreateEdge(qctx, input, std::move(edgeName), std::move(schema), ifNotExists));
    }

private:
    CreateEdge(QueryContext* qctx,
               PlanNode* input,
               std::string edgeName,
               meta::cpp2::Schema schema,
               bool ifNotExists)
        : CreateSchemaNode(qctx,
                           input,
                           Kind::kCreateEdge,
                           std::move(edgeName),
                           std::move(schema),
                           ifNotExists) {}
};

class AlterSchemaNode : public SingleInputNode {
protected:
    AlterSchemaNode(QueryContext* qctx,
                    Kind kind,
                    PlanNode* input,
                    GraphSpaceID space,
                    std::string name,
                    std::vector<meta::cpp2::AlterSchemaItem> items,
                    meta::cpp2::SchemaProp schemaProp)
        : SingleInputNode(qctx, kind, input),
          space_(space),
          name_(std::move(name)),
          schemaItems_(std::move(items)),
          schemaProp_(std::move(schemaProp)) {}

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

    std::unique_ptr<PlanNodeDescription> explain() const override;

protected:
    GraphSpaceID space_;
    std::string name_;
    std::vector<meta::cpp2::AlterSchemaItem> schemaItems_;
    meta::cpp2::SchemaProp schemaProp_;
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
            qctx, input, space, std::move(name), std::move(items), std::move(schemaProp)));
    }

private:
    AlterTag(QueryContext* qctx,
             PlanNode* input,
             GraphSpaceID space,
             std::string name,
             std::vector<meta::cpp2::AlterSchemaItem> items,
             meta::cpp2::SchemaProp schemaProp)
        : AlterSchemaNode(qctx,
                          Kind::kAlterTag,
                          input,
                          space,
                          std::move(name),
                          std::move(items),
                          std::move(schemaProp)) {}
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
            qctx, input, space, std::move(name), std::move(items), std::move(schemaProp)));
    }

private:
    AlterEdge(QueryContext* qctx,
              PlanNode* input,
              GraphSpaceID space,
              std::string name,
              std::vector<meta::cpp2::AlterSchemaItem> items,
              meta::cpp2::SchemaProp schemaProp)
        : AlterSchemaNode(qctx,
                          Kind::kAlterEdge,
                          input,
                          space,
                          std::move(name),
                          std::move(items),
                          std::move(schemaProp)) {}
};

class DescSchemaNode : public SingleInputNode {
protected:
    DescSchemaNode(QueryContext* qctx, PlanNode* input, Kind kind, std::string name)
        : SingleInputNode(qctx, kind, input), name_(std::move(name)) {}

public:
    const std::string& getName() const {
        return name_;
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

protected:
    std::string name_;
};

class DescTag final : public DescSchemaNode {
public:
    static DescTag* make(QueryContext* qctx, PlanNode* input, std::string tagName) {
        return qctx->objPool()->add(new DescTag(qctx, input, std::move(tagName)));
    }

private:
    DescTag(QueryContext* qctx, PlanNode* input, std::string tagName)
        : DescSchemaNode(qctx, input, Kind::kDescTag, std::move(tagName)) {}
};

class DescEdge final : public DescSchemaNode {
public:
    static DescEdge* make(QueryContext* qctx, PlanNode* input, std::string edgeName) {
        return qctx->objPool()->add(new DescEdge(qctx, input, std::move(edgeName)));
    }

private:
    DescEdge(QueryContext* qctx, PlanNode* input, std::string edgeName)
        : DescSchemaNode(qctx, input, Kind::kDescEdge, std::move(edgeName)) {}
};

class ShowCreateTag final : public DescSchemaNode {
public:
    static ShowCreateTag* make(QueryContext* qctx, PlanNode* input, std::string name) {
        return qctx->objPool()->add(new ShowCreateTag(qctx, input, std::move(name)));
    }

private:
    ShowCreateTag(QueryContext* qctx, PlanNode* input, std::string name)
        : DescSchemaNode(qctx, input, Kind::kShowCreateTag, std::move(name)) {}
};

class ShowCreateEdge final : public DescSchemaNode {
public:
    static ShowCreateEdge* make(QueryContext* qctx, PlanNode* input, std::string name) {
        return qctx->objPool()->add(new ShowCreateEdge(qctx, input, std::move(name)));
    }

private:
    ShowCreateEdge(QueryContext* qctx, PlanNode* input, std::string name)
        : DescSchemaNode(qctx, input, Kind::kShowCreateEdge, std::move(name)) {}
};

class ShowTags final : public SingleInputNode {
public:
    static ShowTags* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowTags(qctx, input));
    }

private:
    ShowTags(QueryContext* qctx, PlanNode* input) : SingleInputNode(qctx, Kind::kShowTags, input) {}
};

class ShowEdges final : public SingleInputNode {
public:
    static ShowEdges* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowEdges(qctx, input));
    }

private:
    ShowEdges(QueryContext* qctx, PlanNode* input)
        : SingleInputNode(qctx, Kind::kShowEdges, input) {}
};

class DropSchemaNode : public SingleInputNode {
protected:
    DropSchemaNode(QueryContext* qctx, Kind kind, PlanNode* input, std::string name, bool ifExists)
        : SingleInputNode(qctx, kind, input), name_(std::move(name)), ifExists_(ifExists) {}

public:
    const std::string& getName() const {
        return name_;
    }

    GraphSpaceID getIfExists() const {
        return ifExists_;
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

protected:
    std::string name_;
    bool ifExists_;
};

class DropTag final : public DropSchemaNode {
public:
    static DropTag* make(QueryContext* qctx, PlanNode* input, std::string name, bool ifExists) {
        return qctx->objPool()->add(new DropTag(qctx, input, std::move(name), ifExists));
    }

private:
    DropTag(QueryContext* qctx, PlanNode* input, std::string name, bool ifExists)
        : DropSchemaNode(qctx, Kind::kDropTag, input, std::move(name), ifExists) {}
};

class DropEdge final : public DropSchemaNode {
public:
    static DropEdge* make(QueryContext* qctx, PlanNode* input, std::string name, bool ifExists) {
        return qctx->objPool()->add(new DropEdge(qctx, input, std::move(name), ifExists));
    }

private:
    DropEdge(QueryContext* qctx, PlanNode* input, std::string name, bool ifExists)
        : DropSchemaNode(qctx, Kind::kDropEdge, input, std::move(name), ifExists) {}
};

class CreateIndexNode : public SingleInputNode {
protected:
    CreateIndexNode(QueryContext* qctx,
                    PlanNode* input,
                    Kind kind,
                    std::string schemaName,
                    std::string indexName,
                    std::vector<meta::cpp2::IndexFieldDef> fields,
                    bool ifNotExists,
                    const std::string *comment)
        : SingleInputNode(qctx, kind, input),
          schemaName_(std::move(schemaName)),
          indexName_(std::move(indexName)),
          fields_(std::move(fields)),
          ifNotExists_(ifNotExists),
          comment_(comment) {}

public:
    const std::string& getSchemaName() const {
        return schemaName_;
    }

    const std::string& getIndexName() const {
        return indexName_;
    }

    const std::vector<meta::cpp2::IndexFieldDef>& getFields() const {
        return fields_;
    }

    bool getIfNotExists() const {
        return ifNotExists_;
    }

    const std::string* getComment() const {
        return comment_;
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

protected:
    std::string                             schemaName_;
    std::string                             indexName_;
    std::vector<meta::cpp2::IndexFieldDef>  fields_;
    bool                                    ifNotExists_;
    const std::string*                            comment_;
};

class CreateTagIndex final : public CreateIndexNode {
public:
    static CreateTagIndex* make(QueryContext* qctx,
                                PlanNode* input,
                                std::string tagName,
                                std::string indexName,
                                std::vector<meta::cpp2::IndexFieldDef> fields,
                                bool ifNotExists,
                                const std::string *comment) {
        return qctx->objPool()->add(new CreateTagIndex(qctx,
                                                       input,
                                                       std::move(tagName),
                                                       std::move(indexName),
                                                       std::move(fields),
                                                       ifNotExists,
                                                       comment));
    }

private:
    CreateTagIndex(QueryContext* qctx,
                   PlanNode* input,
                   std::string tagName,
                   std::string indexName,
                   std::vector<meta::cpp2::IndexFieldDef> fields,
                   bool ifNotExists,
                   const std::string *comment)
        : CreateIndexNode(qctx,
                          input,
                          Kind::kCreateTagIndex,
                          std::move(tagName),
                          std::move(indexName),
                          std::move(fields),
                          ifNotExists,
                          comment) {}
};

class CreateEdgeIndex final : public CreateIndexNode {
public:
    static CreateEdgeIndex* make(QueryContext* qctx,
                                 PlanNode* input,
                                 std::string edgeName,
                                 std::string indexName,
                                 std::vector<meta::cpp2::IndexFieldDef> fields,
                                 bool ifNotExists,
                                 const std::string *comment) {
        return qctx->objPool()->add(new CreateEdgeIndex(qctx,
                                                        input,
                                                        std::move(edgeName),
                                                        std::move(indexName),
                                                        std::move(fields),
                                                        ifNotExists,
                                                        comment));
    }

private:
    CreateEdgeIndex(QueryContext* qctx,
                    PlanNode* input,
                    std::string edgeName,
                    std::string indexName,
                    std::vector<meta::cpp2::IndexFieldDef> fields,
                    bool ifNotExists,
                    const std::string *comment)
        : CreateIndexNode(qctx,
                          input,
                          Kind::kCreateEdgeIndex,
                          std::move(edgeName),
                          std::move(indexName),
                          std::move(fields),
                          ifNotExists,
                          comment) {}
};

class DescIndexNode : public SingleInputNode {
protected:
    DescIndexNode(QueryContext* qctx, PlanNode* input, Kind kind, std::string indexName)
        : SingleInputNode(qctx, kind, input), indexName_(std::move(indexName)) {}

public:
    const std::string& getIndexName() const {
        return indexName_;
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

protected:
    std::string indexName_;
};

class DescTagIndex final : public DescIndexNode {
public:
    static DescTagIndex* make(QueryContext* qctx, PlanNode* input, std::string indexName) {
        return qctx->objPool()->add(new DescTagIndex(qctx, input, std::move(indexName)));
    }

private:
    DescTagIndex(QueryContext* qctx, PlanNode* input, std::string indexName)
        : DescIndexNode(qctx, input, Kind::kDescTagIndex, std::move(indexName)) {}
};

class DescEdgeIndex final : public DescIndexNode {
public:
    static DescEdgeIndex* make(QueryContext* qctx, PlanNode* input, std::string indexName) {
        return qctx->objPool()->add(new DescEdgeIndex(qctx, input, std::move(indexName)));
    }

private:
    DescEdgeIndex(QueryContext* qctx, PlanNode* input, std::string indexName)
        : DescIndexNode(qctx, input, Kind::kDescEdgeIndex, std::move(indexName)) {}
};

class DropIndexNode : public SingleInputNode {
protected:
    DropIndexNode(QueryContext* qctx,
                  Kind kind,
                  PlanNode* input,
                  std::string indexName,
                  bool ifExists)
        : SingleInputNode(qctx, kind, input),
          indexName_(std::move(indexName)),
          ifExists_(ifExists) {}

public:
    const std::string& getIndexName() const {
        return indexName_;
    }

    GraphSpaceID getIfExists() const {
        return ifExists_;
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

protected:
    std::string indexName_;
    bool ifExists_;
};

class DropTagIndex final : public DropIndexNode {
public:
    static DropTagIndex* make(QueryContext* qctx,
                              PlanNode* input,
                              std::string indexName,
                              bool ifExists) {
        return qctx->objPool()->add(new DropTagIndex(qctx, input, std::move(indexName), ifExists));
    }

private:
    DropTagIndex(QueryContext* qctx, PlanNode* input, std::string indexName, bool ifExists)
        : DropIndexNode(qctx, Kind::kDropTagIndex, input, std::move(indexName), ifExists) {}
};

class DropEdgeIndex final : public DropIndexNode {
public:
    static DropEdgeIndex* make(QueryContext* qctx,
                               PlanNode* input,
                               std::string indexName,
                               bool ifExists) {
        return qctx->objPool()->add(new DropEdgeIndex(qctx, input, std::move(indexName), ifExists));
    }

private:
    DropEdgeIndex(QueryContext* qctx, PlanNode* input, std::string indexName, bool ifExists)
        : DropIndexNode(qctx, Kind::kDropEdgeIndex, input, std::move(indexName), ifExists) {}
};

class ShowCreateTagIndex final : public DescIndexNode {
public:
    static ShowCreateTagIndex* make(QueryContext* qctx, PlanNode* input, std::string indexName) {
        return qctx->objPool()->add(new ShowCreateTagIndex(qctx, input, std::move(indexName)));
    }

private:
    ShowCreateTagIndex(QueryContext* qctx, PlanNode* input, std::string indexName)
        : DescIndexNode(qctx, input, Kind::kShowCreateTagIndex, std::move(indexName)) {}
};

class ShowCreateEdgeIndex final : public DescIndexNode {
public:
    static ShowCreateEdgeIndex* make(QueryContext* qctx, PlanNode* input, std::string indexName) {
        return qctx->objPool()->add(new ShowCreateEdgeIndex(qctx, input, std::move(indexName)));
    }

private:
    ShowCreateEdgeIndex(QueryContext* qctx, PlanNode* input, std::string indexName)
        : DescIndexNode(qctx, input, Kind::kShowCreateEdgeIndex, std::move(indexName)) {}
};

class ShowTagIndexes final : public SingleInputNode {
public:
    static ShowTagIndexes* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowTagIndexes(qctx, input));
    }

private:
    ShowTagIndexes(QueryContext* qctx, PlanNode* input)
        : SingleInputNode(qctx, Kind::kShowTagIndexes, input) {}
};

class ShowEdgeIndexes final : public SingleInputNode {
public:
    static ShowEdgeIndexes* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowEdgeIndexes(qctx, input));
    }

private:
    ShowEdgeIndexes(QueryContext* qctx, PlanNode* input)
        : SingleInputNode(qctx, Kind::kShowEdgeIndexes, input) {}
};

class ShowTagIndexStatus final : public SingleInputNode {
public:
    static ShowTagIndexStatus* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowTagIndexStatus(qctx, input));
    }

private:
    ShowTagIndexStatus(QueryContext* qctx, PlanNode* input)
        : SingleInputNode(qctx, Kind::kShowTagIndexStatus, input) {}
};

class ShowEdgeIndexStatus final : public SingleInputNode {
public:
    static ShowEdgeIndexStatus* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowEdgeIndexStatus(qctx, input));
    }

private:
    ShowEdgeIndexStatus(QueryContext* qctx, PlanNode* input)
        : SingleInputNode(qctx, Kind::kShowEdgeIndexStatus, input) {}
};

}   // namespace graph
}   // namespace nebula
#endif   // PLANNER_PLAN_MAINTAIN_H_

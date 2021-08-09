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
class CreateSchemaNode : public SingleDependencyNode {
protected:
    CreateSchemaNode(QueryContext* qctx,
                     PlanNode* input,
                     Kind kind,
                     std::string name,
                     meta::cpp2::Schema schema,
                     bool ifNotExists)
        : SingleDependencyNode(qctx, kind, input),
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

class AlterSchemaNode : public SingleDependencyNode {
protected:
    AlterSchemaNode(QueryContext* qctx,
                    Kind kind,
                    PlanNode* input,
                    GraphSpaceID space,
                    std::string name,
                    std::vector<meta::cpp2::AlterSchemaItem> items,
                    meta::cpp2::SchemaProp schemaProp)
        : SingleDependencyNode(qctx, kind, input),
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

class DescSchemaNode : public SingleDependencyNode {
protected:
    DescSchemaNode(QueryContext* qctx, PlanNode* input, Kind kind, std::string name)
        : SingleDependencyNode(qctx, kind, input), name_(std::move(name)) {}

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

class ShowTags final : public SingleDependencyNode {
public:
    static ShowTags* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowTags(qctx, input));
    }

private:
    ShowTags(QueryContext* qctx, PlanNode* input)
        : SingleDependencyNode(qctx, Kind::kShowTags, input) {}
};

class ShowEdges final : public SingleDependencyNode {
public:
    static ShowEdges* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowEdges(qctx, input));
    }

private:
    ShowEdges(QueryContext* qctx, PlanNode* input)
        : SingleDependencyNode(qctx, Kind::kShowEdges, input) {}
};

class DropSchemaNode : public SingleDependencyNode {
protected:
    DropSchemaNode(QueryContext* qctx, Kind kind, PlanNode* input, std::string name, bool ifExists)
        : SingleDependencyNode(qctx, kind, input), name_(std::move(name)), ifExists_(ifExists) {}

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

class CreateIndexNode : public SingleDependencyNode {
protected:
    CreateIndexNode(QueryContext* qctx,
                    PlanNode* input,
                    Kind kind,
                    std::string schemaName,
                    std::string indexName,
                    std::vector<meta::cpp2::IndexFieldDef> fields,
                    bool ifNotExists,
                    const std::string *comment)
        : SingleDependencyNode(qctx, kind, input),
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

class DescIndexNode : public SingleDependencyNode {
protected:
    DescIndexNode(QueryContext* qctx, PlanNode* input, Kind kind, std::string indexName)
        : SingleDependencyNode(qctx, kind, input), indexName_(std::move(indexName)) {}

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

class DropIndexNode : public SingleDependencyNode {
protected:
    DropIndexNode(QueryContext* qctx,
                  Kind kind,
                  PlanNode* input,
                  std::string indexName,
                  bool ifExists)
        : SingleDependencyNode(qctx, kind, input),
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

class ShowTagIndexes final : public SingleDependencyNode {
public:
    static ShowTagIndexes* make(QueryContext* qctx, PlanNode* input, std::string name) {
        return qctx->objPool()->add(new ShowTagIndexes(qctx, input, std::move(name)));
    }

    const std::string& name() const {
        return name_;
    }

private:
    ShowTagIndexes(QueryContext* qctx, PlanNode* input, std::string name)
        : SingleDependencyNode(qctx, Kind::kShowTagIndexes, input) {
            name_ = std::move(name);
        }

private:
    std::string              name_;
};

class ShowEdgeIndexes final : public SingleDependencyNode {
public:
    static ShowEdgeIndexes* make(QueryContext* qctx, PlanNode* input, std::string name) {
        return qctx->objPool()->add(new ShowEdgeIndexes(qctx, input, std::move(name)));
    }

    const std::string& name() const {
        return name_;
    }

private:
    ShowEdgeIndexes(QueryContext* qctx, PlanNode* input, std::string name)
        : SingleDependencyNode(qctx, Kind::kShowEdgeIndexes, input) {
            name_ = std::move(name);
        }

private:
    std::string              name_;
};

class ShowTagIndexStatus final : public SingleDependencyNode {
public:
    static ShowTagIndexStatus* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowTagIndexStatus(qctx, input));
    }

private:
    ShowTagIndexStatus(QueryContext* qctx, PlanNode* input)
        : SingleDependencyNode(qctx, Kind::kShowTagIndexStatus, input) {}
};

class ShowEdgeIndexStatus final : public SingleDependencyNode {
public:
    static ShowEdgeIndexStatus* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowEdgeIndexStatus(qctx, input));
    }

private:
    ShowEdgeIndexStatus(QueryContext* qctx, PlanNode* input)
        : SingleDependencyNode(qctx, Kind::kShowEdgeIndexStatus, input) {}
};

class CreateFTIndexNode : public SingleInputNode {
protected:
    CreateFTIndexNode(QueryContext* qctx,
                      Kind kind,
                      PlanNode* input,
                      std::string indexName,
                      nebula::meta::cpp2::FTIndex index)
        : SingleInputNode(qctx, kind, input)
        , indexName_(std::move(indexName))
        , index_(std::move(index)) {}

public:
    const std::string& getIndexName() const {
        return indexName_;
    }

    const meta::cpp2::FTIndex& getIndex() const {
        return index_;
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

protected:
    std::string              indexName_;
    meta::cpp2::FTIndex      index_;
};
class CreateFTIndex final : public CreateFTIndexNode {
public:
    static CreateFTIndex* make(QueryContext* qctx,
                               PlanNode* input,
                               std::string indexName,
                               meta::cpp2::FTIndex index) {
        return qctx->objPool()->add(new CreateFTIndex(qctx,
                                                      input,
                                                      std::move(indexName),
                                                      std::move(index)));
    }

private:
    CreateFTIndex(QueryContext* qctx,
                  PlanNode* input,
                  std::string indexName,
                  meta::cpp2::FTIndex index)
        : CreateFTIndexNode(qctx,
                            Kind::kCreateFTIndex,
                            input,
                            std::move(indexName),
                            std::move(index)) {}
};

class DropFTIndexNode : public SingleInputNode {
protected:
    DropFTIndexNode(QueryContext* qctx, Kind kind, PlanNode* input, std::string name)
        : SingleInputNode(qctx, kind, input), name_(std::move(name)) {}

public:
    const std::string& getName() const {
        return name_;
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

protected:
    std::string name_;
};

class DropFTIndex final : public DropFTIndexNode {
public:
    static DropFTIndex* make(QueryContext* qctx, PlanNode* input, std::string name) {
        return qctx->objPool()->add(new DropFTIndex(qctx, input, std::move(name)));
    }

private:
    DropFTIndex(QueryContext* qctx, PlanNode* input, std::string name)
        : DropFTIndexNode(qctx, Kind::kDropFTIndex, input, std::move(name)) {}
};

class ShowFTIndexes final : public SingleInputNode {
public:
    static ShowFTIndexes* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowFTIndexes(qctx, input));
    }

private:
    ShowFTIndexes(QueryContext* qctx, PlanNode* input)
        : SingleInputNode(qctx, Kind::kShowFTIndexes, input) {}
};

}   // namespace graph
}   // namespace nebula
#endif   // PLANNER_PLAN_MAINTAIN_H_

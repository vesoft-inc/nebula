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
        return qctx->objPool()->add(new CreateTag(qctx->genId(), input,
                                                  std::move(tagName),
                                                  std::move(schema),
                                                  ifNotExists));
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
                           ifNotExists) {}
};

class CreateEdge final : public CreateSchemaNode {
public:
    static CreateEdge* make(QueryContext* qctx,
                            PlanNode* input,
                            std::string edgeName,
                            meta::cpp2::Schema schema,
                            bool ifNotExists) {
        return qctx->objPool()->add(new CreateEdge(qctx->genId(), input,
                                                   std::move(edgeName),
                                                   std::move(schema),
                                                   ifNotExists));
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
                           ifNotExists) {}
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
        return qctx->objPool()->add(new AlterTag(qctx->genId(), input, space,
                                                 std::move(name), std::move(items),
                                                 std::move(schemaProp)));
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
        return qctx->objPool()->add(new AlterEdge(qctx->genId(), input, space,
                                                  std::move(name), std::move(items),
                                                  std::move(schemaProp)));
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
                          std::move(schemaProp)) {}
};

class DescSchemaNode : public SingleInputNode {
protected:
    DescSchemaNode(int64_t id,
                   PlanNode* input,
                   Kind kind,
                   std::string name)
        : SingleInputNode(id, kind, input)
        , name_(std::move(name)) {}

public:
    const std::string& getName() const {
        return name_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    std::string            name_;
};

class DescTag final : public DescSchemaNode {
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
        : DescSchemaNode(id, input, Kind::kDescTag, std::move(tagName)) {}
};

class DescEdge final : public DescSchemaNode {
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
        : DescSchemaNode(id, input, Kind::kDescEdge, std::move(edgeName)) {
    }
};

class ShowCreateTag final : public DescSchemaNode {
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
        : DescSchemaNode(id, input, Kind::kShowCreateTag, std::move(name)) {
    }
};

class ShowCreateEdge final : public DescSchemaNode {
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
        : DescSchemaNode(id, input, Kind::kShowCreateEdge, std::move(name)) {}
};

class ShowTags final : public SingleInputNode {
public:
    static ShowTags* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowTags(qctx->genId(), input));
    }

private:
    ShowTags(int64_t id, PlanNode* input)
        : SingleInputNode(id, Kind::kShowTags, input) {}
};

class ShowEdges final : public SingleInputNode {
public:
    static ShowEdges* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowEdges(qctx->genId(), input));
    }

private:
    ShowEdges(int64_t id, PlanNode* input)
        : SingleInputNode(id, Kind::kShowEdges, input) {
    }
};

class DropSchemaNode : public SingleInputNode {
protected:
    DropSchemaNode(int64_t id,
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

class DropTag final : public DropSchemaNode {
public:
    static DropTag* make(QueryContext* qctx,
                         PlanNode* input,
                         std::string name,
                         bool ifExists) {
        return qctx->objPool()->add(new DropTag(qctx->genId(), input,
                                                std::move(name), ifExists));
    }

private:
    DropTag(int64_t id,
            PlanNode* input,
            std::string name,
            bool ifExists)
        : DropSchemaNode(id, Kind::kDropTag, input, std::move(name), ifExists) {}
};

class DropEdge final : public DropSchemaNode {
public:
    static DropEdge* make(QueryContext* qctx,
                          PlanNode* input,
                          std::string name,
                          bool ifExists) {
        return qctx->objPool()->add(new DropEdge(qctx->genId(), input,
                                                 std::move(name), ifExists));
    }

private:
    DropEdge(int64_t id,
             PlanNode* input,
             std::string name,
             bool ifExists)
        : DropSchemaNode(id, Kind::kDropEdge, input, std::move(name), ifExists) {}
};

class CreateIndexNode : public SingleInputNode {
protected:
    CreateIndexNode(int64_t id,
                    PlanNode* input,
                    Kind kind,
                    std::string schemaName,
                    std::string indexName,
                    std::vector<std::string> fields,
                    bool ifNotExists)
        : SingleInputNode(id, kind, input)
        , schemaName_(std::move(schemaName))
        , indexName_(std::move(indexName))
        , fields_(std::move(fields))
        , ifNotExists_(ifNotExists) {}

public:
    const std::string& getSchemaName() const {
        return schemaName_;
    }

    const std::string& getIndexName() const {
        return indexName_;
    }

    const std::vector<std::string>& getFields() const {
        return fields_;
    }

    bool getIfNotExists() const {
        return ifNotExists_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    std::string               schemaName_;
    std::string               indexName_;
    std::vector<std::string>  fields_;
    bool                      ifNotExists_;
};

class CreateTagIndex final : public CreateIndexNode {
public:
    static CreateTagIndex* make(QueryContext* qctx,
                                PlanNode* input,
                                std::string tagName,
                                std::string indexName,
                                std::vector<std::string> fields,
                                bool ifNotExists) {
        return qctx->objPool()->add(new CreateTagIndex(qctx->genId(), input,
                                                       std::move(tagName),
                                                       std::move(indexName),
                                                       std::move(fields),
                                                       ifNotExists));
    }

private:
    CreateTagIndex(int64_t id,
                   PlanNode* input,
                   std::string tagName,
                   std::string indexName,
                   std::vector<std::string> fields,
                   bool ifNotExists)
        : CreateIndexNode(id, input,
                          Kind::kCreateTagIndex,
                          std::move(tagName),
                          std::move(indexName),
                          std::move(fields),
                          ifNotExists) {}
};

class CreateEdgeIndex final : public CreateIndexNode {
public:
    static CreateEdgeIndex* make(QueryContext* qctx,
                                 PlanNode* input,
                                 std::string edgeName,
                                 std::string indexName,
                                 std::vector<std::string> fields,
                                 bool ifNotExists) {
        return qctx->objPool()->add(new CreateEdgeIndex(qctx->genId(), input,
                                                        std::move(edgeName),
                                                        std::move(indexName),
                                                        std::move(fields),
                                                        ifNotExists));
    }

private:
    CreateEdgeIndex(int64_t id,
                    PlanNode* input,
                    std::string edgeName,
                    std::string indexName,
                    std::vector<std::string> fields,
                    bool ifNotExists)
        : CreateIndexNode(id, input,
                          Kind::kCreateEdgeIndex,
                          std::move(edgeName),
                          std::move(indexName),
                          std::move(fields),
                          ifNotExists) {}
};

class DescIndexNode : public SingleInputNode {
protected:
    DescIndexNode(int64_t id,
                  PlanNode* input,
                  Kind kind,
                  std::string indexName)
        : SingleInputNode(id, kind, input)
        , indexName_(std::move(indexName)) {}

public:
    const std::string& getIndexName() const {
        return indexName_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    std::string            indexName_;
};

class DescTagIndex final : public DescIndexNode {
public:
    static DescTagIndex* make(QueryContext* qctx,
                              PlanNode* input,
                              std::string indexName) {
        return qctx->objPool()->add(new DescTagIndex(qctx->genId(), input,
                                                     std::move(indexName)));
    }

private:
    DescTagIndex(int64_t id,
                 PlanNode* input,
                 std::string indexName)
        : DescIndexNode(id, input, Kind::kDescTagIndex, std::move(indexName)) {}
};

class DescEdgeIndex final : public DescIndexNode {
public:
    static DescEdgeIndex* make(QueryContext* qctx,
                               PlanNode* input,
                               std::string indexName) {
        return qctx->objPool()->add(new DescEdgeIndex(qctx->genId(), input,
                                                      std::move(indexName)));
    }

private:
    DescEdgeIndex(int64_t id,
                  PlanNode* input,
                  std::string indexName)
        : DescIndexNode(id, input, Kind::kDescEdgeIndex, std::move(indexName)) {}
};

class DropIndexNode : public SingleInputNode {
protected:
    DropIndexNode(int64_t id,
                  Kind kind,
                  PlanNode* input,
                  std::string indexName,
                  bool ifExists)
        : SingleInputNode(id, kind, input)
        , indexName_(std::move(indexName))
        , ifExists_(ifExists) {}

public:
    const std::string& getIndexName() const {
        return indexName_;
    }

    GraphSpaceID getIfExists() const {
        return ifExists_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    std::string            indexName_;
    bool                   ifExists_;
};

class DropTagIndex final : public DropIndexNode {
public:
    static DropTagIndex* make(QueryContext* qctx,
                              PlanNode* input,
                              std::string indexName,
                              bool ifExists) {
        return qctx->objPool()->add(new DropTagIndex(qctx->genId(), input,
                                                     std::move(indexName), ifExists));
    }

private:
    DropTagIndex(int64_t id,
                 PlanNode* input,
                 std::string indexName,
                 bool ifExists)
        : DropIndexNode(id, Kind::kDropTagIndex, input,
                        std::move(indexName), ifExists) {}
};

class DropEdgeIndex final : public DropIndexNode {
public:
    static DropEdgeIndex* make(QueryContext* qctx,
                               PlanNode* input,
                               std::string indexName,
                               bool ifExists) {
        return qctx->objPool()->add(new DropEdgeIndex(qctx->genId(), input,
                                                      std::move(indexName), ifExists));
    }

private:
    DropEdgeIndex(int64_t id,
                  PlanNode* input,
                  std::string indexName,
                  bool ifExists)
        : DropIndexNode(id, Kind::kDropEdgeIndex, input,
                        std::move(indexName), ifExists) {}
};

class RebuildIndexNode : public SingleInputNode {
protected:
    RebuildIndexNode(int64_t id,
                     Kind kind,
                     PlanNode* input,
                     std::string indexName)
        : SingleInputNode(id, kind, input)
        , indexName_(std::move(indexName)) {}

public:
    const std::string& getIndexName() const {
        return indexName_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    std::string            indexName_;
};

class RebuildTagIndex final : public RebuildIndexNode {
public:
    static RebuildTagIndex* make(QueryContext* qctx,
                                 PlanNode* input,
                                 std::string indexName) {
        return qctx->objPool()->add(new RebuildTagIndex(qctx->genId(), input,
                                                        std::move(indexName)));
    }

private:
    RebuildTagIndex(int64_t id,
                    PlanNode* input,
                    std::string indexName)
        : RebuildIndexNode(id, Kind::kRebuildTagIndex, input, std::move(indexName)) {}
};

class RebuildEdgeIndex final : public RebuildIndexNode {
public:
    static RebuildEdgeIndex* make(QueryContext* qctx,
                                  PlanNode* input,
                                  std::string indexName) {
        return qctx->objPool()->add(new RebuildEdgeIndex(qctx->genId(), input,
                                                         std::move(indexName)));
    }

private:
    RebuildEdgeIndex(int64_t id,
                     PlanNode* input,
                     std::string indexName)
        : RebuildIndexNode(id, Kind::kRebuildEdgeIndex, input, std::move(indexName)) {}
};

}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MAINTAIN_H_

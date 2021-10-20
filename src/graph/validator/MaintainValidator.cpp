/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/validator/MaintainValidator.h"

#include "common/charset/Charset.h"
#include "common/expression/ConstantExpression.h"
#include "graph/planner/plan/Admin.h"
#include "graph/planner/plan/Maintain.h"
#include "graph/planner/plan/Query.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/FTIndexUtils.h"
#include "graph/util/IndexUtil.h"
#include "graph/util/SchemaUtil.h"
#include "parser/MaintainSentences.h"

namespace nebula {
namespace graph {

static Status validateColumns(const std::vector<ColumnSpecification *> &columnSpecs,
                              meta::cpp2::Schema &schema) {
  for (auto &spec : columnSpecs) {
    meta::cpp2::ColumnDef column;
    auto type = spec->type();
    column.set_name(*spec->name());
    column.type.set_type(type);
    if (meta::cpp2::PropertyType::FIXED_STRING == type) {
      column.type.set_type_length(spec->typeLen());
    }
    for (const auto &property : spec->properties()->properties()) {
      if (property->isNullable()) {
        column.set_nullable(property->nullable());
      } else if (property->isDefaultValue()) {
        if (!ExpressionUtils::isEvaluableExpr(property->defaultValue())) {
          return Status::SemanticError("Wrong default value experssion `%s'",
                                       property->defaultValue()->toString().c_str());
        }
        auto *defaultValueExpr = property->defaultValue();
        // some expression is evaluable but not pure so only fold instead of eval here
        auto foldRes = ExpressionUtils::foldConstantExpr(defaultValueExpr);
        NG_RETURN_IF_ERROR(foldRes);
        column.set_default_value(foldRes.value()->encode());
      } else if (property->isComment()) {
        column.set_comment(*DCHECK_NOTNULL(property->comment()));
      }
    }
    if (!column.nullable_ref().has_value()) {
      column.set_nullable(true);
    }
    schema.columns_ref().value().emplace_back(std::move(column));
  }
  return Status::OK();
}

static StatusOr<std::vector<meta::cpp2::AlterSchemaItem>> validateSchemaOpts(
    const std::vector<AlterSchemaOptItem *> &schemaOpts) {
  std::vector<meta::cpp2::AlterSchemaItem> schemaItems;
  std::unordered_set<std::string> uniqueColName;
  for (const auto &schemaOpt : schemaOpts) {
    meta::cpp2::AlterSchemaItem schemaItem;
    auto opType = schemaOpt->toType();
    schemaItem.set_op(opType);
    meta::cpp2::Schema schema;

    if (opType == meta::cpp2::AlterSchemaOp::DROP) {
      const auto &colNames = schemaOpt->columnNames();
      for (auto &colName : colNames) {
        if (!uniqueColName.emplace(*colName).second) {
          return Status::SemanticError("Duplicate column name `%s'", colName->c_str());
        }
        meta::cpp2::ColumnDef column;
        column.name = *colName;
        schema.columns_ref().value().emplace_back(std::move(column));
      }
    } else {
      const auto &specs = schemaOpt->columnSpecs();
      for (auto &spec : specs) {
        if (!uniqueColName.emplace(*spec->name()).second) {
          return Status::SemanticError("Duplicate column name `%s'", spec->name()->c_str());
        }
      }
      NG_LOG_AND_RETURN_IF_ERROR(validateColumns(specs, schema));
    }

    schemaItem.set_schema(std::move(schema));
    schemaItems.emplace_back(std::move(schemaItem));
  }
  return schemaItems;
}

static StatusOr<meta::cpp2::SchemaProp> validateSchemaProps(
    const std::vector<SchemaPropItem *> &schemaProps) {
  meta::cpp2::SchemaProp schemaProp;
  for (const auto &prop : schemaProps) {
    auto propType = prop->getPropType();
    switch (propType) {
      case SchemaPropItem::TTL_DURATION: {
        auto ttlDur = prop->getTtlDuration();
        NG_RETURN_IF_ERROR(ttlDur);
        schemaProp.set_ttl_duration(ttlDur.value());
        break;
      }
      case SchemaPropItem::TTL_COL: {
        // Check the legality of the column in meta
        auto ttlCol = prop->getTtlCol();
        NG_RETURN_IF_ERROR(ttlCol);
        schemaProp.set_ttl_col(ttlCol.value());
        break;
      }
      case SchemaPropItem::COMMENT: {
        // Check the legality of the column in meta
        auto comment = prop->getComment();
        NG_RETURN_IF_ERROR(comment);
        schemaProp.set_comment(comment.value());
        break;
      }
      default: {
        return Status::SemanticError("Property type not support");
      }
    }
  }
  return schemaProp;
}

static Status checkColName(const std::vector<ColumnSpecification *> specs) {
  std::unordered_set<std::string> uniqueColName;
  for (const auto &spec : specs) {
    auto name = *spec->name();
    if (!uniqueColName.emplace(name).second) {
      return Status::SemanticError("Duplicate column name `%s'", name.c_str());
    }
  }
  return Status::OK();
}

Status CreateTagValidator::validateImpl() {
  createCtx_ = getContext<CreateSchemaContext>();
  auto sentence = static_cast<CreateTagSentence *>(sentence_);
  createCtx_->ifNotExist = sentence->isIfNotExist();
  auto name = *sentence->name();
  // Check the validateContext has the same name schema
  auto pro = vctx_->getSchema(name);
  if (pro != nullptr) {
    return Status::SemanticError("Has the same name `%s' in the SequentialSentences", name.c_str());
  }
  meta::cpp2::Schema schema;
  NG_RETURN_IF_ERROR(checkColName(sentence->columnSpecs()));
  NG_RETURN_IF_ERROR(validateColumns(sentence->columnSpecs(), schema));
  NG_RETURN_IF_ERROR(SchemaUtil::validateProps(sentence->getSchemaProps(), schema));
  // Save the schema in validateContext
  auto pool = qctx_->objPool();
  auto schemaPro = SchemaUtil::generateSchemaProvider(pool, 0, schema);
  vctx_->addSchema(name, schemaPro);
  createCtx_->name = std::move(name);
  createCtx_->schema = std::move(schema);
  return Status::OK();
}

Status CreateEdgeValidator::validateImpl() {
  createCtx_ = getContext<CreateSchemaContext>();
  auto sentence = static_cast<CreateEdgeSentence *>(sentence_);
  createCtx_->ifNotExist = sentence->isIfNotExist();
  auto name = *sentence->name();
  // Check the validateContext has the same name schema
  auto pro = vctx_->getSchema(name);
  if (pro != nullptr) {
    return Status::SemanticError("Has the same name `%s' in the SequentialSentences", name.c_str());
  }
  meta::cpp2::Schema schema;
  NG_RETURN_IF_ERROR(checkColName(sentence->columnSpecs()));
  NG_RETURN_IF_ERROR(validateColumns(sentence->columnSpecs(), schema));
  NG_RETURN_IF_ERROR(SchemaUtil::validateProps(sentence->getSchemaProps(), schema));
  // Save the schema in validateContext
  auto pool = qctx_->objPool();
  auto schemaPro = SchemaUtil::generateSchemaProvider(pool, 0, schema);
  vctx_->addSchema(name, schemaPro);
  createCtx_->name = std::move(name);
  createCtx_->schema = std::move(schema);
  return Status::OK();
}

Status DescTagValidator::validateImpl() { return Status::OK(); }

Status DescTagValidator::toPlan() {
  auto sentence = static_cast<DescribeTagSentence *>(sentence_);
  auto name = *sentence->name();
  auto doNode = DescTag::make(qctx_, nullptr, std::move(name));
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status DescEdgeValidator::validateImpl() { return Status::OK(); }

Status DescEdgeValidator::toPlan() {
  auto sentence = static_cast<DescribeEdgeSentence *>(sentence_);
  auto name = *sentence->name();
  auto doNode = DescEdge::make(qctx_, nullptr, std::move(name));
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status AlterTagValidator::validateImpl() {
  alterCtx_ = getContext<AlterSchemaContext>();
  auto sentence = static_cast<AlterTagSentence *>(sentence_);
  auto schemaItems = validateSchemaOpts(sentence->getSchemaOpts());
  NG_RETURN_IF_ERROR(schemaItems);
  alterCtx_->schemaItems = std::move(schemaItems.value());
  auto schemaProps = validateSchemaProps(sentence->getSchemaProps());
  NG_RETURN_IF_ERROR(schemaProps);
  alterCtx_->schemaProps = std::move(schemaProps.value());
  return Status::OK();
}

Status AlterEdgeValidator::validateImpl() {
  alterCtx_ = getContext<AlterSchemaContext>();
  auto sentence = static_cast<AlterEdgeSentence *>(sentence_);
  auto schemaItems = validateSchemaOpts(sentence->getSchemaOpts());
  NG_RETURN_IF_ERROR(schemaItems);
  alterCtx_->schemaItems = std::move(schemaItems.value());
  auto schemaProps = validateSchemaProps(sentence->getSchemaProps());
  NG_RETURN_IF_ERROR(schemaProps);
  alterCtx_->schemaProps = std::move(schemaProps.value());
  return Status::OK();
}

Status ShowTagsValidator::validateImpl() { return Status::OK(); }

Status ShowTagsValidator::toPlan() {
  auto *doNode = ShowTags::make(qctx_, nullptr);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status ShowEdgesValidator::validateImpl() { return Status::OK(); }

Status ShowEdgesValidator::toPlan() {
  auto *doNode = ShowEdges::make(qctx_, nullptr);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status ShowCreateTagValidator::validateImpl() { return Status::OK(); }

Status ShowCreateTagValidator::toPlan() {
  auto sentence = static_cast<ShowCreateTagSentence *>(sentence_);
  auto *doNode = ShowCreateTag::make(qctx_, nullptr, *sentence->name());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status ShowCreateEdgeValidator::validateImpl() { return Status::OK(); }

Status ShowCreateEdgeValidator::toPlan() {
  auto sentence = static_cast<ShowCreateEdgeSentence *>(sentence_);
  auto *doNode = ShowCreateEdge::make(qctx_, nullptr, *sentence->name());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status DropTagValidator::validateImpl() { return Status::OK(); }

Status DropTagValidator::toPlan() {
  auto sentence = static_cast<DropTagSentence *>(sentence_);
  auto *doNode = DropTag::make(qctx_, nullptr, *sentence->name(), sentence->isIfExists());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status DropEdgeValidator::validateImpl() { return Status::OK(); }

Status DropEdgeValidator::toPlan() {
  auto sentence = static_cast<DropEdgeSentence *>(sentence_);
  auto *doNode = DropEdge::make(qctx_, nullptr, *sentence->name(), sentence->isIfExists());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status CreateTagIndexValidator::validateImpl() {
  auto sentence = static_cast<CreateTagIndexSentence *>(sentence_);
  name_ = *sentence->tagName();
  index_ = *sentence->indexName();
  fields_ = sentence->fields();
  ifNotExist_ = sentence->isIfNotExist();
  // TODO(darion) Save the index
  return Status::OK();
}

Status CreateTagIndexValidator::toPlan() {
  auto sentence = static_cast<CreateTagIndexSentence *>(sentence_);
  auto *doNode = CreateTagIndex::make(qctx_,
                                      nullptr,
                                      *sentence->tagName(),
                                      *sentence->indexName(),
                                      sentence->fields(),
                                      sentence->isIfNotExist(),
                                      sentence->comment());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status CreateEdgeIndexValidator::validateImpl() {
  auto sentence = static_cast<CreateEdgeIndexSentence *>(sentence_);
  name_ = *sentence->edgeName();
  index_ = *sentence->indexName();
  fields_ = sentence->fields();
  ifNotExist_ = sentence->isIfNotExist();
  // TODO(darion) Save the index
  return Status::OK();
}

Status CreateEdgeIndexValidator::toPlan() {
  auto sentence = static_cast<CreateEdgeIndexSentence *>(sentence_);
  auto *doNode = CreateEdgeIndex::make(qctx_,
                                       nullptr,
                                       *sentence->edgeName(),
                                       *sentence->indexName(),
                                       sentence->fields(),
                                       sentence->isIfNotExist(),
                                       sentence->comment());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status DropTagIndexValidator::validateImpl() {
  auto sentence = static_cast<DropTagIndexSentence *>(sentence_);
  indexName_ = *sentence->indexName();
  ifExist_ = sentence->isIfExists();
  return Status::OK();
}

Status DropTagIndexValidator::toPlan() {
  auto *doNode = DropTagIndex::make(qctx_, nullptr, indexName_, ifExist_);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status DropEdgeIndexValidator::validateImpl() {
  auto sentence = static_cast<DropEdgeIndexSentence *>(sentence_);
  indexName_ = *sentence->indexName();
  ifExist_ = sentence->isIfExists();
  return Status::OK();
}

Status DropEdgeIndexValidator::toPlan() {
  auto *doNode = DropEdgeIndex::make(qctx_, nullptr, indexName_, ifExist_);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status DescribeTagIndexValidator::validateImpl() {
  auto sentence = static_cast<DescribeTagIndexSentence *>(sentence_);
  indexName_ = *sentence->indexName();
  return Status::OK();
}

Status DescribeTagIndexValidator::toPlan() {
  auto *doNode = DescTagIndex::make(qctx_, nullptr, indexName_);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status DescribeEdgeIndexValidator::validateImpl() {
  auto sentence = static_cast<DescribeEdgeIndexSentence *>(sentence_);
  indexName_ = *sentence->indexName();
  return Status::OK();
}

Status DescribeEdgeIndexValidator::toPlan() {
  auto *doNode = DescEdgeIndex::make(qctx_, nullptr, indexName_);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status ShowCreateTagIndexValidator::validateImpl() {
  auto sentence = static_cast<ShowCreateTagIndexSentence *>(sentence_);
  indexName_ = *sentence->indexName();
  return Status::OK();
}

Status ShowCreateTagIndexValidator::toPlan() {
  auto *doNode = ShowCreateTagIndex::make(qctx_, nullptr, indexName_);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status ShowCreateEdgeIndexValidator::validateImpl() {
  auto sentence = static_cast<ShowCreateTagIndexSentence *>(sentence_);
  indexName_ = *sentence->indexName();
  return Status::OK();
}

Status ShowCreateEdgeIndexValidator::toPlan() {
  auto *doNode = ShowCreateEdgeIndex::make(qctx_, nullptr, indexName_);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status ShowTagIndexesValidator::validateImpl() {
  auto sentence = static_cast<ShowTagIndexesSentence *>(sentence_);
  name_ = *sentence->name();
  return Status::OK();
}

Status ShowTagIndexesValidator::toPlan() {
  auto *doNode = ShowTagIndexes::make(qctx_, nullptr, name_);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status ShowEdgeIndexesValidator::validateImpl() {
  auto sentence = static_cast<ShowEdgeIndexesSentence *>(sentence_);
  name_ = *sentence->name();
  return Status::OK();
}

Status ShowEdgeIndexesValidator::toPlan() {
  auto *doNode = ShowEdgeIndexes::make(qctx_, nullptr, name_);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status ShowTagIndexStatusValidator::validateImpl() { return Status::OK(); }

Status ShowTagIndexStatusValidator::toPlan() {
  auto *doNode = ShowTagIndexStatus::make(qctx_, nullptr);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status ShowEdgeIndexStatusValidator::validateImpl() { return Status::OK(); }

Status ShowEdgeIndexStatusValidator::toPlan() {
  auto *doNode = ShowEdgeIndexStatus::make(qctx_, nullptr);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status AddGroupValidator::validateImpl() {
  auto sentence = static_cast<AddGroupSentence *>(sentence_);
  if (*sentence->groupName() == "default") {
    return Status::SemanticError("Group default conflict");
  }
  return Status::OK();
}

Status AddGroupValidator::toPlan() {
  auto sentence = static_cast<AddGroupSentence *>(sentence_);
  auto *doNode =
      AddGroup::make(qctx_, nullptr, *sentence->groupName(), sentence->zoneNames()->zoneNames());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status DropGroupValidator::validateImpl() { return Status::OK(); }

Status DropGroupValidator::toPlan() {
  auto sentence = static_cast<DropGroupSentence *>(sentence_);
  auto *doNode = DropGroup::make(qctx_, nullptr, *sentence->groupName());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status DescribeGroupValidator::validateImpl() { return Status::OK(); }

Status DescribeGroupValidator::toPlan() {
  auto sentence = static_cast<DescribeGroupSentence *>(sentence_);
  auto *doNode = DescribeGroup::make(qctx_, nullptr, *sentence->groupName());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status ListGroupsValidator::validateImpl() { return Status::OK(); }

Status ListGroupsValidator::toPlan() {
  auto *doNode = ListGroups::make(qctx_, nullptr);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status AddZoneIntoGroupValidator::validateImpl() { return Status::OK(); }

Status AddZoneIntoGroupValidator::toPlan() {
  auto sentence = static_cast<AddZoneIntoGroupSentence *>(sentence_);
  auto *doNode =
      AddZoneIntoGroup::make(qctx_, nullptr, *sentence->groupName(), *sentence->zoneName());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status DropZoneFromGroupValidator::validateImpl() { return Status::OK(); }

Status DropZoneFromGroupValidator::toPlan() {
  auto sentence = static_cast<DropZoneFromGroupSentence *>(sentence_);
  auto *doNode =
      DropZoneFromGroup::make(qctx_, nullptr, *sentence->groupName(), *sentence->zoneName());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status AddZoneValidator::validateImpl() { return Status::OK(); }

Status AddZoneValidator::toPlan() {
  auto sentence = static_cast<AddZoneSentence *>(sentence_);
  auto *doNode = AddZone::make(qctx_, nullptr, *sentence->zoneName(), sentence->hosts()->hosts());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status DropZoneValidator::validateImpl() { return Status::OK(); }

Status DropZoneValidator::toPlan() {
  auto sentence = static_cast<DropZoneSentence *>(sentence_);
  auto *doNode = DropZone::make(qctx_, nullptr, *sentence->zoneName());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status DescribeZoneValidator::validateImpl() { return Status::OK(); }

Status DescribeZoneValidator::toPlan() {
  auto sentence = static_cast<DescribeZoneSentence *>(sentence_);
  auto *doNode = DescribeZone::make(qctx_, nullptr, *sentence->zoneName());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status ListZonesValidator::validateImpl() { return Status::OK(); }

Status ListZonesValidator::toPlan() {
  auto *doNode = ListZones::make(qctx_, nullptr);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status AddHostIntoZoneValidator::validateImpl() { return Status::OK(); }

Status AddHostIntoZoneValidator::toPlan() {
  auto sentence = static_cast<AddHostIntoZoneSentence *>(sentence_);
  auto *doNode = AddHostIntoZone::make(qctx_, nullptr, *sentence->zoneName(), *sentence->address());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status DropHostFromZoneValidator::validateImpl() { return Status::OK(); }

Status DropHostFromZoneValidator::toPlan() {
  auto sentence = static_cast<DropHostFromZoneSentence *>(sentence_);
  auto *doNode =
      DropHostFromZone::make(qctx_, nullptr, *sentence->zoneName(), *sentence->address());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status CreateFTIndexValidator::validateImpl() {
  auto sentence = static_cast<CreateFTIndexSentence *>(sentence_);
  auto name = *sentence->indexName();
  if (name.substr(0, sizeof(FULLTEXT_INDEX_NAME_PREFIX) - 1) != FULLTEXT_INDEX_NAME_PREFIX) {
    return Status::SyntaxError("Index name must begin with \"%s\"", FULLTEXT_INDEX_NAME_PREFIX);
  }
  auto tsRet = FTIndexUtils::getTSClients(qctx_->getMetaClient());
  NG_RETURN_IF_ERROR(tsRet);
  auto tsIndex = FTIndexUtils::checkTSIndex(std::move(tsRet).value(), name);
  NG_RETURN_IF_ERROR(tsIndex);
  if (tsIndex.value()) {
    return Status::Error("text search index exist : %s", name.c_str());
  }
  auto space = vctx_->whichSpace();
  auto status = sentence->isEdge()
                    ? qctx_->schemaMng()->toEdgeType(space.id, *sentence->schemaName())
                    : qctx_->schemaMng()->toTagID(space.id, *sentence->schemaName());
  NG_RETURN_IF_ERROR(status);
  nebula::cpp2::SchemaID id;
  if (sentence->isEdge()) {
    id.set_edge_type(status.value());
  } else {
    id.set_tag_id(status.value());
  }
  index_.set_space_id(space.id);
  index_.set_depend_schema(std::move(id));
  index_.set_fields(sentence->fields());
  return Status::OK();
}

Status CreateFTIndexValidator::toPlan() {
  auto sentence = static_cast<CreateFTIndexSentence *>(sentence_);
  auto *doNode = CreateFTIndex::make(qctx_, nullptr, *sentence->indexName(), index_);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status DropFTIndexValidator::validateImpl() {
  auto tsRet = FTIndexUtils::getTSClients(qctx_->getMetaClient());
  NG_RETURN_IF_ERROR(tsRet);
  return Status::OK();
}

Status DropFTIndexValidator::toPlan() {
  auto sentence = static_cast<DropFTIndexSentence *>(sentence_);
  auto *doNode = DropFTIndex::make(qctx_, nullptr, *sentence->name());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

Status ShowFTIndexesValidator::validateImpl() { return Status::OK(); }

Status ShowFTIndexesValidator::toPlan() {
  auto *doNode = ShowFTIndexes::make(qctx_, nullptr);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}
}  // namespace graph
}  // namespace nebula

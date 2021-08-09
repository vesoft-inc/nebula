/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "validator/MutateValidator.h"

#include "common/expression/LabelAttributeExpression.h"
#include "planner/plan/Mutate.h"
#include "planner/plan/Query.h"
#include "util/SchemaUtil.h"
#include "visitor/RewriteSymExprVisitor.h"

namespace nebula {
namespace graph {

Status InsertVerticesValidator::validateImpl() {
    spaceId_ = vctx_->whichSpace().id;
    NG_RETURN_IF_ERROR(check());
    NG_RETURN_IF_ERROR(prepareVertices());
    return Status::OK();
}

Status InsertVerticesValidator::toPlan() {
    auto doNode = InsertVertices::make(qctx_,
                                       nullptr,
                                       spaceId_,
                                       std::move(vertices_),
                                       std::move(tagPropNames_),
                                       ifNotExists_);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status InsertVerticesValidator::check() {
    auto sentence = static_cast<InsertVerticesSentence*>(sentence_);
    ifNotExists_ = sentence->isIfNotExists();
    rows_ = sentence->rows();
    if (rows_.empty()) {
        return Status::SemanticError("VALUES cannot be empty");
    }

    auto tagItems = sentence->tagItems();

    schemas_.reserve(tagItems.size());

    for (auto& item : tagItems) {
        auto *tagName = item->tagName();
        auto tagStatus = qctx_->schemaMng()->toTagID(spaceId_, *tagName);
        if (!tagStatus.ok()) {
            LOG(ERROR) << "No schema found for " << *tagName;
            return Status::SemanticError("No schema found for `%s'", tagName->c_str());
        }

        auto tagId = tagStatus.value();
        auto schema = qctx_->schemaMng()->getTagSchema(spaceId_, tagId);
        if (schema == nullptr) {
            LOG(ERROR) << "No schema found for " << *tagName;
            return Status::SemanticError("No schema found for `%s'", tagName->c_str());
        }

        std::vector<std::string> names;
        if (item->isDefaultPropNames()) {
            size_t numFields = schema->getNumFields();
            for (size_t i = 0; i < numFields; ++i) {
                const char* propName = schema->getFieldName(i);
                names.emplace_back(propName);
            }
            propSize_ += numFields;
        } else {
            auto props = item->properties();
            // Check prop name is in schema
            for (auto *it : props) {
                if (schema->getFieldIndex(*it) < 0) {
                    LOG(ERROR) << "Unknown column `" << *it << "' in schema";
                    return Status::SemanticError("Unknown column `%s' in schema", it->c_str());
                }
                propSize_++;
                names.emplace_back(*it);
            }
        }
        tagPropNames_[tagId] = names;
        schemas_.emplace_back(tagId, schema);
    }
    return Status::OK();
}

Status InsertVerticesValidator::prepareVertices() {
    vertices_.reserve(rows_.size());
    for (auto i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        if (propSize_ != row->values().size()) {
            return Status::SemanticError("Column count doesn't match value count.");
        }
        if (!evaluableExpr(row->id())) {
            LOG(ERROR) << "Wrong vid expression `" << row->id()->toString() << "\"";
            return Status::SemanticError("Wrong vid expression `%s'",
                                         row->id()->toString().c_str());
        }
        auto idStatus = SchemaUtil::toVertexID(row->id(), vidType_);
        NG_RETURN_IF_ERROR(idStatus);
        auto vertexId = std::move(idStatus).value();

        // check value expr
        for (auto &value : row->values()) {
            if (!evaluableExpr(value)) {
                LOG(ERROR) << "Insert wrong value: `" << value->toString() << "'.";
                return Status::SemanticError("Insert wrong value: `%s'.",
                                             value->toString().c_str());
            }
        }
        auto valsRet = SchemaUtil::toValueVec(row->values());
        NG_RETURN_IF_ERROR(valsRet);
        auto values = std::move(valsRet).value();

        std::vector<storage::cpp2::NewTag> tags(schemas_.size());
        int32_t handleValueNum = 0;
        for (auto count = 0u; count < schemas_.size(); count++) {
            auto tagId = schemas_[count].first;
            auto schema = schemas_[count].second;
            auto &propNames = tagPropNames_[tagId];
            std::vector<Value> props;
            props.reserve(propNames.size());
            for (auto index = 0u; index < propNames.size(); index++) {
                props.emplace_back(std::move(values[handleValueNum]));
                handleValueNum++;
            }
            auto &tag = tags[count];
            tag.set_tag_id(tagId);
            tag.set_props(std::move(props));
        }

        storage::cpp2::NewVertex vertex;
        vertex.set_id(vertexId);
        vertex.set_tags(std::move(tags));
        vertices_.emplace_back(std::move(vertex));
    }
    return Status::OK();
}

Status InsertEdgesValidator::validateImpl() {
    spaceId_ = vctx_->whichSpace().id;
    NG_RETURN_IF_ERROR(check());
    NG_RETURN_IF_ERROR(prepareEdges());
    return Status::OK();
}

Status InsertEdgesValidator::toPlan() {
    using IsoLevel = meta::cpp2::IsolationLevel;
    auto isoLevel = space_.spaceDesc.isolation_level_ref().value_or(IsoLevel::DEFAULT);
    auto useChainInsert = isoLevel == IsoLevel::TOSS;
    auto doNode = InsertEdges::make(qctx_,
                                    nullptr,
                                    spaceId_,
                                    std::move(edges_),
                                    std::move(propNames_),
                                    ifNotExists_,
                                    useChainInsert);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status InsertEdgesValidator::check() {
    auto sentence = static_cast<InsertEdgesSentence*>(sentence_);
    ifNotExists_ = sentence->isIfNotExists();
    auto edgeStatus = qctx_->schemaMng()->toEdgeType(spaceId_, *sentence->edge());
    NG_RETURN_IF_ERROR(edgeStatus);
    edgeType_ = edgeStatus.value();
    auto props = sentence->properties();
    rows_ = sentence->rows();

    schema_ = qctx_->schemaMng()->getEdgeSchema(spaceId_, edgeType_);
    if (schema_ == nullptr) {
        LOG(ERROR) << "No schema found for " << sentence->edge();
        return Status::SemanticError("No schema found for `%s'", sentence->edge()->c_str());
    }

    if (sentence->isDefaultPropNames()) {
        size_t propNums = schema_->getNumFields();
        for (size_t i = 0; i < propNums; ++i) {
            const char* propName = schema_->getFieldName(i);
            propNames_.emplace_back(propName);
        }
    } else {
        // Check prop name is in schema
        for (auto *it : props) {
            if (schema_->getFieldIndex(*it) < 0) {
                LOG(ERROR) << "Unknown column `" << *it << "' in schema";
                return Status::SemanticError("Unknown column `%s' in schema", it->c_str());
            }
            propNames_.emplace_back(*it);
        }
    }
    return Status::OK();
}

Status InsertEdgesValidator::prepareEdges() {
    using IsoLevel = meta::cpp2::IsolationLevel;
    auto isoLevel = space_.spaceDesc.isolation_level_ref().value_or(IsoLevel::DEFAULT);
    auto useToss = isoLevel == IsoLevel::TOSS;
    auto size = useToss ? rows_.size() : rows_.size() * 2;
    edges_.reserve(size);
    for (auto i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        if (propNames_.size() != row->values().size()) {
            return Status::SemanticError("Column count doesn't match value count.");
        }
        if (!evaluableExpr(row->srcid())) {
            LOG(ERROR) << "Wrong src vid expression `" << row->srcid()->toString() << "\"";
            return Status::SemanticError("Wrong src vid expression `%s'",
                                         row->srcid()->toString().c_str());
        }

        if (!evaluableExpr(row->dstid())) {
            LOG(ERROR) << "Wrong dst vid expression `" << row->dstid()->toString() << "\"";
            return Status::SemanticError("Wrong dst vid expression `%s'",
                                         row->dstid()->toString().c_str());
        }

        auto idStatus = SchemaUtil::toVertexID(row->srcid(), vidType_);
        NG_RETURN_IF_ERROR(idStatus);
        auto srcId = std::move(idStatus).value();
        idStatus = SchemaUtil::toVertexID(row->dstid(), vidType_);
        NG_RETURN_IF_ERROR(idStatus);
        auto dstId = std::move(idStatus).value();

        int64_t rank = row->rank();

        // check value expr
        for (auto &value : row->values()) {
            if (!evaluableExpr(value)) {
                LOG(ERROR) << "Insert wrong value: `" << value->toString() << "'.";
                return Status::SemanticError("Insert wrong value: `%s'.",
                                             value->toString().c_str());
            }
        }

        auto valsRet = SchemaUtil::toValueVec(row->values());
        NG_RETURN_IF_ERROR(valsRet);
        auto props = std::move(valsRet).value();
        storage::cpp2::NewEdge edge;
        storage::cpp2::EdgeKey key;

        key.set_src(srcId);
        key.set_dst(dstId);
        key.set_edge_type(edgeType_);
        key.set_ranking(rank);
        edge.set_key(key);
        edge.set_props(std::move(props));
        edges_.emplace_back(edge);
        if (!useToss) {
            // inbound
            key.set_src(dstId);
            key.set_dst(srcId);
            key.set_edge_type(-edgeType_);
            edge.set_key(key);
            edges_.emplace_back(std::move(edge));
        }
    }

    return Status::OK();
}

Status DeleteVerticesValidator::validateImpl() {
    auto sentence = static_cast<DeleteVerticesSentence*>(sentence_);
    spaceId_ = vctx_->whichSpace().id;
    if (sentence->vertices()->isRef()) {
        vidRef_ = sentence->vertices()->ref();
        auto type = deduceExprType(vidRef_);
        NG_RETURN_IF_ERROR(type);
        if (type.value() != vidType_) {
            std::stringstream ss;
            ss << "The vid `" << vidRef_->toString() << "' should be type of `" << vidType_
               << "', but was`" << type.value() << "'";
            return Status::SemanticError(ss.str());
        }
    } else {
        auto vIds = sentence->vertices()->vidList();
        for (auto vId : vIds) {
            auto idStatus = SchemaUtil::toVertexID(vId, vidType_);
            NG_RETURN_IF_ERROR(idStatus);
            vertices_.emplace_back(std::move(idStatus).value());
        }
    }

    auto ret = qctx_->schemaMng()->getAllEdge(spaceId_);
    NG_RETURN_IF_ERROR(ret);
    edgeNames_ = std::move(ret).value();
    for (auto &name : edgeNames_) {
        auto edgeStatus = qctx_->schemaMng()->toEdgeType(spaceId_, name);
        NG_RETURN_IF_ERROR(edgeStatus);
        auto edgeType = edgeStatus.value();
        edgeTypes_.emplace_back(edgeType);
    }
    return Status::OK();
}

std::string DeleteVerticesValidator::buildVIds() {
    auto input = vctx_->anonVarGen()->getVar();
    DataSet ds;
    ds.colNames.emplace_back(kVid);
    for (auto& vid : vertices_) {
        Row row;
        row.values.emplace_back(vid);
        ds.rows.emplace_back(std::move(row));
    }
    qctx_->ectx()->setResult(input, ResultBuilder().value(Value(std::move(ds))).finish());
    auto *pool = qctx_->objPool();
    auto *vIds = VariablePropertyExpression::make(pool, input, kVid);
    vidRef_ = vIds;
    return input;
}

Status DeleteVerticesValidator::toPlan() {
    std::string vidVar;
    if (!vertices_.empty() && vidRef_ == nullptr) {
        vidVar = buildVIds();
    } else if (vidRef_ != nullptr && vidRef_->kind() == Expression::Kind::kVarProperty) {
        vidVar = static_cast<PropertyExpression*>(vidRef_)->sym();
    } else if (vidRef_ != nullptr && vidRef_->kind() == Expression::Kind::kInputProperty) {
        vidVar = inputVarName_;
    }

    auto* dedupVid = Dedup::make(qctx_, nullptr);
    dedupVid->setInputVar(vidVar);

    std::vector<storage::cpp2::EdgeProp> edgeProps;
    // make edgeRefs and edgeProp
    auto index = 0u;
    DCHECK(edgeTypes_.size() == edgeNames_.size());
    auto *pool = qctx_->objPool();
    for (auto &name : edgeNames_) {
        auto *edgeKeyRef = new EdgeKeyRef(EdgeSrcIdExpression::make(pool, name),
                                          EdgeDstIdExpression::make(pool, name),
                                          EdgeRankExpression::make(pool, name));
        edgeKeyRef->setType(EdgeTypeExpression::make(pool, name));
        qctx_->objPool()->add(edgeKeyRef);
        edgeKeyRefs_.emplace_back(edgeKeyRef);

        storage::cpp2::EdgeProp edgeProp;
        edgeProp.set_type(edgeTypes_[index]);
        edgeProp.props_ref().value().emplace_back(kSrc);
        edgeProp.props_ref().value().emplace_back(kDst);
        edgeProp.props_ref().value().emplace_back(kType);
        edgeProp.props_ref().value().emplace_back(kRank);
        edgeProps.emplace_back(edgeProp);

        edgeProp.set_type(-edgeTypes_[index]);
        edgeProps.emplace_back(std::move(edgeProp));
        index++;
    }

    auto vertexPropsPtr = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    auto edgePropsPtr = std::make_unique<std::vector<storage::cpp2::EdgeProp>>(edgeProps);
    auto statPropsPtr = std::make_unique<std::vector<storage::cpp2::StatProp>>();
    auto exprPtr = std::make_unique<std::vector<storage::cpp2::Expr>>();
    auto* getNeighbors = GetNeighbors::make(qctx_,
                                            dedupVid,
                                            spaceId_,
                                            vidRef_,
                                            edgeTypes_,
                                            storage::cpp2::EdgeDirection::BOTH,
                                            nullptr,
                                            std::move(edgePropsPtr),
                                            std::move(statPropsPtr),
                                            std::move(exprPtr));

    auto *yieldColumns = pool->makeAndAdd<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(EdgeSrcIdExpression::make(pool, "*"), kSrc));
    yieldColumns->addColumn(new YieldColumn(EdgeTypeExpression::make(pool, "*"), kType));
    yieldColumns->addColumn(new YieldColumn(EdgeRankExpression::make(pool, "*"), kRank));
    yieldColumns->addColumn(new YieldColumn(EdgeDstIdExpression::make(pool, "*"), kDst));
    auto *edgeKey = Project::make(qctx_, getNeighbors, yieldColumns);

    auto *dedupEdgeKey = Dedup::make(qctx_, edgeKey);

    // create deleteEdges node
    auto *edgeKeyRef = pool->makeAndAdd<EdgeKeyRef>(InputPropertyExpression::make(pool, kSrc),
                                                    InputPropertyExpression::make(pool, kDst),
                                                    InputPropertyExpression::make(pool, kRank),
                                                    true);
    edgeKeyRef->setType(InputPropertyExpression::make(pool, kType));
    auto *deNode = DeleteEdges::make(qctx_,
                                     dedupEdgeKey,
                                     spaceId_,
                                     edgeKeyRef);

    auto *dvNode = DeleteVertices::make(qctx_,
                                        deNode,
                                        spaceId_,
                                        vidRef_);

    dvNode->setInputVar(dedupVid->outputVar());
    root_ = dvNode;
    tail_ = dedupVid;
    return Status::OK();
}

Status DeleteEdgesValidator::validateImpl() {
    auto sentence = static_cast<DeleteEdgesSentence*>(sentence_);
    auto spaceId = vctx_->whichSpace().id;
    auto edgeStatus = qctx_->schemaMng()->toEdgeType(spaceId, *sentence->edge());
    NG_RETURN_IF_ERROR(edgeStatus);
    auto edgeType = edgeStatus.value();
    auto *pool = qctx_->objPool();
    if (sentence->isRef()) {
        edgeKeyRefs_.emplace_back(sentence->edgeKeyRef());
        (*edgeKeyRefs_.begin())->setType(ConstantExpression::make(pool, edgeType));
        NG_RETURN_IF_ERROR(checkInput());
    } else {
        return buildEdgeKeyRef(sentence->edgeKeys()->keys(), edgeType);
    }

    return Status::OK();
}

Status DeleteEdgesValidator::buildEdgeKeyRef(const std::vector<EdgeKey*> &edgeKeys,
                                             const EdgeType edgeType) {
    edgeKeyVar_ = vctx_->anonVarGen()->getVar();
    DataSet ds({kSrc, kType, kRank, kDst});
    for (auto &edgeKey : edgeKeys) {
        Row row;
        storage::cpp2::EdgeKey key;
        auto srcIdStatus = SchemaUtil::toVertexID(edgeKey->srcid(), vidType_);
        NG_RETURN_IF_ERROR(srcIdStatus);
        auto dstIdStatus = SchemaUtil::toVertexID(edgeKey->dstid(), vidType_);
        NG_RETURN_IF_ERROR(dstIdStatus);

        auto srcId = std::move(srcIdStatus).value();
        auto dstId = std::move(dstIdStatus).value();
        // out edge
        row.emplace_back(std::move(srcId));
        row.emplace_back(edgeType);
        row.emplace_back(edgeKey->rank());
        row.emplace_back(std::move(dstId));
        ds.emplace_back(std::move(row));
    }
    qctx_->ectx()->setResult(edgeKeyVar_, ResultBuilder().value(Value(std::move(ds))).finish());
    auto *pool = qctx_->objPool();
    auto *srcIdExpr = InputPropertyExpression::make(pool, kSrc);
    auto *typeExpr = InputPropertyExpression::make(pool, kType);
    auto *rankExpr = InputPropertyExpression::make(pool, kRank);
    auto *dstIdExpr = InputPropertyExpression::make(pool, kDst);
    auto* edgeKeyRef = new EdgeKeyRef(srcIdExpr, dstIdExpr, rankExpr);
    edgeKeyRef->setType(typeExpr);
    qctx_->objPool()->add(edgeKeyRef);

    edgeKeyRefs_.emplace_back(edgeKeyRef);
    return Status::OK();
}

Status DeleteEdgesValidator::checkInput() {
    CHECK(!edgeKeyRefs_.empty());
    auto &edgeKeyRef = *edgeKeyRefs_.begin();
    NG_LOG_AND_RETURN_IF_ERROR(deduceProps(edgeKeyRef->srcid(), exprProps_));
    NG_LOG_AND_RETURN_IF_ERROR(deduceProps(edgeKeyRef->dstid(), exprProps_));
    NG_LOG_AND_RETURN_IF_ERROR(deduceProps(edgeKeyRef->rank(), exprProps_));

    if (!exprProps_.srcTagProps().empty() || !exprProps_.dstTagProps().empty() ||
        !exprProps_.edgeProps().empty()) {
        return Status::SyntaxError("Only support input and variable.");
    }

    if (!exprProps_.inputProps().empty() && !exprProps_.varProps().empty()) {
        return Status::SemanticError("Not support both input and variable.");
    }

    if (!exprProps_.varProps().empty() && exprProps_.varProps().size() > 1) {
        return Status::SemanticError("Only one variable allowed to use.");
    }

    auto status = deduceExprType(edgeKeyRef->srcid());
    NG_RETURN_IF_ERROR(status);

    status = deduceExprType(edgeKeyRef->dstid());
    NG_RETURN_IF_ERROR(status);

    status = deduceExprType(edgeKeyRef->rank());
    NG_RETURN_IF_ERROR(status);

    if (edgeKeyRef->srcid()->kind() == Expression::Kind::kVarProperty) {
        edgeKeyVar_ = static_cast<PropertyExpression*>(edgeKeyRef->srcid())->sym();
    } else if (edgeKeyRef->srcid()->kind() == Expression::Kind::kInputProperty) {
        edgeKeyVar_ = inputVarName_;
    }
    return Status::OK();
}

Status DeleteEdgesValidator::toPlan() {
    auto dedup = Dedup::make(qctx_, nullptr);
    dedup->setInputVar(edgeKeyVar_);

    auto *doNode = DeleteEdges::make(qctx_,
                                     dedup,
                                     vctx_->whichSpace().id,
                                     edgeKeyRefs_.front());
    root_ = doNode;
    tail_ = dedup;
    return Status::OK();
}

Status UpdateValidator::initProps() {
    spaceId_ = vctx_->whichSpace().id;
    insertable_ = sentence_->getInsertable();
    if (sentence_->getName() != nullptr) {
        name_ = *sentence_->getName();
    }
    NG_RETURN_IF_ERROR(getUpdateProps());
    NG_RETURN_IF_ERROR(getCondition());
    return getReturnProps();
}

Status UpdateValidator::getCondition() {
    auto *clause = sentence_->whenClause();
    if (clause && clause->filter()) {
        auto filter = clause->filter()->clone();
        bool hasWrongType = false;
        auto symExpr = rewriteSymExpr(filter, name_, hasWrongType, isEdge_);
        if (hasWrongType) {
            return Status::SemanticError("Has wrong expr in `%s'",
                                         filter->toString().c_str());
        }
        if (symExpr != nullptr) {
            filter = symExpr;
        }
        auto typeStatus = deduceExprType(filter);
        NG_RETURN_IF_ERROR(typeStatus);
        auto type = typeStatus.value();
        if (type != Value::Type::BOOL
            && type != Value::Type::NULLVALUE
            && type != Value::Type::__EMPTY__) {
            std::stringstream ss;
            ss << "`" << filter->toString() << "', expected Boolean, but was `" << type << "'";
            return Status::SemanticError(ss.str());
        }
        condition_ = filter->encode();
    }
    return Status::OK();
}

Status UpdateValidator::getReturnProps() {
    auto *clause = sentence_->yieldClause();
    if (clause != nullptr) {
        auto yields = clause->columns();
        for (auto *col : yields) {
            yieldColNames_.emplace_back(col->name());
            std::string encodeStr;
            auto copyColExpr = col->expr()->clone();
            NG_LOG_AND_RETURN_IF_ERROR(checkAndResetSymExpr(copyColExpr, name_, encodeStr));
            returnProps_.emplace_back(std::move(encodeStr));
        }
    }
    return Status::OK();
}

Status UpdateValidator::getUpdateProps() {
    auto status = Status::OK();
    auto items = sentence_->updateList()->items();
    std::unordered_set<std::string> symNames;
    std::string fieldName;
    std::string symName;
    for (auto& item : items) {
        storage::cpp2::UpdatedProp updatedProp;
        // The syntax has guaranteed it is name or expression
        if (item->getFieldName() != nullptr) {
            symName = name_;
            fieldName = *item->getFieldName();
            symNames.emplace(name_);
        }
        if (item->getFieldExpr() != nullptr) {
            DCHECK(item->getFieldExpr()->kind() == Expression::Kind::kLabelAttribute);
            auto laExpr = static_cast<const LabelAttributeExpression*>(item->getFieldExpr());
            symNames.emplace(laExpr->left()->name());
            symName = laExpr->left()->name();
            const auto &value = laExpr->right()->value();
            fieldName = value.getStr();
        }
        auto valueExpr = item->value();
        if (valueExpr == nullptr) {
            LOG(ERROR) << "valueExpr is nullptr";
            return Status::SyntaxError("Empty update item field value.");
        }
        std::string encodeStr;
        auto copyValueExpr = valueExpr->clone();
        NG_LOG_AND_RETURN_IF_ERROR(checkAndResetSymExpr(copyValueExpr, symName, encodeStr));
        updatedProp.set_value(std::move(encodeStr));
        updatedProp.set_name(fieldName);
        updatedProps_.emplace_back(std::move(updatedProp));
    }

    if (symNames.size() != 1) {
        auto errorMsg = "Multi schema name: " + folly::join(",", symNames);
        LOG(ERROR) << errorMsg;
        return Status::SemanticError(std::move(errorMsg));
    }
    if (!symName.empty()) {
        name_ = symName;
    }
    return status;
}


Status UpdateValidator::checkAndResetSymExpr(Expression* inExpr,
                                             const std::string& symName,
                                             std::string &encodeStr) {
    bool hasWrongType = false;
    auto symExpr = rewriteSymExpr(inExpr, symName, hasWrongType, isEdge_);
    if (hasWrongType) {
        return Status::SemanticError("Has wrong expr in `%s'",
                             inExpr->toString().c_str());
    }
    if (symExpr != nullptr) {
        encodeStr = symExpr->encode();
        return Status::OK();
    }
    encodeStr = inExpr->encode();
    return Status::OK();
}

// rewrite the expr which has kSymProperty expr to toExpr
Expression *UpdateValidator::rewriteSymExpr(Expression *expr,
                                            const std::string &sym,
                                            bool &hasWrongType,
                                            bool isEdge) {
    RewriteSymExprVisitor visitor(qctx_->objPool(), sym, isEdge);
    expr->accept(&visitor);
    hasWrongType = visitor.hasWrongType();
    return std::move(visitor).expr();
}

Status UpdateVertexValidator::validateImpl() {
    auto sentence = static_cast<UpdateVertexSentence*>(sentence_);
    auto idRet = SchemaUtil::toVertexID(sentence->getVid(), vidType_);
    if (!idRet.ok()) {
        LOG(ERROR) << idRet.status();
        return idRet.status();
    }
    vId_ = std::move(idRet).value();
    NG_RETURN_IF_ERROR(initProps());
    auto ret = qctx_->schemaMng()->toTagID(spaceId_, name_);
    if (!ret.ok()) {
        LOG(ERROR) << "No schema found for " << name_;
        return Status::SemanticError("No schema found for `%s'", name_.c_str());
    }
    tagId_ = ret.value();
    return Status::OK();
}

Status UpdateVertexValidator::toPlan() {
    auto *update = UpdateVertex::make(qctx_,
                                      nullptr,
                                      spaceId_,
                                      std::move(name_),
                                      vId_,
                                      tagId_,
                                      insertable_,
                                      std::move(updatedProps_),
                                      std::move(returnProps_),
                                      std::move(condition_),
                                      std::move(yieldColNames_));
    root_ = update;
    tail_ = root_;
    return Status::OK();
}

Status UpdateEdgeValidator::validateImpl() {
    auto sentence = static_cast<UpdateEdgeSentence*>(sentence_);
    auto srcIdRet = SchemaUtil::toVertexID(sentence->getSrcId(), vidType_);
    if (!srcIdRet.ok()) {
        LOG(ERROR) << srcIdRet.status();
        return srcIdRet.status();
    }
    srcId_ = std::move(srcIdRet).value();
    auto dstIdRet = SchemaUtil::toVertexID(sentence->getDstId(), vidType_);
    if (!dstIdRet.ok()) {
        LOG(ERROR) << dstIdRet.status();
        return dstIdRet.status();
    }
    dstId_ = std::move(dstIdRet).value();
    rank_ = sentence->getRank();
    NG_RETURN_IF_ERROR(initProps());
    auto ret = qctx_->schemaMng()->toEdgeType(spaceId_, name_);
    if (!ret.ok()) {
        LOG(ERROR) << "No schema found for " << name_;
        return Status::SemanticError("No schema found for `%s'", name_.c_str());
    }
    edgeType_ = ret.value();
    return Status::OK();
}

Status UpdateEdgeValidator::toPlan() {
    auto *outNode = UpdateEdge::make(qctx_,
                                     nullptr,
                                     spaceId_,
                                     name_,
                                     srcId_,
                                     dstId_,
                                     edgeType_,
                                     rank_,
                                     insertable_,
                                     updatedProps_,
                                     {},
                                     condition_,
                                     {});
    using IsoLevel = meta::cpp2::IsolationLevel;
    auto isoLevel = space_.spaceDesc.isolation_level_ref().value_or(IsoLevel::DEFAULT);
    auto useToss = isoLevel == IsoLevel::TOSS;
    if (useToss) {
        root_ = outNode;
        tail_ = root_;
    } else {
        auto *inNode = UpdateEdge::make(qctx_,
                                        outNode,
                                        spaceId_,
                                        std::move(name_),
                                        std::move(dstId_),
                                        std::move(srcId_),
                                        -edgeType_,
                                        rank_,
                                        insertable_,
                                        std::move(updatedProps_),
                                        std::move(returnProps_),
                                        std::move(condition_),
                                        std::move(yieldColNames_));
        root_ = inNode;
        tail_ = outNode;
    }
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula

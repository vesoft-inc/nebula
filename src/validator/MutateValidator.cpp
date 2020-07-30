/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "validator/MutateValidator.h"
#include "util/SchemaUtil.h"
#include "planner/Mutate.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
Status InsertVerticesValidator::validateImpl() {
    spaceId_ = vctx_->whichSpace().id;
    auto status = Status::OK();
    do {
        status = check();
        if (!status.ok()) {
            break;
        }
        status = prepareVertices();
        if (!status.ok()) {
            break;
        }
    } while (false);
    return status;
}

Status InsertVerticesValidator::toPlan() {
    auto *plan = qctx_->plan();
    auto doNode = InsertVertices::make(plan,
                                       nullptr,
                                       spaceId_,
                                       std::move(vertices_),
                                       std::move(tagPropNames_),
                                       overwritable_);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status InsertVerticesValidator::check() {
    auto sentence = static_cast<InsertVerticesSentence*>(sentence_);
    rows_ = sentence->rows();
    if (rows_.empty()) {
        return Status::Error("VALUES cannot be empty");
    }

    auto tagItems = sentence->tagItems();
    overwritable_ = sentence->overwritable();

    schemas_.reserve(tagItems.size());

    for (auto& item : tagItems) {
        auto *tagName = item->tagName();
        auto tagStatus = qctx_->schemaMng()->toTagID(spaceId_, *tagName);
        if (!tagStatus.ok()) {
            LOG(ERROR) << "No schema found for " << *tagName;
            return Status::Error("No schema found for `%s'", tagName->c_str());
        }

        auto tagId = tagStatus.value();
        auto schema = qctx_->schemaMng()->getTagSchema(spaceId_, tagId);
        if (schema == nullptr) {
            LOG(ERROR) << "No schema found for " << *tagName;
            return Status::Error("No schema found for `%s'", tagName->c_str());
        }

        std::vector<std::string> names;
        auto props = item->properties();
        // Check prop name is in schema
        for (auto *it : props) {
            if (schema->getFieldIndex(*it) < 0) {
                LOG(ERROR) << "Unknown column `" << *it << "' in schema";
                return Status::Error("Unknown column `%s' in schema", it->c_str());
            }
            propSize_++;
            names.emplace_back(*it);
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
            return Status::Error("Wrong number of value");
        }
        auto idStatus = SchemaUtil::toVertexID(row->id());
        if (!idStatus.ok()) {
            return idStatus.status();
        }
        auto vertexId = std::move(idStatus).value();
        auto valsRet = SchemaUtil::toValueVec(row->values());
        if (!valsRet.ok()) {
            return valsRet.status();
        }
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
                auto schemaType = schema->getFieldType(propNames[index]);
                auto valueStatus = SchemaUtil::toSchemaValue(schemaType, values[handleValueNum]);
                if (!valueStatus.ok()) {
                    return valueStatus.status();
                }
                props.emplace_back(std::move(valueStatus).value());
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
    auto status = Status::OK();
    do {
        status = check();
        if (!status.ok()) {
            break;
        }

        status = prepareEdges();
        if (!status.ok()) {
            break;
        }
    } while (false);
    return status;
}

Status InsertEdgesValidator::toPlan() {
    auto *plan = qctx_->plan();
    auto doNode = InsertEdges::make(plan,
                                    nullptr,
                                    spaceId_,
                                    std::move(edges_),
                                    std::move(propNames_),
                                    overwritable_);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status InsertEdgesValidator::check() {
    auto sentence = static_cast<InsertEdgesSentence*>(sentence_);
    overwritable_ = sentence->overwritable();
    auto edgeStatus = qctx_->schemaMng()->toEdgeType(spaceId_, *sentence->edge());
    if (!edgeStatus.ok()) {
        return edgeStatus.status();
    }
    edgeType_ = edgeStatus.value();
    auto props = sentence->properties();
    rows_ = sentence->rows();

    schema_ = qctx_->schemaMng()->getEdgeSchema(spaceId_, edgeType_);
    if (schema_ == nullptr) {
        LOG(ERROR) << "No schema found for " << sentence->edge();
        return Status::Error("No schema found for `%s'", sentence->edge()->c_str());
    }

    // Check prop name is in schema
    for (auto *it : props) {
        if (schema_->getFieldIndex(*it) < 0) {
            LOG(ERROR) << "Unknown column `" << *it << "' in schema";
            return Status::Error("Unknown column `%s' in schema", it->c_str());
        }
        propNames_.emplace_back(*it);
    }

    return Status::OK();
}

Status InsertEdgesValidator::prepareEdges() {;
    edges_.reserve(rows_.size()*2);
    for (auto i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        if (propNames_.size() != row->values().size()) {
            return Status::Error("Wrong number of value");
        }
        auto idStatus = SchemaUtil::toVertexID(row->srcid());
        if (!idStatus.ok()) {
            return idStatus.status();
        }
        auto srcId = std::move(idStatus).value();
        idStatus = SchemaUtil::toVertexID(row->dstid());
        if (!idStatus.ok()) {
            return idStatus.status();
        }
        auto dstId = std::move(idStatus).value();

        int64_t rank = row->rank();

        auto valsRet = SchemaUtil::toValueVec(row->values());
        if (!valsRet.ok()) {
            return valsRet.status();
        }
        auto values = std::move(valsRet).value();
        std::vector<Value> props;
        for (auto index = 0u; index < propNames_.size(); index++) {
            auto schemaType = schema_->getFieldType(propNames_[index]);
            auto valueStatus = SchemaUtil::toSchemaValue(schemaType, values[index]);
            if (!valueStatus.ok()) {
                return valueStatus.status();
            }
            props.emplace_back(std::move(valueStatus).value());
        }

        // outbound
        storage::cpp2::NewEdge edge;
        edge.key.set_src(srcId);
        edge.key.set_dst(dstId);
        edge.key.set_ranking(rank);
        edge.key.set_edge_type(edgeType_);
        edge.set_props(std::move(props));
        edge.__isset.key = true;
        edge.__isset.props = true;
        edges_.emplace_back(edge);

        // inbound
        edge.key.set_src(dstId);
        edge.key.set_dst(srcId);
        edge.key.set_edge_type(-edgeType_);
        edges_.emplace_back(std::move(edge));
    }

    return Status::OK();
}

Status DeleteVerticesValidator::validateImpl() {
    auto sentence = static_cast<DeleteVerticesSentence*>(sentence_);
    spaceId_ = vctx_->whichSpace().id;
    if (sentence->isRef()) {
        vidRef_ = sentence->vidRef();
        auto type = deduceExprType(vidRef_);
        if (!type.ok()) {
            return type.status();
        }
        if (type.value() != Value::Type::STRING) {
            std::stringstream ss;
            ss << "The vid should be string type, "
               << "but input is `" << type.value() << "'";
            return Status::Error(ss.str());
        }
    } else {
        auto vIds = sentence->vidList()->vidList();
        for (auto vId : vIds) {
            auto idStatus = SchemaUtil::toVertexID(vId);
            if (!idStatus.ok()) {
                return idStatus.status();
            }
            vertices_.emplace_back(std::move(idStatus).value());
        }
    }

    auto ret = qctx_->schemaMng()->getAllEdge(spaceId_);
    if (!ret.ok()) {
        return ret.status();
    }
    edgeNames_ = std::move(ret).value();
    for (auto &name : edgeNames_) {
        auto edgeStatus = qctx_->schemaMng()->toEdgeType(spaceId_, name);
        if (!edgeStatus.ok()) {
            return edgeStatus.status();
        }
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

    auto* vIds = new VariablePropertyExpression(
            new std::string(input),
            new std::string(kVid));
    qctx_->plan()->saveObject(vIds);
    vidRef_ = vIds;
    return input;
}

Status DeleteVerticesValidator::toPlan() {
    auto plan = qctx_->plan();
    std::string vidVar;
    if (!vertices_.empty() && vidRef_ == nullptr) {
        vidVar = buildVIds();
    } else if (vidRef_ != nullptr && vidRef_->kind() == Expression::Kind::kVarProperty) {
        vidVar = *static_cast<SymbolPropertyExpression*>(vidRef_)->sym();
    }

    std::vector<storage::cpp2::EdgeProp> edgeProps;
    // make edgeRefs and edgeProp
    auto index = 0u;
    DCHECK(edgeTypes_.size() == edgeNames_.size());
    for (auto &name : edgeNames_) {
        auto *edgeKeyRef = new EdgeKeyRef(
                new EdgeSrcIdExpression(new std::string(name)),
                new EdgeDstIdExpression(new std::string(name)),
                new EdgeRankExpression(new std::string(name)));
        edgeKeyRef->setType(new EdgeTypeExpression(new std::string(name)));
        qctx_->plan()->saveObject(edgeKeyRef);
        edgeKeyRefs_.emplace_back(edgeKeyRef);

        storage::cpp2::EdgeProp edgeProp;
        edgeProp.set_type(edgeTypes_[index]);
        edgeProp.props.emplace_back(kSrc);
        edgeProp.props.emplace_back(kDst);
        edgeProp.props.emplace_back(kType);
        edgeProp.props.emplace_back(kRank);
        edgeProps.emplace_back(edgeProp);

        edgeProp.set_type(-edgeTypes_[index]);
        edgeProps.emplace_back(std::move(edgeProp));
        index++;
    }

    auto vertexPropsPtr = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    auto edgePropsPtr = std::make_unique<std::vector<storage::cpp2::EdgeProp>>(edgeProps);
    auto statPropsPtr = std::make_unique<std::vector<storage::cpp2::StatProp>>();
    auto exprPtr = std::make_unique<std::vector<storage::cpp2::Expr>>();
    auto* getNeighbors = GetNeighbors::make(plan,
                                            nullptr,
                                            spaceId_,
                                            vidRef_,
                                            edgeTypes_,
                                            storage::cpp2::EdgeDirection::BOTH,
                                            nullptr,
                                            std::move(edgePropsPtr),
                                            std::move(statPropsPtr),
                                            std::move(exprPtr));
    getNeighbors->setInputVar(vidVar);

    // create deleteEdges node
    auto *deNode = DeleteEdges::make(plan,
                                     getNeighbors,
                                     spaceId_,
                                     std::move(edgeKeyRefs_));

    deNode->setInputVar(getNeighbors->varName());

    auto *dvNode = DeleteVertices::make(plan,
                                        deNode,
                                        spaceId_,
                                        vidRef_);

    dvNode->setInputVar(vidVar);
    root_ = dvNode;
    tail_ = getNeighbors;
    return Status::OK();
}

Status DeleteEdgesValidator::validateImpl() {
    auto sentence = static_cast<DeleteEdgesSentence*>(sentence_);
    auto spaceId = vctx_->whichSpace().id;
    auto edgeStatus = qctx_->schemaMng()->toEdgeType(spaceId, *sentence->edge());
    if (!edgeStatus.ok()) {
        return edgeStatus.status();
    }
    auto edgeType = edgeStatus.value();
    if (sentence->isRef()) {
        edgeKeyRefs_.emplace_back(sentence->edgeKeyRef());
        (*edgeKeyRefs_.begin())->setType(new ConstantExpression(edgeType));
        auto status = checkInput();
        if (!status.ok()) {
            return status;
        }
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
        auto srcIdStatus = SchemaUtil::toVertexID(edgeKey->srcid());
        if (!srcIdStatus.ok()) {
            return srcIdStatus.status();
        }
        auto dstIdStatus = SchemaUtil::toVertexID(edgeKey->dstid());
        if (!dstIdStatus.ok()) {
            return dstIdStatus.status();
        }

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

    auto* srcIdExpr = new InputPropertyExpression(new std::string(kSrc));
    auto* typeExpr = new InputPropertyExpression(new std::string(kType));
    auto* rankExpr = new InputPropertyExpression(new std::string(kRank));
    auto* dstIdExpr = new InputPropertyExpression(new std::string(kDst));
    auto* edgeKeyRef = new EdgeKeyRef(srcIdExpr, dstIdExpr, rankExpr);
    edgeKeyRef->setType(typeExpr);
    qctx_->plan()->saveObject(edgeKeyRef);

    edgeKeyRefs_.emplace_back(edgeKeyRef);
    return Status::OK();
}

Status DeleteEdgesValidator::checkInput() {
    CHECK(!edgeKeyRefs_.empty());
    auto &edgeKeyRef = *edgeKeyRefs_.begin();
    NG_LOG_AND_RETURN_IF_ERROR(deduceProps(edgeKeyRef->srcid()));
    NG_LOG_AND_RETURN_IF_ERROR(deduceProps(edgeKeyRef->dstid()));
    NG_LOG_AND_RETURN_IF_ERROR(deduceProps(edgeKeyRef->rank()));

    if (!srcTagProps_.empty() || !dstTagProps_.empty() || !edgeProps_.empty()) {
        return Status::SyntaxError("Only support input and variable.");
    }

    if (!inputProps_.empty() && !varProps_.empty()) {
        return Status::Error("Not support both input and variable.");
    }

    if (!varProps_.empty() && varProps_.size() > 1) {
        return Status::Error("Only one variable allowed to use.");
    }

    auto status = deduceExprType(edgeKeyRef->srcid());
    NG_RETURN_IF_ERROR(status);

    status = deduceExprType(edgeKeyRef->dstid());
    NG_RETURN_IF_ERROR(status);

    status = deduceExprType(edgeKeyRef->rank());
    NG_RETURN_IF_ERROR(status);

    if (edgeKeyRef->srcid()->kind() == Expression::Kind::kVarProperty) {
        edgeKeyVar_ = *static_cast<SymbolPropertyExpression*>(edgeKeyRef->srcid())->sym();
    }
    return Status::OK();
}

Status DeleteEdgesValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *doNode = DeleteEdges::make(plan,
                                     nullptr,
                                     vctx_->whichSpace().id,
                                     edgeKeyRefs_);
    doNode->setInputVar(edgeKeyVar_);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula

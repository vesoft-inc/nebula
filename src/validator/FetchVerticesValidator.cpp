/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "validator/FetchVerticesValidator.h"
#include "planner/Query.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"
#include "visitor/DeducePropsVisitor.h"

namespace nebula {
namespace graph {

static constexpr char VertexID[] = "VertexID";

Status FetchVerticesValidator::validateImpl() {
    NG_RETURN_IF_ERROR(check());
    NG_RETURN_IF_ERROR(prepareVertices());
    NG_RETURN_IF_ERROR(prepareProperties());
    return Status::OK();
}

Status FetchVerticesValidator::toPlan() {
    // Start [-> some input] -> GetVertices [-> Project] [-> Dedup] [-> next stage] -> End
    std::string vidsVar = (srcRef_ == nullptr ? buildConstantInput() : buildRuntimeInput());
    auto *getVerticesNode = GetVertices::make(qctx_,
                                              nullptr,
                                              space_.id,
                                              src_,
                                              std::move(props_),
                                              std::move(exprs_),
                                              dedup_,
                                              std::move(orderBy_),
                                              limit_,
                                              std::move(filter_));
    getVerticesNode->setInputVar(vidsVar);
    getVerticesNode->setColNames(std::move(gvColNames_));
    // pipe will set the input variable
    PlanNode *current = getVerticesNode;

    if (withProject_) {
        auto *projectNode = Project::make(qctx_, current, newYieldColumns_);
        projectNode->setInputVar(current->outputVar());
        projectNode->setColNames(colNames_);
        current = projectNode;
    }
    // Project select properties then dedup
    if (dedup_) {
        auto *dedupNode = Dedup::make(qctx_, current);
        dedupNode->setInputVar(current->outputVar());
        dedupNode->setColNames(colNames_);
        current = dedupNode;

        // the framework will add data collect to collect the result
        // if the result is required
    }
    root_ = current;
    tail_ = getVerticesNode;
    return Status::OK();
}

Status FetchVerticesValidator::check() {
    auto *sentence = static_cast<FetchVerticesSentence *>(sentence_);

    if (!sentence->isAllTagProps()) {
        onStar_ = false;
        auto tagName = *(sentence->tag());
        auto tagStatus = qctx_->schemaMng()->toTagID(space_.id, tagName);
        NG_RETURN_IF_ERROR(tagStatus);
        auto tagId = tagStatus.value();

        tags_.emplace(tagName, tagId);
        auto schema = qctx_->schemaMng()->getTagSchema(space_.id, tagId);
        if (schema == nullptr) {
            LOG(ERROR) << "No schema found for " << tagName;
            return Status::Error("No schema found for `%s'", tagName.c_str());
        }
        tagsSchema_.emplace(tagId, schema);
    } else {
        onStar_ = true;
        const auto allTagsResult = qctx_->schemaMng()->getAllVerTagSchema(space_.id);
        NG_RETURN_IF_ERROR(allTagsResult);
        const auto allTags = std::move(allTagsResult).value();
        for (const auto &tag : allTags) {
            tagsSchema_.emplace(tag.first, tag.second.back());
        }
        for (const auto &tagSchema : tagsSchema_) {
            auto tagNameResult = qctx_->schemaMng()->toTagName(space_.id, tagSchema.first);
            NG_RETURN_IF_ERROR(tagNameResult);
            tags_.emplace(std::move(tagNameResult).value(), tagSchema.first);
        }
    }
    return Status::OK();
}

Status FetchVerticesValidator::prepareVertices() {
    auto *sentence = static_cast<FetchVerticesSentence *>(sentence_);
    // from ref, eval when execute
    if (sentence->isRef()) {
        srcRef_ = sentence->ref();
        auto result = checkRef(srcRef_, Value::Type::STRING);
        NG_RETURN_IF_ERROR(result);
        inputVar_ = std::move(result).value();
        return Status::OK();
    }

    // from constant, eval now
    // TODO(shylock) add eval() method for expression
    QueryExpressionContext dummy(nullptr);
    auto vids = sentence->vidList();
    srcVids_.rows.reserve(vids.size());
    for (const auto vid : vids) {
        DCHECK(ExpressionUtils::isConstExpr(vid));
        auto v = vid->eval(dummy);
        if (!SchemaUtil::isValidVid(v, space_.spaceDesc.vid_type)) {
            return Status::NotSupported("Not a vertex id");
        }
        srcVids_.emplace_back(nebula::Row({std::move(v).getStr()}));
    }
    return Status::OK();
}

// TODO(shylock) select _vid property instead of return always.
Status FetchVerticesValidator::prepareProperties() {
    auto *sentence = static_cast<FetchVerticesSentence *>(sentence_);
    auto *yield = sentence->yieldClause();
    if (yield == nullptr) {
        return preparePropertiesWithoutYield();
    } else {
        return preparePropertiesWithYield(yield);
    }
}

Status FetchVerticesValidator::preparePropertiesWithYield(const YieldClause *yield) {
    withProject_ = true;
    // outputs
    auto yieldSize = yield->columns().size();
    colNames_.reserve(yieldSize + 1);
    outputs_.reserve(yieldSize + 1);
    colNames_.emplace_back(VertexID);
    gvColNames_.emplace_back(colNames_.back());
    outputs_.emplace_back(VertexID, Value::Type::STRING);   // kVid

    dedup_ = yield->isDistinct();
    ExpressionProps exprProps;
    DeducePropsVisitor deducePropsVisitor(qctx_, space_.id, &exprProps);
    for (auto col : yield->columns()) {
        if (col->expr()->kind() == Expression::Kind::kLabelAttribute) {
            auto laExpr = static_cast<LabelAttributeExpression *>(col->expr());
            col->setExpr(ExpressionUtils::rewriteLabelAttribute<TagPropertyExpression>(laExpr));
        } else {
            ExpressionUtils::rewriteLabelAttribute<TagPropertyExpression>(col->expr());
        }
        col->expr()->accept(&deducePropsVisitor);
        if (!deducePropsVisitor.ok()) {
            return std::move(deducePropsVisitor).status();
        }
        if (exprProps.hasInputVarProperty()) {
            return Status::Error("Unsupported input/variable property expression in yield.");
        }
        if (!exprProps.edgeProps().empty()) {
            return Status::Error("Unsupported edge property expression in yield.");
        }
        if (exprProps.hasSrcDstTagProperty()) {
            return Status::Error("Unsupported src/dst property expression in yield.");
        }

        colNames_.emplace_back(deduceColName(col));
        auto typeResult = deduceExprType(col->expr());
        NG_RETURN_IF_ERROR(typeResult);
        outputs_.emplace_back(colNames_.back(), typeResult.value());
        // TODO(shylock) think about the push-down expr
    }
    if (exprProps.tagProps().empty()) {
        return Status::Error("Unsupported empty tag property expression in yield.");
    }

    if (onStar_) {
        for (const auto &tag : exprProps.tagNameIds()) {
            if (tags_.find(tag.first) == tags_.end()) {
                return Status::SemanticError("Mismatched tag.");
            }
        }
    } else {
        if (tags_ != exprProps.tagNameIds()) {
            return Status::SemanticError("Mismatched tag.");
        }
    }
    for (const auto &tagNameId : exprProps.tagNameIds()) {
        storage::cpp2::VertexProp vProp;
        std::vector<std::string> props;
        props.reserve(exprProps.tagProps().at(tagNameId.second).size());
        vProp.set_tag(tagNameId.second);
        for (const auto &prop : exprProps.tagProps().at(tagNameId.second)) {
            props.emplace_back(prop.toString());
            gvColNames_.emplace_back(tagNameId.first + "." + prop.toString());
        }
        vProp.set_props(std::move(props));
        props_.emplace_back(std::move(vProp));
    }

    // insert the reserved properties expression be compatible with 1.0
    // TODO(shylock) select kVid from storage
    newYieldColumns_ = qctx_->objPool()->add(new YieldColumns());
    // note eval vid by input expression
    newYieldColumns_->addColumn(new YieldColumn(
        new InputPropertyExpression(new std::string(VertexID)), new std::string(VertexID)));
    for (auto col : yield->columns()) {
        newYieldColumns_->addColumn(col->clone().release());
    }
    return Status::OK();
}

Status FetchVerticesValidator::preparePropertiesWithoutYield() {
    // empty for all tag and properties
    props_.clear();
    outputs_.emplace_back(VertexID, Value::Type::STRING);
    colNames_.emplace_back(VertexID);
    gvColNames_.emplace_back(colNames_.back());
    for (const auto &tagSchema : tagsSchema_) {
        storage::cpp2::VertexProp vProp;
        vProp.set_tag(tagSchema.first);
        auto tagNameResult = qctx_->schemaMng()->toTagName(space_.id, tagSchema.first);
        NG_RETURN_IF_ERROR(tagNameResult);
        auto tagName = std::move(tagNameResult).value();
        for (std::size_t i = 0; i < tagSchema.second->getNumFields(); ++i) {
            outputs_.emplace_back(
                tagSchema.second->getFieldName(i),
                SchemaUtil::propTypeToValueType(tagSchema.second->getFieldType(i)));
            colNames_.emplace_back(tagName + "." + tagSchema.second->getFieldName(i));
            gvColNames_.emplace_back(colNames_.back());
        }
        props_.emplace_back(std::move(vProp));
    }
    return Status::OK();
}

// TODO(shylock) optimize dedup input when distinct given
std::string FetchVerticesValidator::buildConstantInput() {
    auto input = vctx_->anonVarGen()->getVar();
    qctx_->ectx()->setResult(input, ResultBuilder().value(Value(std::move(srcVids_))).finish());

    src_ = qctx_->objPool()->makeAndAdd<VariablePropertyExpression>(new std::string(input),
                                                                    new std::string(kVid));
    return input;
}

std::string FetchVerticesValidator::buildRuntimeInput() {
    src_ = DCHECK_NOTNULL(srcRef_);
    return inputVar_;
}

}   // namespace graph
}   // namespace nebula

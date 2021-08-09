/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "validator/FetchVerticesValidator.h"
#include "planner/plan/Query.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"
#include "visitor/DeducePropsVisitor.h"

namespace nebula {
namespace graph {

static constexpr char VertexID[] = "VertexID";

Status FetchVerticesValidator::validateImpl() {
    props_ = std::make_unique<std::vector<VertexProp>>();
    exprs_ = std::make_unique<std::vector<Expr>>();
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
    getVerticesNode->setColNames(gvColNames_);
    // pipe will set the input variable
    PlanNode *current = getVerticesNode;

    if (withYield_) {
        current = Project::make(qctx_, current, newYieldColumns_);

        // Project select properties then dedup
        if (dedup_) {
            current = Dedup::make(qctx_, current);

            // the framework will add data collect to collect the result
            // if the result is required
        }
    } else {
        auto *pool = qctx_->objPool();
        auto *columns = pool->add(new YieldColumns());
        columns->addColumn(new YieldColumn(VertexExpression::make(pool), "vertices_"));
        current = Project::make(qctx_, current, columns);
    }
    root_ = current;
    tail_ = getVerticesNode;
    return Status::OK();
}

Status FetchVerticesValidator::check() {
    auto *sentence = static_cast<FetchVerticesSentence *>(sentence_);

    if (!sentence->isAllTagProps()) {
        onStar_ = false;
        auto tags = sentence->tags()->labels();
        for (const auto &tag : tags) {
            auto tagStatus = qctx_->schemaMng()->toTagID(space_.id, *tag);
            NG_RETURN_IF_ERROR(tagStatus);
            auto tagId = tagStatus.value();

            tags_.emplace(*tag, tagId);
            auto schema = qctx_->schemaMng()->getTagSchema(space_.id, tagId);
            if (schema == nullptr) {
                LOG(ERROR) << "No schema found for " << *tag;
                return Status::SemanticError("No schema found for `%s'", tag->c_str());
            }
            tagsSchema_.emplace(tagId, schema);
        }
    } else {
        onStar_ = true;
        const auto allTagsResult = qctx_->schemaMng()->getAllLatestVerTagSchema(space_.id);
        NG_RETURN_IF_ERROR(allTagsResult);
        const auto allTags = std::move(allTagsResult).value();
        for (const auto &tag : allTags) {
            tagsSchema_.emplace(tag.first, tag.second);
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
    if (sentence->vertices()->isRef()) {
        srcRef_ = sentence->vertices()->ref();
        auto result = checkRef(srcRef_, vidType_);
        NG_RETURN_IF_ERROR(result);
        inputVar_ = std::move(result).value();
        return Status::OK();
    }

    // from constant, eval now
    // TODO(shylock) add eval() method for expression
    QueryExpressionContext dummy(nullptr);
    auto vids = sentence->vertices()->vidList();
    srcVids_.rows.reserve(vids.size());
    for (const auto vid : vids) {
        DCHECK(ExpressionUtils::isConstExpr(vid));
        auto v = vid->eval(dummy);
        if (v.type() != vidType_) {
            std::stringstream ss;
            ss << "`" << vid->toString() << "', the vid should be type of " << vidType_
               << ", but was`" << v.type() << "'";
            return Status::SemanticError(ss.str());
        }
        srcVids_.emplace_back(nebula::Row({std::move(v)}));
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
    withYield_ = true;
    // outputs
    auto yieldSize = yield->columns().size();
    outputs_.reserve(yieldSize + 1);
    gvColNames_.emplace_back(nebula::kVid);
    outputs_.emplace_back(VertexID, vidType_);   // kVid

    dedup_ = yield->isDistinct();
    ExpressionProps exprProps;
    DeducePropsVisitor deducePropsVisitor(qctx_, space_.id, &exprProps, &userDefinedVarNameList_);

    auto* pool = qctx_->objPool();
    for (auto col : yield->columns()) {
        col->setExpr(ExpressionUtils::rewriteLabelAttr2TagProp(col->expr()));
        NG_RETURN_IF_ERROR(invalidLabelIdentifiers(col->expr()));
        col->expr()->accept(&deducePropsVisitor);
        if (!deducePropsVisitor.ok()) {
            return std::move(deducePropsVisitor).status();
        }
        if (exprProps.hasInputVarProperty()) {
            return Status::SemanticError(
                "Unsupported input/variable property expression in yield.");
        }
        if (!exprProps.edgeProps().empty()) {
            return Status::SemanticError("Unsupported edge property expression in yield.");
        }
        if (exprProps.hasSrcDstTagProperty()) {
            return Status::SemanticError("Unsupported src/dst property expression in yield.");
        }

        auto typeResult = deduceExprType(col->expr());
        NG_RETURN_IF_ERROR(typeResult);
        outputs_.emplace_back(col->name(), typeResult.value());
        // TODO(shylock) think about the push-down expr
    }
    if (exprProps.tagProps().empty()) {
        return Status::SemanticError("Unsupported empty tag property expression in yield.");
    }

    for (const auto &tag : exprProps.tagNameIds()) {
        if (tags_.find(tag.first) == tags_.end()) {
            return Status::SemanticError("Mismatched tag: %s", tag.first.c_str());
        }
    }
    for (const auto &tagNameId : exprProps.tagNameIds()) {
        storage::cpp2::VertexProp vProp;
        std::vector<std::string> propNames;
        propNames.reserve(exprProps.tagProps().at(tagNameId.second).size());
        vProp.set_tag(tagNameId.second);
        for (const auto &prop : exprProps.tagProps().at(tagNameId.second)) {
            propNames.emplace_back(prop.toString());
            gvColNames_.emplace_back(tagNameId.first + "." + prop.toString());
        }
        vProp.set_props(std::move(propNames));
        props_->emplace_back(std::move(vProp));
    }

    // insert the reserved properties expression be compatible with 1.0
    // TODO(shylock) select kVid from storage
    newYieldColumns_ = qctx_->objPool()->add(new YieldColumns());
    // note eval vid by input expression
    newYieldColumns_->addColumn(
        new YieldColumn(InputPropertyExpression::make(pool, nebula::kVid), VertexID));
    for (auto col : yield->columns()) {
        newYieldColumns_->addColumn(col->clone().release());
    }
    return Status::OK();
}

Status FetchVerticesValidator::preparePropertiesWithoutYield() {
    props_->clear();
    outputs_.emplace_back("vertices_", Value::Type::VERTEX);
    gvColNames_.emplace_back(nebula::kVid);
    for (const auto &tagSchema : tagsSchema_) {
        storage::cpp2::VertexProp vProp;
        vProp.set_tag(tagSchema.first);
        std::vector<std::string> propNames;
        propNames.reserve(tagSchema.second->getNumFields() + 1);
        auto tagNameResult = qctx_->schemaMng()->toTagName(space_.id, tagSchema.first);
        NG_RETURN_IF_ERROR(tagNameResult);
        auto tagName = std::move(tagNameResult).value();
        for (std::size_t i = 0; i < tagSchema.second->getNumFields(); ++i) {
            const auto propName = tagSchema.second->getFieldName(i);
            propNames.emplace_back(propName);
            gvColNames_.emplace_back(tagName + "." + propName);
        }
        gvColNames_.emplace_back(tagName + "._tag");
        propNames.emplace_back(nebula::kTag);   // "_tag"
        vProp.set_props(std::move(propNames));
        props_->emplace_back(std::move(vProp));
    }
    return Status::OK();
}

// TODO(shylock) optimize dedup input when distinct given
std::string FetchVerticesValidator::buildConstantInput() {
    auto input = vctx_->anonVarGen()->getVar();
    qctx_->ectx()->setResult(input, ResultBuilder().value(Value(std::move(srcVids_))).finish());

    auto *pool = qctx_->objPool();
    src_ = VariablePropertyExpression::make(pool, input, kVid);
    return input;
}

std::string FetchVerticesValidator::buildRuntimeInput() {
    src_ = DCHECK_NOTNULL(srcRef_);
    return inputVar_;
}

}   // namespace graph
}   // namespace nebula

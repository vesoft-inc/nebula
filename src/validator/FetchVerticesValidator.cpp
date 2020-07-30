/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "validator/FetchVerticesValidator.h"
#include "planner/Query.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"

namespace nebula {
namespace graph {

Status FetchVerticesValidator::validateImpl() {
    NG_RETURN_IF_ERROR(check());
    NG_RETURN_IF_ERROR(prepareVertices());
    NG_RETURN_IF_ERROR(prepareProperties());
    return Status::OK();
}

Status FetchVerticesValidator::toPlan() {
    // Start [-> some input] -> GetVertices [-> Project] [-> Dedup] [-> next stage] -> End
    auto *sentence = static_cast<FetchVerticesSentence*>(sentence_);
    auto *plan = qctx_->plan();
    auto *getVerticesNode = GetVertices::make(plan,
                                              nullptr,
                                              spaceId_,
                                              std::move(vertices_),
                                              std::move(src_),
                                              std::move(props_),
                                              std::move(exprs_),
                                              dedup_,
                                              std::move(orderBy_),
                                              limit_,
                                              std::move(filter_));
    getVerticesNode->setInputVar(inputVar_);
    // pipe will set the input variable
    PlanNode *current = getVerticesNode;

    if (withProject_) {
        auto *projectNode = Project::make(plan, current, sentence->yieldClause()->yields());
        projectNode->setInputVar(current->varName());
        projectNode->setColNames(colNames_);
        current = projectNode;
    }
    // Project select properties then dedup
    if (dedup_) {
        auto *dedupNode = Dedup::make(plan, current);
        dedupNode->setInputVar(current->varName());
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
    auto *sentence = static_cast<FetchVerticesSentence*>(sentence_);
    spaceId_ = vctx_->whichSpace().id;

    tagName_ = *sentence->tag();
    if (!sentence->isAllTagProps()) {
        tagName_ = *(sentence->tag());
        auto tagStatus = qctx_->schemaMng()->toTagID(spaceId_, tagName_);
        NG_RETURN_IF_ERROR(tagStatus);

        tagId_ = tagStatus.value();
        schema_ = qctx_->schemaMng()->getTagSchema(spaceId_, tagId_.value());
        if (schema_ == nullptr) {
            LOG(ERROR) << "No schema found for " << tagName_;
            return Status::Error("No schema found for `%s'", tagName_.c_str());
        }
    }
    return Status::OK();
}

Status FetchVerticesValidator::prepareVertices() {
    auto *sentence = static_cast<FetchVerticesSentence*>(sentence_);
    // from ref, eval when execute
    if (sentence->isRef()) {
        src_ = sentence->ref();
        auto result = checkRef(src_, Value::Type::STRING);
        NG_RETURN_IF_ERROR(result);
        inputVar_ = std::move(result).value();
        return Status::OK();
    }

    // from constant, eval now
    // TODO(shylock) add eval() method for expression
    QueryExpressionContext dummy(nullptr);
    auto vids = sentence->vidList();
    vertices_.reserve(vids.size());
    for (const auto vid : vids) {
        // TODO(shylock) Add a new value type VID to semantic this
        DCHECK(ExpressionUtils::isConstExpr(vid));
        auto v = vid->eval(dummy);
        if (!v.isStr()) {   // string as vid
            return Status::NotSupported("Not a vertex id");
        }
        vertices_.emplace_back(nebula::Row({std::move(v).getStr()}));
    }
    return Status::OK();
}

// TODO(shylock) select _vid property instead of return always.
Status FetchVerticesValidator::prepareProperties() {
    auto *sentence = static_cast<FetchVerticesSentence*>(sentence_);
    auto *yield = sentence->yieldClause();
    if (yield == nullptr) {
        // empty for all tag and properties
        props_.clear();
        if (!sentence->isAllTagProps()) {
            // for one tag all properties
            storage::cpp2::VertexProp prop;
            prop.set_tag(tagId_.value());
            // empty for all
            props_.emplace_back(std::move(prop));
            outputs_.emplace_back(kVid, Value::Type::STRING);
            colNames_.emplace_back(kVid);
            for (std::size_t i = 0; i < schema_->getNumFields(); ++i) {
                outputs_.emplace_back(schema_->getFieldName(i),
                                      SchemaUtil::propTypeToValueType(schema_->getFieldType(i)));
                colNames_.emplace_back(schema_->getFieldName(i));
            }
        } else {
            // all schema properties
            const auto allTagsResult = qctx_->schemaMng()->getAllVerTagSchema(spaceId_);
            NG_RETURN_IF_ERROR(allTagsResult);
            const auto allTags = std::move(allTagsResult).value();
            std::vector<std::pair<TagID, std::shared_ptr<const meta::NebulaSchemaProvider>>>
                allTagsSchema;
            allTagsSchema.reserve(allTags.size());
            for (const auto &tag : allTags) {
                allTagsSchema.emplace_back(tag.first, tag.second.back());
            }
            std::sort(allTagsSchema.begin(), allTagsSchema.end(), [](const auto &a, const auto &b) {
                return a.first < b.first;
            });
            outputs_.emplace_back(kVid, Value::Type::STRING);
            colNames_.emplace_back(kVid);
            for (const auto &tagSchema : allTagsSchema) {
                for (std::size_t i = 0; i < tagSchema.second->getNumFields(); ++i) {
                    outputs_.emplace_back(
                        tagSchema.second->getFieldName(i),
                        SchemaUtil::propTypeToValueType(tagSchema.second->getFieldType(i)));
                    colNames_.emplace_back(tagSchema.second->getFieldName(i));
                }
            }
        }
    } else {
        CHECK(!sentence->isAllTagProps()) << "Not supported yield for *.";
        withProject_ = true;
        dedup_ = yield->isDistinct();
        storage::cpp2::VertexProp prop;
        prop.set_tag(tagId_.value());
        std::vector<std::string> propsName;
        propsName.reserve(yield->columns().size());
        for (auto col : yield->columns()) {
            if (col->expr()->kind() == Expression::Kind::kSymProperty) {
                auto symbolExpr = static_cast<SymbolPropertyExpression *>(col->expr());
                col->setExpr(ExpressionUtils::transSymbolPropertyExpression<TagPropertyExpression>(
                    symbolExpr));
            } else {
                ExpressionUtils::transAllSymbolPropertyExpr<TagPropertyExpression>(col->expr());
            }
            const auto *invalidExpr = findInvalidYieldExpression(col->expr());
            if (invalidExpr != nullptr) {
                return Status::Error("Invalid yield expression `%s'.",
                                     col->expr()->toString().c_str());
            }
            // The properties from storage directly push down only
            // The other will be computed in Project Executor
            const auto storageExprs = ExpressionUtils::findAllStorage(col->expr());
            for (const auto &storageExpr : storageExprs) {
                const auto *expr = static_cast<const SymbolPropertyExpression *>(storageExpr);
                if (*expr->sym() != tagName_) {
                    return Status::Error("Mismatched tag name");
                }
                // Check is prop name in schema
                if (schema_->getFieldIndex(*expr->prop()) < 0) {
                    LOG(ERROR) << "Unknown column `" << *expr->prop() << "' in tag `"
                                << tagName_ << "'.";
                    return Status::Error("Unknown column `%s' in tag `%s'.",
                                            expr->prop()->c_str(),
                                            tagName_.c_str());
                }
                propsName.emplace_back(*expr->prop());
            }
            // TODO(shylock) think about the push-down expr
        }
        prop.set_props(std::move(propsName));
        props_.emplace_back(std::move(prop));

        // outputs
        colNames_ = deduceColNames(yield->yields());
        outputs_.reserve(colNames_.size());
        for (std::size_t i = 0; i < colNames_.size(); ++i) {
            auto typeResult = deduceExprType(yield->columns()[i]->expr());
            NG_RETURN_IF_ERROR(typeResult);
            outputs_.emplace_back(colNames_[i], typeResult.value());
        }
    }

    return Status::OK();
}

/*static*/
const Expression *FetchVerticesValidator::findInvalidYieldExpression(const Expression *root) {
    return ExpressionUtils::findAnyKind(root,
                                        {Expression::Kind::kInputProperty,
                                         Expression::Kind::kVarProperty,
                                         Expression::Kind::kEdgeSrc,
                                         Expression::Kind::kEdgeType,
                                         Expression::Kind::kEdgeRank,
                                         Expression::Kind::kEdgeDst});
}

}   // namespace graph
}   // namespace nebula

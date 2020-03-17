/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "parser/TraverseSentences.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "graph/GoExecutor.h"
#include "graph/PipeExecutor.h"
#include "graph/OrderByExecutor.h"
#include "graph/FetchVerticesExecutor.h"
#include "graph/FetchEdgesExecutor.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "graph/SetExecutor.h"
#include "graph/LookupExecutor.h"
#include "graph/MatchExecutor.h"
#include "graph/FindPathExecutor.h"
#include "graph/LimitExecutor.h"
#include "graph/YieldExecutor.h"
#include "graph/GroupByExecutor.h"
#include "graph/SchemaHelper.h"
#include "graph/DeleteVerticesExecutor.h"
#include "graph/DeleteEdgesExecutor.h"

namespace nebula {
namespace graph {

std::unique_ptr<TraverseExecutor> TraverseExecutor::makeTraverseExecutor(Sentence *sentence) {
    return makeTraverseExecutor(sentence, ectx());
}


// static
std::unique_ptr<TraverseExecutor>
TraverseExecutor::makeTraverseExecutor(Sentence *sentence, ExecutionContext *ectx) {
    auto kind = sentence->kind();
    std::unique_ptr<TraverseExecutor> executor;
    switch (kind) {
        case Sentence::Kind::kGo:
            executor = std::make_unique<GoExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kPipe:
            executor = std::make_unique<PipeExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kOrderBy:
            executor = std::make_unique<OrderByExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kFetchVertices:
            executor = std::make_unique<FetchVerticesExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kFetchEdges:
            executor = std::make_unique<FetchEdgesExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kSet:
            executor = std::make_unique<SetExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kMatch:
            executor = std::make_unique<MatchExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kLookup:
            executor = std::make_unique<LookupExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kYield:
            executor = std::make_unique<YieldExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kFindPath:
            executor = std::make_unique<FindPathExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kLimit:
            executor = std::make_unique<LimitExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::KGroupBy:
            executor = std::make_unique<GroupByExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kDeleteVertices:
            executor = std::make_unique<DeleteVerticesExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kDeleteEdges:
            executor = std::make_unique<DeleteEdgesExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kUnknown:
            LOG(FATAL) << "Sentence kind unknown";
            break;
        default:
            LOG(FATAL) << "Sentence kind illegal: " << kind;
            break;
    }
    return executor;
}

nebula::cpp2::SupportedType TraverseExecutor::calculateExprType(Expression* exp) const {
    auto spaceId = ectx()->rctx()->session()->space();
    switch (exp->kind()) {
        case Expression::kPrimary:
        case Expression::kFunctionCall:
        case Expression::kUnary:
        case Expression::kArithmetic: {
            return nebula::cpp2::SupportedType::UNKNOWN;
        }
        case Expression::kTypeCasting: {
            auto exprPtr = static_cast<const TypeCastingExpression *>(exp);
            return SchemaHelper::columnTypeToSupportedType(
                                                    exprPtr->getType());
        }
        case Expression::kRelational:
        case Expression::kLogical: {
            return nebula::cpp2::SupportedType::BOOL;
        }
        case Expression::kDestProp:
        case Expression::kSourceProp: {
            auto* tagPropExp = static_cast<const AliasPropertyExpression*>(exp);
            const auto* tagName = tagPropExp->alias();
            const auto* propName = tagPropExp->prop();
            auto tagIdRet = ectx()->schemaManager()->toTagID(spaceId, *tagName);
            if (tagIdRet.ok()) {
                auto ts = ectx()->schemaManager()->getTagSchema(spaceId, tagIdRet.value());
                if (ts != nullptr) {
                    return ts->getFieldType(*propName).type;
                }
            }
            return nebula::cpp2::SupportedType::UNKNOWN;
        }
        case Expression::kEdgeDstId:
        case Expression::kEdgeSrcId: {
            return nebula::cpp2::SupportedType::VID;
        }
        case Expression::kEdgeRank:
        case Expression::kEdgeType: {
            return nebula::cpp2::SupportedType::INT;
        }
        case Expression::kAliasProp: {
            auto* edgeExp = static_cast<const AliasPropertyExpression*>(exp);
            const auto* propName = edgeExp->prop();
            auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId, *edgeExp->alias());
            if (edgeStatus.ok()) {
                auto edgeType = edgeStatus.value();
                auto schema = ectx()->schemaManager()->getEdgeSchema(spaceId, edgeType);
                if (schema != nullptr) {
                    return schema->getFieldType(*propName).type;
                }
            }
            return nebula::cpp2::SupportedType::UNKNOWN;
        }
        case Expression::kVariableProp:
        case Expression::kInputProp: {
            auto* propExp = static_cast<const AliasPropertyExpression*>(exp);
            const auto* propName = propExp->prop();
            if (inputs_ == nullptr) {
                return nebula::cpp2::SupportedType::UNKNOWN;
            } else {
                return inputs_->getColumnType(*propName);
            }
        }
        default: {
            VLOG(1) << "Unsupport expression type! kind = "
                    << std::to_string(static_cast<uint8_t>(exp->kind()));
            return nebula::cpp2::SupportedType::UNKNOWN;
        }
    }
}

Status TraverseExecutor::checkIfDuplicateColumn() const {
    if (inputs_ == nullptr) {
        return Status::OK();
    }

    auto colNames = inputs_->getColNames();
    std::unordered_set<std::string> uniqueNames;
    for (auto &colName : colNames) {
        auto ret = uniqueNames.emplace(colName);
        if (!ret.second) {
            return Status::Error("Duplicate column `%s'", colName.c_str());
        }
    }
    return Status::OK();
}

StatusOr<std::vector<VertexID>>
TraverseExecutor::getVids(ExpressionContext* expCtx,
        BaseVerticesSentence *sentence, bool distinct) const {
    std::vector <VertexID> vids;
    if (sentence->isRef()) {
        std::string *colName = nullptr;
        std::string *varName = nullptr;
        auto *expr = sentence->ref();
        if (expr->isInputExpression()) {
            auto *iexpr = static_cast<InputPropertyExpression *>(expr);
            colName = iexpr->prop();
        } else if (expr->isVariableExpression()) {
            auto *vexpr = static_cast<VariablePropertyExpression *>(expr);
            varName = vexpr->alias();
            colName = vexpr->prop();
        } else {
            //  should never come to here.
            //  only support input and variable yet.
            return Status::Error("Unknown kind of expression.");
        }
        if (colName != nullptr && *colName == "*") {
            return Status::Error("Cant not use `*' to reference a vertex id column.");
        }
        const InterimResult *inputs;
        if (varName == nullptr) {
            inputs = inputs_.get();
        } else {
            bool existing = false;
            inputs = ectx()->variableHolder()->get(*varName, &existing);
            if (!existing) {
                return Status::Error("Variable `%s' not defined", varName->c_str());
            }
        }
        if (inputs == nullptr || !inputs->hasData()) {
            return vids;
        }

        auto status = checkIfDuplicateColumn();
        if (!status.ok()) {
            return status;
        }
        StatusOr <std::vector<VertexID>> result;
        if (distinct) {
            result = inputs->getDistinctVIDs(*colName);
        } else {
            result = inputs->getVIDs(*colName);
        }
        if (!result.ok()) {
            return std::move(result).status();
        }
        vids = std::move(result).value();
    } else {
        if (expCtx == nullptr) {
            return Status::Error("ExpressionContext is nullptr");
        }
        std::unique_ptr <std::unordered_set<VertexID>> uniqID;
        if (distinct) {
            uniqID = std::make_unique<std::unordered_set<VertexID>> ();
        }

        auto vidList = sentence->vidList();
        Getters getters;
        for (auto *expr : vidList) {
            expr->setContext(expCtx);
            auto status = expr->prepare();
            if (!status.ok()) {
                return status;
            }
            auto value = expr->eval(getters);
            if (!value.ok()) {
                return std::move(value).status();
            }
            auto v = value.value();
            if (!Expression::isInt(v)) {
                return Status::Error("Vertex ID should be of type integer");
            }

            auto valInt = Expression::asInt(v);
            if (distinct) {
                auto result = uniqID->emplace(valInt);
                if (result.second) {
                    vids.emplace_back(valInt);
                }
            } else {
                vids.emplace_back(valInt);
            }
        }
    }
    return vids;
}

StatusOr<std::vector<storage::cpp2::EdgeKey>>
TraverseExecutor::getEdgeKeys(ExpressionContext* expCtx,
        BaseEdgesSentence *sentence, EdgeType edgeType, bool distinct) const {
    std::vector<storage::cpp2::EdgeKey> edgeKeys;
    using EdgeKeyHashSet = std::unordered_set<
            storage::cpp2::EdgeKey,
            std::function<size_t(const storage::cpp2::EdgeKey& key)>>;

    auto hash = [] (const storage::cpp2::EdgeKey &key) -> size_t {
        return std::hash<VertexID>()(key.src)
               ^ std::hash<VertexID>()(key.dst)
               ^ std::hash<EdgeRanking>()(key.ranking);
    };
    if (sentence->isRef()) {
        auto *edgeKeyRef = sentence->ref();
        auto srcId = edgeKeyRef->srcid();
        auto dstId = edgeKeyRef->dstid();
        auto rank = edgeKeyRef->rank();
        if (srcId == nullptr || dstId == nullptr) {
            return Status::Error("SrcId or dstId is nullptr.");
        }

        if ((*srcId == "*") || (*dstId == "*") || (rank != nullptr && *rank == "*")) {
            return Status::Error("Can not use `*' to reference a vertex id column.");
        }

        auto ret = edgeKeyRef->varname();
        if (!ret.ok()) {
            return std::move(ret).status();
        }
        auto varName = std::move(ret).value();
        const InterimResult *inputs;
        if (sentence->ref()->isInputExpr()) {
            inputs = inputs_.get();
        } else {
            bool existing = false;
            inputs = ectx()->variableHolder()->get(varName, &existing);
            if (!existing) {
                return Status::Error("Variable `%s' not defined", varName.c_str());
            }
        }
        if (inputs == nullptr || !inputs->hasData()) {
            // we have empty imputs from pipe.
            return edgeKeys;
        }

        auto status = checkIfDuplicateColumn();
        if (!status.ok()) {
            return status;
        }
        auto srcRet = inputs->getVIDs(*srcId);
        if (!srcRet.ok()) {
            return srcRet.status();
        }
        auto srcVids = std::move(srcRet).value();
        auto dstRet = inputs->getVIDs(*dstId);
        if (!dstRet.ok()) {
            return dstRet.status();
        }
        auto dstVids = std::move(dstRet).value();

        std::vector<EdgeRanking> ranks;
        if (rank != nullptr) {
            auto rankRet = inputs->getVIDs(*rank);
            if (!rankRet.ok()) {
                return rankRet.status();
            }
            ranks = std::move(rankRet).value();
        }

        std::unique_ptr<EdgeKeyHashSet> uniq;
        if (distinct) {
            uniq = std::make_unique<EdgeKeyHashSet>(256, hash);
        }
        for (decltype(srcVids.size()) index = 0u; index < srcVids.size(); ++index) {
            storage::cpp2::EdgeKey key;
            key.set_src(srcVids[index]);
            key.set_edge_type(edgeType);
            key.set_dst(dstVids[index]);
            key.set_ranking(rank == nullptr ? 0 : ranks[index]);

            if (distinct) {
                auto result = uniq->emplace(key);
                if (result.second) {
                    edgeKeys.emplace_back(std::move(key));
                }
            } else {
                edgeKeys.emplace_back(std::move(key));
            }
        }

    } else {
        if (expCtx == nullptr) {
            return Status::Error("ExpressionContext is nullptr");
        }
        std::unique_ptr<EdgeKeyHashSet> uniq;
        if (distinct) {
            uniq = std::make_unique<EdgeKeyHashSet>(256, hash);
        }

        auto edgeKeyExprs = sentence->keys()->keys();
        Getters getters;

        for (auto *keyExpr : edgeKeyExprs) {
            auto *srcExpr = keyExpr->srcid();
            srcExpr->setContext(expCtx);

            auto *dstExpr = keyExpr->dstid();
            dstExpr->setContext(expCtx);

            auto rank = keyExpr->rank();
            auto status = srcExpr->prepare();
            if (!status.ok()) {
                return status;
            }
            status = dstExpr->prepare();
            if (!status.ok()) {
                return status;
            }
            auto value = srcExpr->eval(getters);
            if (!value.ok()) {
                return std::move(value).status();
            }
            auto srcid = value.value();
            value = dstExpr->eval(getters);
            if (!value.ok()) {
                return std::move(value).status();
            }
            auto dstid = value.value();
            if (!Expression::isInt(srcid) || !Expression::isInt(dstid)) {
                return Status::Error("ID should be of type integer.");
            }
            storage::cpp2::EdgeKey key;
            key.set_src(Expression::asInt(srcid));
            key.set_edge_type(edgeType);
            key.set_dst(Expression::asInt(dstid));
            key.set_ranking(rank);

            if (distinct) {
                auto ret = uniq->emplace(key);
                if (ret.second) {
                    edgeKeys.emplace_back(std::move(key));
                }
            } else {
                edgeKeys.emplace_back(std::move(key));
            }
        }
    }
    return edgeKeys;
}

Status Collector::collect(VariantType &var, RowWriter *writer) {
    switch (var.which()) {
        case VAR_INT64:
            (*writer) << boost::get<int64_t>(var);
            break;
        case VAR_DOUBLE:
            (*writer) << boost::get<double>(var);
            break;
        case VAR_BOOL:
            (*writer) << boost::get<bool>(var);
            break;
        case VAR_STR:
            (*writer) << boost::get<std::string>(var);
            break;
        default:
            std::string errMsg =
                folly::stringPrintf("Unknown VariantType: %d", var.which());
            LOG(ERROR) << errMsg;
            return Status::Error(errMsg);
    }
    return Status::OK();
}

Status Collector::collectWithoutSchema(VariantType &var, RowWriter *writer) {
    switch (var.which()) {
        case VAR_INT64:
            VLOG(3) << boost::get<int64_t>(var);
            (*writer) << RowWriter::ColType(nebula::cpp2::SupportedType::INT)
                << boost::get<int64_t>(var);
            break;
        case VAR_DOUBLE:
            VLOG(3) << boost::get<double>(var);
            (*writer) << RowWriter::ColType(nebula::cpp2::SupportedType::DOUBLE)
                << boost::get<double>(var);
            break;
        case VAR_BOOL:
            VLOG(3) << boost::get<bool>(var);
            (*writer) << RowWriter::ColType(nebula::cpp2::SupportedType::BOOL)
                << boost::get<bool>(var);
            break;
        case VAR_STR:
            VLOG(3) << boost::get<std::string>(var);
            (*writer) << RowWriter::ColType(nebula::cpp2::SupportedType::STRING)
                << boost::get<std::string>(var);
            break;
        default:
            std::string errMsg =
                folly::stringPrintf("Unknown VariantType: %d", var.which());
            LOG(ERROR) << errMsg;
            return Status::Error(errMsg);
    }
    return Status::OK();
}

OptVariantType Collector::getProp(const meta::SchemaProviderIf *schema,
                                  const std::string &prop,
                                  const RowReader *reader) {
    DCHECK(reader != nullptr);
    DCHECK(schema != nullptr);
    using nebula::cpp2::SupportedType;
    auto type = schema->getFieldType(prop).type;
    switch (type) {
        case SupportedType::BOOL: {
            bool v;
            reader->getBool(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return v;
        }
        case SupportedType::TIMESTAMP:
        case SupportedType::INT: {
            int64_t v;
            reader->getInt(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return v;
        }
        case SupportedType::VID: {
            VertexID v;
            reader->getVid(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return v;
        }
        case SupportedType::FLOAT: {
            float v;
            reader->getFloat(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return static_cast<double>(v);
        }
        case SupportedType::DOUBLE: {
            double v;
            reader->getDouble(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return v;
        }
        case SupportedType::STRING: {
            folly::StringPiece v;
            reader->getString(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return v.toString();
        }
        default:
            std::string errMsg =
                folly::stringPrintf("Unknown type: %d", static_cast<int32_t>(type));
            LOG(ERROR) << errMsg;
            return Status::Error(errMsg);
    }
}

Status Collector::getSchema(const std::vector<VariantType> &vals,
                            const std::vector<std::string> &colNames,
                            const std::vector<nebula::cpp2::SupportedType> &colTypes,
                            SchemaWriter *outputSchema) {
    DCHECK(outputSchema != nullptr);
    DCHECK_EQ(vals.size(), colNames.size());
    DCHECK_EQ(vals.size(), colTypes.size());
    using nebula::cpp2::SupportedType;
    auto index = 0u;
    for (auto &it : colTypes) {
        SupportedType type;
        if (it == SupportedType::UNKNOWN) {
            switch (vals[index].which()) {
                case VAR_INT64:
                    // all integers in InterimResult are regarded as type of INT
                    type = SupportedType::INT;
                    break;
                case VAR_DOUBLE:
                    type = SupportedType::DOUBLE;
                    break;
                case VAR_BOOL:
                    type = SupportedType::BOOL;
                    break;
                case VAR_STR:
                    type = SupportedType::STRING;
                    break;
                default:
                    std::string errMsg =
                        folly::stringPrintf("Unknown VariantType: %d", vals[index].which());
                    LOG(ERROR) << errMsg;
                    return Status::Error(errMsg);
            }
        } else {
            type = it;
        }

        outputSchema->appendCol(colNames[index], type);
        index++;
    }
    return Status::OK();
}

Status YieldClauseWrapper::prepare(
        const InterimResult *inputs,
        const VariableHolder *varHolder,
        std::vector<YieldColumn*> &yields) {
    auto cols = clause_->columns();
    yieldColsHolder_ = std::make_unique<YieldColumns>();
    for (auto *col : cols) {
        if (col->expr()->isInputExpression()) {
            if (inputs == nullptr) {
                return Status::Error("Inputs nullptr.");
            }
            if (needAllPropsFromInput(col, inputs, yields)) {
                continue;
            }
        } else if (col->expr()->isVariableExpression()) {
            if (varHolder == nullptr) {
                return Status::Error("VarHolder nullptr.");
            }
            auto ret = needAllPropsFromVar(col, varHolder, yields);
            if (!ret.ok()) {
                return std::move(ret).status();
            }
            if (ret.value()) {
                continue;
            }
        }
        yields.emplace_back(col);
    }

    return Status::OK();
}

bool YieldClauseWrapper::needAllPropsFromInput(const YieldColumn *col,
                                               const InterimResult *inputs,
                                               std::vector<YieldColumn*> &yields) {
    auto *inputExpr = static_cast<InputPropertyExpression*>(col->expr());
    auto *colName = inputExpr->prop();
    if (*colName == "*") {
        auto colNames = inputs->getColNames();
        for (auto &prop : colNames) {
            Expression *expr = new InputPropertyExpression(new std::string(prop));
            YieldColumn *column = new YieldColumn(expr);
            yieldColsHolder_->addColumn(column);
            yields.emplace_back(column);
        }
        return true;
    }
    return false;
}

StatusOr<bool> YieldClauseWrapper::needAllPropsFromVar(
                                             const YieldColumn *col,
                                             const VariableHolder *varHolder,
                                             std::vector<YieldColumn*> &yields) {
    auto *variableExpr = static_cast<VariablePropertyExpression*>(col->expr());
    auto *colName = variableExpr->prop();
    bool existing = false;
    auto *varName = variableExpr->alias();
    auto varInputs = varHolder->get(*varName, &existing);
    if (varInputs == nullptr && !existing) {
        return Status::Error("Variable `%s' not defined.", varName->c_str());
    }
    if (*colName == "*") {
        if (varInputs != nullptr) {
            auto colNames = varInputs->getColNames();
            for (auto &prop : colNames) {
                auto *alias = new std::string(*(variableExpr->alias()));
                Expression *expr =
                        new VariablePropertyExpression(alias, new std::string(prop));
                YieldColumn *column = new YieldColumn(expr);
                yieldColsHolder_->addColumn(column);
                yields.emplace_back(column);
            }
        } else {
            // should not reach here.
            return Status::Error("Variable `%s' is nullptr.", varName->c_str());
        }
        return true;
    }
    return false;
}

Status WhereWrapper::prepare(ExpressionContext *ectx) {
    Status status = Status::OK();
    if (where_ == nullptr) {
        return status;
    }

    filter_ = where_->filter();
    if (filter_ != nullptr) {
        filter_->setContext(ectx);
        status = filter_->prepare();
        if (!status.ok()) {
            return status;
        }
    }

    if (!FLAGS_filter_pushdown) {
        return status;
    }

    auto encode = Expression::encode(where_->filter());
    auto decode = Expression::decode(std::move(encode));
    if (!decode.ok()) {
        return std::move(decode).status();
    }
    filterRewrite_ = std::move(decode).value();
    if (!rewrite(filterRewrite_.get())) {
        filterRewrite_ = nullptr;
    }
    if (filterRewrite_ != nullptr) {
        VLOG(1) << "Filter pushdown: " << filterRewrite_->toString();
        filterPushdown_ = Expression::encode(filterRewrite_.get());
    }
    return status;
}

bool WhereWrapper::rewrite(Expression *expr) const {
    switch (expr->kind()) {
        case Expression::kLogical: {
            auto *logExpr = static_cast<LogicalExpression*>(expr);
            // Rewrite rule will not be applied to XOR
            if (logExpr->op() == LogicalExpression::Operator::XOR) {
                return canPushdown(logExpr);
            }
            auto leftCanPushdown = rewrite(const_cast<Expression*>(logExpr->left()));
            auto rightCanPushdown = rewrite(const_cast<Expression*>(logExpr->right()));
            switch (logExpr->op()) {
                case LogicalExpression::Operator::OR: {
                    if (!leftCanPushdown || !rightCanPushdown) {
                        return false;
                    } else {
                        return true;
                    }
                }
                case LogicalExpression::Operator::AND: {
                    if (!leftCanPushdown && !rightCanPushdown) {
                        return false;
                    } else if (!leftCanPushdown) {
                        auto *truePri = new PrimaryExpression(true);
                        logExpr->resetLeft(truePri);
                    } else if (!rightCanPushdown) {
                        auto *truePri = new PrimaryExpression(true);
                        logExpr->resetRight(truePri);
                    }
                    return true;
                }
                default:
                    return false;
            }
        }
        case Expression::kUnary:
        case Expression::kTypeCasting:
        case Expression::kArithmetic:
        case Expression::kRelational:
        case Expression::kFunctionCall: {
            return canPushdown(expr);
        }
        case Expression::kPrimary:
        case Expression::kSourceProp:
        case Expression::kEdgeRank:
        case Expression::kEdgeDstId:
        case Expression::kEdgeSrcId:
        case Expression::kEdgeType:
        case Expression::kAliasProp: {
            return true;
        }
        case Expression::kMax:
        case Expression::kVariableProp:
        case Expression::kDestProp:
        case Expression::kInputProp:
        case Expression::kUUID: {
            return false;
        }
        default: {
            LOG(ERROR) << "Unkown expression: " << expr->kind();
            return false;
        }
    }
}

bool WhereWrapper::canPushdown(Expression *expr) const {
    auto ectx = std::make_unique<ExpressionContext>();
    expr->setContext(ectx.get());
    auto status = expr->prepare();
    if (!status.ok()) {
        // We had do the preparation before rewrite, it would not fail here.
        LOG(ERROR) << "Prepare failed when rewrite filter: " << status.toString();
        return false;
    }
    if (ectx->hasInputProp() || ectx->hasVariableProp() || ectx->hasDstTagProp()) {
        return false;
    }
    return true;
}
}   // namespace graph
}   // namespace nebula

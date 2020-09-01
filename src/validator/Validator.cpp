/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/Validator.h"

#include "common/function/FunctionManager.h"
#include "parser/Sentence.h"
#include "planner/Query.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"
#include "validator/ACLValidator.h"
#include "validator/AdminJobValidator.h"
#include "validator/AdminValidator.h"
#include "validator/AssignmentValidator.h"
#include "validator/BalanceValidator.h"
#include "validator/ExplainValidator.h"
#include "validator/FetchEdgesValidator.h"
#include "validator/FetchVerticesValidator.h"
#include "validator/GetSubgraphValidator.h"
#include "validator/GoValidator.h"
#include "validator/GroupByValidator.h"
#include "validator/LimitValidator.h"
#include "validator/MaintainValidator.h"
#include "validator/MutateValidator.h"
#include "validator/OrderByValidator.h"
#include "validator/PipeValidator.h"
#include "validator/ReportError.h"
#include "validator/SequentialValidator.h"
#include "validator/SetValidator.h"
#include "validator/UseValidator.h"
#include "validator/YieldValidator.h"
#include "visitor/EvaluableExprVisitor.h"

namespace nebula {
namespace graph {

Validator::Validator(Sentence* sentence, QueryContext* qctx)
    : sentence_(DCHECK_NOTNULL(sentence)),
      qctx_(DCHECK_NOTNULL(qctx)),
      vctx_(DCHECK_NOTNULL(qctx->vctx())) {}

std::unique_ptr<Validator> Validator::makeValidator(Sentence* sentence, QueryContext* context) {
    auto kind = sentence->kind();
    switch (kind) {
        case Sentence::Kind::kExplain:
            return std::make_unique<ExplainValidator>(sentence, context);
        case Sentence::Kind::kSequential:
            return std::make_unique<SequentialValidator>(sentence, context);
        case Sentence::Kind::kGo:
            return std::make_unique<GoValidator>(sentence, context);
        case Sentence::Kind::kPipe:
            return std::make_unique<PipeValidator>(sentence, context);
        case Sentence::Kind::kAssignment:
            return std::make_unique<AssignmentValidator>(sentence, context);
        case Sentence::Kind::kSet:
            return std::make_unique<SetValidator>(sentence, context);
        case Sentence::Kind::kUse:
            return std::make_unique<UseValidator>(sentence, context);
        case Sentence::Kind::kGetSubgraph:
            return std::make_unique<GetSubgraphValidator>(sentence, context);
        case Sentence::Kind::kLimit:
            return std::make_unique<LimitValidator>(sentence, context);
        case Sentence::Kind::kOrderBy:
            return std::make_unique<OrderByValidator>(sentence, context);
        case Sentence::Kind::kYield:
            return std::make_unique<YieldValidator>(sentence, context);
        case Sentence::Kind::kGroupBy:
            return std::make_unique<GroupByValidator>(sentence, context);
        case Sentence::Kind::kCreateSpace:
            return std::make_unique<CreateSpaceValidator>(sentence, context);
        case Sentence::Kind::kCreateTag:
            return std::make_unique<CreateTagValidator>(sentence, context);
        case Sentence::Kind::kCreateEdge:
            return std::make_unique<CreateEdgeValidator>(sentence, context);
        case Sentence::Kind::kDescribeSpace:
            return std::make_unique<DescSpaceValidator>(sentence, context);
        case Sentence::Kind::kDescribeTag:
            return std::make_unique<DescTagValidator>(sentence, context);
        case Sentence::Kind::kDescribeEdge:
            return std::make_unique<DescEdgeValidator>(sentence, context);
        case Sentence::Kind::kAlterTag:
            return std::make_unique<AlterTagValidator>(sentence, context);
        case Sentence::Kind::kAlterEdge:
            return std::make_unique<AlterEdgeValidator>(sentence, context);
        case Sentence::Kind::kShowSpaces:
            return std::make_unique<ShowSpacesValidator>(sentence, context);
        case Sentence::Kind::kShowTags:
            return std::make_unique<ShowTagsValidator>(sentence, context);
        case Sentence::Kind::kShowEdges:
            return std::make_unique<ShowEdgesValidator>(sentence, context);
        case Sentence::Kind::kDropSpace:
            return std::make_unique<DropSpaceValidator>(sentence, context);
        case Sentence::Kind::kDropTag:
            return std::make_unique<DropTagValidator>(sentence, context);
        case Sentence::Kind::kDropEdge:
            return std::make_unique<DropEdgeValidator>(sentence, context);
        case Sentence::Kind::kShowCreateSpace:
            return std::make_unique<ShowCreateSpaceValidator>(sentence, context);
        case Sentence::Kind::kShowCreateTag:
            return std::make_unique<ShowCreateTagValidator>(sentence, context);
        case Sentence::Kind::kShowCreateEdge:
            return std::make_unique<ShowCreateEdgeValidator>(sentence, context);
        case Sentence::Kind::kInsertVertices:
            return std::make_unique<InsertVerticesValidator>(sentence, context);
        case Sentence::Kind::kInsertEdges:
            return std::make_unique<InsertEdgesValidator>(sentence, context);
        case Sentence::Kind::kCreateUser:
            return std::make_unique<CreateUserValidator>(sentence, context);
        case Sentence::Kind::kDropUser:
            return std::make_unique<DropUserValidator>(sentence, context);
        case Sentence::Kind::kAlterUser:
            return std::make_unique<UpdateUserValidator>(sentence, context);
        case Sentence::Kind::kShowUsers:
            return std::make_unique<ShowUsersValidator>(sentence, context);
        case Sentence::Kind::kChangePassword:
            return std::make_unique<ChangePasswordValidator>(sentence, context);
        case Sentence::Kind::kGrant:
            return std::make_unique<GrantRoleValidator>(sentence, context);
        case Sentence::Kind::kRevoke:
            return std::make_unique<RevokeRoleValidator>(sentence, context);
        case Sentence::Kind::kShowRoles:
            return std::make_unique<ShowRolesInSpaceValidator>(sentence, context);
        case Sentence::Kind::kBalance:
            return std::make_unique<BalanceValidator>(sentence, context);
        case Sentence::Kind::kAdminJob:
            return std::make_unique<AdminJobValidator>(sentence, context);
        case Sentence::Kind::kFetchVertices:
            return std::make_unique<FetchVerticesValidator>(sentence, context);
        case Sentence::Kind::kFetchEdges:
            return std::make_unique<FetchEdgesValidator>(sentence, context);
        case Sentence::Kind::kCreateSnapshot:
            return std::make_unique<CreateSnapshotValidator>(sentence, context);
        case Sentence::Kind::kDropSnapshot:
            return std::make_unique<DropSnapshotValidator>(sentence, context);
        case Sentence::Kind::kShowSnapshots:
            return std::make_unique<ShowSnapshotsValidator>(sentence, context);
        case Sentence::Kind::kDeleteVertices:
            return std::make_unique<DeleteVerticesValidator>(sentence, context);
        case Sentence::Kind::kDeleteEdges:
            return std::make_unique<DeleteEdgesValidator>(sentence, context);
        case Sentence::Kind::kUpdateVertex:
            return std::make_unique<UpdateVertexValidator>(sentence, context);
        case Sentence::Kind::kUpdateEdge:
            return std::make_unique<UpdateEdgeValidator>(sentence, context);
        case Sentence::Kind::kShowHosts:
            return std::make_unique<ShowHostsValidator>(sentence, context);
        case Sentence::Kind::kShowParts:
            return std::make_unique<ShowPartsValidator>(sentence, context);
        case Sentence::Kind::kShowCharset:
            return std::make_unique<ShowCharsetValidator>(sentence, context);
        case Sentence::Kind::kShowCollation:
            return std::make_unique<ShowCollationValidator>(sentence, context);
        case Sentence::Kind::kGetConfig:
            return std::make_unique<GetConfigValidator>(sentence, context);
        case Sentence::Kind::kSetConfig:
            return std::make_unique<SetConfigValidator>(sentence, context);
        case Sentence::Kind::kShowConfigs:
            return std::make_unique<ShowConfigsValidator>(sentence, context);
        case Sentence::Kind::kMatch:
        case Sentence::Kind::kUnknown:
        case Sentence::Kind::kCreateTagIndex:
        case Sentence::Kind::kShowCreateTagIndex:
        case Sentence::Kind::kShowTagIndexStatus:
        case Sentence::Kind::kDescribeTagIndex:
        case Sentence::Kind::kShowTagIndexes:
        case Sentence::Kind::kRebuildTagIndex:
        case Sentence::Kind::kDropTagIndex:
        case Sentence::Kind::kCreateEdgeIndex:
        case Sentence::Kind::kShowCreateEdgeIndex:
        case Sentence::Kind::kShowEdgeIndexStatus:
        case Sentence::Kind::kDescribeEdgeIndex:
        case Sentence::Kind::kShowEdgeIndexes:
        case Sentence::Kind::kRebuildEdgeIndex:
        case Sentence::Kind::kDropEdgeIndex:
        case Sentence::Kind::kLookup:
        case Sentence::Kind::kDownload:
        case Sentence::Kind::kIngest:
        case Sentence::Kind::kFindPath:
        case Sentence::Kind::kReturn: {
            // nothing
            DLOG(FATAL) << "Unimplemented sentence " << kind;
        }
    }
    DLOG(FATAL) << "Unknown sentence " << static_cast<int>(kind);
    return std::make_unique<ReportError>(sentence, context);
}

// static
Status Validator::validate(Sentence* sentence, QueryContext* qctx) {
    DCHECK(sentence != nullptr);
    DCHECK(qctx != nullptr);

    // Check if space chosen from session. if chosen, add it to context.
    auto session = qctx->rctx()->session();
    if (session->space() > -1) {
        qctx->vctx()->switchToSpace(session->spaceName(), session->space());
    }

    auto validator = makeValidator(sentence, qctx);
    NG_RETURN_IF_ERROR(validator->validate());

    auto root = validator->root();
    if (!root) {
        return Status::Error("Get null plan from sequential validator");
    }
    qctx->plan()->setRoot(root);
    return Status::OK();
}

Status Validator::appendPlan(PlanNode* node, PlanNode* appended) {
    DCHECK(node != nullptr);
    DCHECK(appended != nullptr);
    if (node->dependencies().size() != 1) {
        return Status::SemanticError("%s not support to append an input.",
                                     PlanNode::toString(node->kind()));
    }
    static_cast<SingleDependencyNode*>(node)->dependsOn(appended);
    return Status::OK();
}

Status Validator::appendPlan(PlanNode* root) {
    return appendPlan(tail_, root);
}

Status Validator::validate() {
    if (!vctx_) {
        VLOG(1) << "Validate context was not given.";
        return Status::SemanticError("Validate context was not given.");
    }

    if (!sentence_) {
        VLOG(1) << "Sentence was not given";
        return Status::SemanticError("Sentence was not given");
    }

    if (!noSpaceRequired_ && !spaceChosen()) {
        VLOG(1) << "Space was not chosen.";
        return Status::SemanticError("Space was not chosen.");
    }

    if (!noSpaceRequired_) {
        space_ = vctx_->whichSpace();
    }

    auto status = validateImpl();
    if (!status.ok()) {
        if (status.isSemanticError()) return status;
        return Status::SemanticError(status.message());
    }

    status = toPlan();
    if (!status.ok()) {
        if (status.isSemanticError()) return status;
        return Status::SemanticError(status.message());
    }

    return Status::OK();
}

bool Validator::spaceChosen() {
    return vctx_->spaceChosen();
}

std::vector<std::string> Validator::deduceColNames(const YieldColumns* cols) const {
    std::vector<std::string> colNames;
    for (auto col : cols->columns()) {
        colNames.emplace_back(deduceColName(col));
    }
    return colNames;
}

std::string Validator::deduceColName(const YieldColumn* col) const {
    if (col->alias() != nullptr) {
        return *col->alias();
    } else {
        return col->toString();
    }
}

#define DETECT_BIEXPR_TYPE(OP)                                                                     \
    auto biExpr = static_cast<const BinaryExpression*>(expr);                                      \
    auto left = deduceExprType(biExpr->left());                                                    \
    NG_RETURN_IF_ERROR(left);                                                                      \
    auto right = deduceExprType(biExpr->right());                                                  \
    NG_RETURN_IF_ERROR(right);                                                                     \
    auto detectVal = kConstantValues.at(left.value()) OP kConstantValues.at(right.value());        \
    if (detectVal.isBadNull()) {                                                                   \
        std::stringstream ss;                                                                      \
        ss << "`" << expr->toString() << "' is not a valid expression, "                           \
           << "can not apply `" << #OP << "' to `" << left.value() << "' and `" << right.value()   \
           << "'.";                                                                                \
        return Status::SemanticError(ss.str());                                                    \
    }                                                                                              \
    return detectVal.type();

#define DETECT_UNARYEXPR_TYPE(OP)                                                                  \
    auto unaryExpr = static_cast<const UnaryExpression*>(expr);                                    \
    auto status = deduceExprType(unaryExpr->operand());                                            \
    NG_RETURN_IF_ERROR(status);                                                                    \
    auto detectVal = OP kConstantValues.at(status.value());                                        \
    if (detectVal.isBadNull()) {                                                                   \
        std::stringstream ss;                                                                      \
        ss << "`" << expr->toString() << "' is not a valid expression, "                           \
           << "can not apply `" << #OP << "' to " << status.value() << ".";                        \
        return Status::SemanticError(ss.str());                                                    \
    }                                                                                              \
    return detectVal.type();

StatusOr<Value::Type> Validator::deduceExprType(const Expression* expr) const {
    static const std::unordered_map<Value::Type, Value> kConstantValues = {
        {Value::Type::__EMPTY__, Value()},
        {Value::Type::NULLVALUE, Value(NullType::__NULL__)},
        {Value::Type::BOOL, Value(true)},
        {Value::Type::INT, Value(1)},
        {Value::Type::FLOAT, Value(1.0)},
        {Value::Type::STRING, Value("123")},
        {Value::Type::DATE, Value(Date())},
        {Value::Type::DATETIME, Value(DateTime())},
        {Value::Type::VERTEX, Value(Vertex())},
        {Value::Type::EDGE, Value(Edge())},
        {Value::Type::PATH, Value(Path())},
        {Value::Type::LIST, Value(List())},
        {Value::Type::MAP, Value(Map())},
        {Value::Type::SET, Value(Set())},
        {Value::Type::DATASET, Value(DataSet())},
    };
    switch (expr->kind()) {
        case Expression::Kind::kAdd: {
            DETECT_BIEXPR_TYPE(+)
        }
        case Expression::Kind::kMinus: {
            DETECT_BIEXPR_TYPE(-)
        }
        case Expression::Kind::kMultiply: {
            DETECT_BIEXPR_TYPE(*)
        }
        case Expression::Kind::kDivision: {
            DETECT_BIEXPR_TYPE(/)
        }
        case Expression::Kind::kMod: {
            DETECT_BIEXPR_TYPE(%)
        }
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelNE:
        case Expression::Kind::kRelLT:
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelGE:
        case Expression::Kind::kContains: {
            auto biExpr = static_cast<const BinaryExpression*>(expr);
            NG_RETURN_IF_ERROR(deduceExprType(biExpr->left()));
            NG_RETURN_IF_ERROR(deduceExprType(biExpr->right()));
            // TODO: Should deduce.
            return Value::Type::BOOL;
        }
        case Expression::Kind::kLogicalAnd: {
            DETECT_BIEXPR_TYPE(&&)
        }
        case Expression::Kind::kLogicalXor:
        case Expression::Kind::kLogicalOr: {
            DETECT_BIEXPR_TYPE(||)
        }
        case Expression::Kind::kRelIn:
        case Expression::Kind::kRelNotIn: {
            auto biExpr = static_cast<const BinaryExpression*>(expr);
            NG_RETURN_IF_ERROR(deduceExprType(biExpr->left()));

            auto right = deduceExprType(biExpr->right());
            NG_RETURN_IF_ERROR(right);
            if (right.value() != Value::Type::LIST &&
                    right.value() != Value::Type::MAP &&
                    right.value() != Value::Type::SET) {
                return Status::SemanticError("`%s': Invalid expression for IN operator, "
                                             "expecting List/Set/Map", biExpr->toString().c_str());
            }
            return Value::Type::BOOL;
        }
        case Expression::Kind::kUnaryPlus: {
            auto unaryExpr = static_cast<const UnaryExpression*>(expr);
            auto status = deduceExprType(unaryExpr->operand());
            NG_RETURN_IF_ERROR(status);

            return status.value();
        }
        case Expression::Kind::kUnaryNegate: {
            DETECT_UNARYEXPR_TYPE(-)
        }
        case Expression::Kind::kUnaryNot: {
            DETECT_UNARYEXPR_TYPE(!)
        }
        case Expression::Kind::kUnaryIncr: {
            auto unaryExpr = static_cast<const UnaryExpression*>(expr);
            auto status = deduceExprType(unaryExpr->operand());
            NG_RETURN_IF_ERROR(status);

            auto detectVal = kConstantValues.at(status.value()) + 1;
            if (detectVal.isBadNull()) {
                std::stringstream ss;
                ss << "`" << expr->toString() << "' is not a valid expression, "
                    << "can not apply `++' to " << status.value() << ".";
                return Status::SemanticError(ss.str());
            }
            return detectVal.type();
        }
        case Expression::Kind::kUnaryDecr: {
            auto unaryExpr = static_cast<const UnaryExpression*>(expr);
            auto status = deduceExprType(unaryExpr->operand());
            NG_RETURN_IF_ERROR(status);

            auto detectVal = kConstantValues.at(status.value()) - 1;
            if (detectVal.isBadNull()) {
                std::stringstream ss;
                ss << "`" << expr->toString() << "' is not a valid expression, "
                    << "can not apply `--' to " << status.value() << ".";
                return Status::SemanticError(ss.str());
            }
            return detectVal.type();
        }
        case Expression::Kind::kFunctionCall: {
            auto funcExpr = static_cast<const FunctionCallExpression *>(expr);
            std::vector<Value::Type> argsTypeList;
            argsTypeList.reserve(funcExpr->args()->numArgs());
            for (auto &arg : funcExpr->args()->args()) {
                auto status = deduceExprType(arg.get());
                NG_RETURN_IF_ERROR(status);
                argsTypeList.push_back(status.value());
            }
            auto result =
                FunctionManager::getReturnType(*(funcExpr->name()), argsTypeList);
            if (!result.ok()) {
                return Status::SemanticError("`%s` is not a valid expression : %s",
                                             expr->toString().c_str(),
                                             result.status().toString().c_str());
            }
            return result.value();
        }
        case Expression::Kind::kTypeCasting: {
            auto castExpr = static_cast<const TypeCastingExpression*>(expr);
            auto result = deduceExprType(castExpr->operand());
            NG_RETURN_IF_ERROR(result);
            if (!evaluableExpr(castExpr->operand())) {
                if (TypeCastingExpression::validateTypeCast(result.value(),
                                                            castExpr->type())) {
                    return castExpr->type();
                }
                std::stringstream out;
                out << "Can not convert " << castExpr->operand()
                    << " 's type : " << result.value() << " to "
                    << castExpr->type();
                return Status::Error(out.str());
            }
            QueryExpressionContext ctx;
            auto* typeCastExpr = const_cast<TypeCastingExpression*>(castExpr);
            auto val = typeCastExpr->eval(ctx(nullptr));
            if (val.isNull()) {
                return Status::SemanticError("`%s` is not a valid expression ",
                                             expr->toString().c_str());
            }
            return val.type();
        }
        case Expression::Kind::kTagProperty:
        case Expression::Kind::kDstProperty:
        case Expression::Kind::kSrcProperty: {
            auto* tagPropExpr = static_cast<const PropertyExpression*>(expr);
            auto* tag = tagPropExpr->sym();
            auto tagId = qctx_->schemaMng()->toTagID(space_.id, *tag);
            if (!tagId.ok()) {
                return tagId.status();
            }
            auto schema = qctx_->schemaMng()->getTagSchema(space_.id, tagId.value());
            if (!schema) {
                return Status::SemanticError(
                    "`%s', not found tag `%s'.", expr->toString().c_str(), tag->c_str());
            }
            auto* prop = tagPropExpr->prop();
            auto* field = schema->field(*prop);
            if (field == nullptr) {
                return Status::SemanticError(
                    "`%s', not found the property `%s'.", expr->toString().c_str(), prop->c_str());
            }
            return SchemaUtil::propTypeToValueType(field->type());
        }
        case Expression::Kind::kEdgeProperty: {
            auto* edgePropExpr = static_cast<const PropertyExpression*>(expr);
            auto* edge = edgePropExpr->sym();
            auto edgeType = qctx_->schemaMng()->toEdgeType(space_.id, *edge);
            if (!edgeType.ok()) {
                return edgeType.status();
            }
            auto schema = qctx_->schemaMng()->getEdgeSchema(space_.id, edgeType.value());
            if (!schema) {
                return Status::SemanticError(
                    "`%s', not found edge `%s'.", expr->toString().c_str(), edge->c_str());
            }
            auto* prop = edgePropExpr->prop();
            auto* field = schema->field(*prop);
            if (field == nullptr) {
                return Status::SemanticError(
                    "`%s', not found the property `%s'.", expr->toString().c_str(), prop->c_str());
            }
            return SchemaUtil::propTypeToValueType(field->type());
        }
        case Expression::Kind::kVarProperty: {
            auto* varPropExpr = static_cast<const PropertyExpression*>(expr);
            auto* var = varPropExpr->sym();
            if (!vctx_->existVar(*var)) {
                return Status::SemanticError(
                    "`%s', not exist variable `%s'", expr->toString().c_str(), var->c_str());
            }
            auto* prop = varPropExpr->prop();
            auto cols = vctx_->getVar(*var);
            auto found = std::find_if(cols.begin(), cols.end(), [&prop] (auto& col) {
                return *prop == col.first;
            });
            if (found == cols.end()) {
                return Status::SemanticError(
                    "`%s', not exist prop `%s'", expr->toString().c_str(), prop->c_str());
            }
            return found->second;
        }
        case Expression::Kind::kInputProperty: {
            auto* inputPropExpr = static_cast<const PropertyExpression*>(expr);
            auto* prop = inputPropExpr->prop();
            auto found = std::find_if(inputs_.begin(), inputs_.end(), [&prop] (auto& col) {
                return *prop == col.first;
            });
            if (found == inputs_.end()) {
                return Status::SemanticError(
                    "`%s', not exist prop `%s'", expr->toString().c_str(), prop->c_str());
            }
            return found->second;
        }
        case Expression::Kind::kLabelAttribute: {
            return Status::SemanticError("LabelAtrributeExpression can not be instantiated.");
        }
        case Expression::Kind::kLabel: {
            return Status::SemanticError("LabelExpression can not be instantiated.");
        }
        case Expression::Kind::kConstant: {
            QueryExpressionContext ctx;
            auto* mutableExpr = const_cast<Expression*>(expr);
            return mutableExpr->eval(ctx(nullptr)).type();
        }
        case Expression::Kind::kEdgeSrc: {
            return Value::Type::STRING;
        }
        case Expression::Kind::kEdgeType: {
            return Value::Type::INT;
        }
        case Expression::Kind::kEdgeRank: {
            return Value::Type::INT;
        }
        case Expression::Kind::kEdgeDst: {
            return Value::Type::STRING;
        }
        case Expression::Kind::kVertex: {
            return Value::Type::VERTEX;
        }
        case Expression::Kind::kEdge: {
            return Value::Type::EDGE;
        }
        case Expression::Kind::kUUID: {
            return Value::Type::STRING;
        }
        case Expression::Kind::kVar: {
            // TODO: not only dataset
            return Value::Type::DATASET;
        }
        case Expression::Kind::kVersionedVar: {
            // TODO: not only dataset
            return Value::Type::DATASET;
        }
        case Expression::Kind::kList: {
            return Value::Type::LIST;
        }
        case Expression::Kind::kSet: {
            return Value::Type::SET;
        }
        case Expression::Kind::kMap: {
            return Value::Type::MAP;
        }
        case Expression::Kind::kAttribute:
        case Expression::Kind::kSubscript: {
            return Value::Type::LIST;   // FIXME(dutor)
        }
    }
    return Status::SemanticError("Unknown expression kind: %ld",
                                 static_cast<int64_t>(expr->kind()));
}

Status Validator::deduceProps(const Expression* expr, ExpressionProps& exprProps) {
    switch (expr->kind()) {
        case Expression::Kind::kVertex:
        case Expression::Kind::kEdge:
        case Expression::Kind::kConstant: {
            break;
        }
        case Expression::Kind::kAdd:
        case Expression::Kind::kMinus:
        case Expression::Kind::kMultiply:
        case Expression::Kind::kDivision:
        case Expression::Kind::kMod:
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelNE:
        case Expression::Kind::kRelLT:
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelGE:
        case Expression::Kind::kContains:
        case Expression::Kind::kSubscript:
        case Expression::Kind::kLogicalAnd:
        case Expression::Kind::kLogicalOr:
        case Expression::Kind::kLogicalXor:
        case Expression::Kind::kRelIn:
        case Expression::Kind::kRelNotIn: {
            auto biExpr = static_cast<const BinaryExpression*>(expr);
            NG_RETURN_IF_ERROR(deduceProps(biExpr->left(), exprProps));
            NG_RETURN_IF_ERROR(deduceProps(biExpr->right(), exprProps));
            break;
        }
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot:
        case Expression::Kind::kUnaryIncr:
        case Expression::Kind::kUnaryDecr: {
            auto unaryExpr = static_cast<const UnaryExpression*>(expr);
            NG_RETURN_IF_ERROR(deduceProps(unaryExpr->operand(), exprProps));
            break;
        }
        case Expression::Kind::kFunctionCall: {
            auto funcExpr = static_cast<const FunctionCallExpression*>(expr);
            for (auto& arg : funcExpr->args()->args()) {
                NG_RETURN_IF_ERROR(deduceProps(arg.get(), exprProps));
            }
            break;
        }
        case Expression::Kind::kDstProperty: {
            auto* tagPropExpr = static_cast<const PropertyExpression*>(expr);
            auto status = qctx_->schemaMng()->toTagID(space_.id, *tagPropExpr->sym());
            NG_RETURN_IF_ERROR(status);
            exprProps.insertDstTagProp(status.value(), *tagPropExpr->prop());
            break;
        }
        case Expression::Kind::kSrcProperty: {
            auto* tagPropExpr = static_cast<const PropertyExpression*>(expr);
            auto status = qctx_->schemaMng()->toTagID(space_.id, *tagPropExpr->sym());
            NG_RETURN_IF_ERROR(status);
            exprProps.insertSrcTagProp(status.value(), *tagPropExpr->prop());
            break;
        }
        case Expression::Kind::kTagProperty: {
            auto* tagPropExpr = static_cast<const PropertyExpression*>(expr);
            auto status = qctx_->schemaMng()->toTagID(space_.id, *tagPropExpr->sym());
            NG_RETURN_IF_ERROR(status);
            exprProps.insertTagProp(status.value(), *tagPropExpr->prop());
            break;
        }
        case Expression::Kind::kEdgeProperty:
        case Expression::Kind::kEdgeSrc:
        case Expression::Kind::kEdgeType:
        case Expression::Kind::kEdgeRank:
        case Expression::Kind::kEdgeDst: {
            auto* edgePropExpr = static_cast<const PropertyExpression*>(expr);
            auto status = qctx_->schemaMng()->toEdgeType(space_.id, *edgePropExpr->sym());
            NG_RETURN_IF_ERROR(status);
            exprProps.insertEdgeProp(status.value(), *edgePropExpr->prop());
            break;
        }
        case Expression::Kind::kInputProperty: {
            auto* inputPropExpr = static_cast<const PropertyExpression*>(expr);
            exprProps.insertInputProp(*inputPropExpr->prop());
            break;
        }
        case Expression::Kind::kVarProperty: {
            auto* varPropExpr = static_cast<const PropertyExpression*>(expr);
            exprProps.insertVarProp(*varPropExpr->sym(), *varPropExpr->prop());
            break;
        }
        case Expression::Kind::kTypeCasting: {
            auto* typeCastExpr = static_cast<const TypeCastingExpression*>(expr);
            NG_RETURN_IF_ERROR(deduceProps(typeCastExpr->operand(), exprProps));
            break;
        }
        case Expression::Kind::kList: {
            auto *list = static_cast<const ListExpression*>(expr);
            for (auto *item : list->items()) {
                NG_RETURN_IF_ERROR(deduceProps(item, exprProps));
            }
            break;
        }
        case Expression::Kind::kSet: {
            auto *set = static_cast<const SetExpression*>(expr);
            for (auto *item : set->items()) {
                NG_RETURN_IF_ERROR(deduceProps(item, exprProps));
            }
            break;
        }
        case Expression::Kind::kMap: {
            auto *map = static_cast<const MapExpression*>(expr);
            for (auto &item : map->items()) {
                NG_RETURN_IF_ERROR(deduceProps(item.second, exprProps));
            }
            break;
        }
        case Expression::Kind::kUUID:
        case Expression::Kind::kVar:
        case Expression::Kind::kVersionedVar:
        case Expression::Kind::kAttribute:
        case Expression::Kind::kLabelAttribute:
        case Expression::Kind::kLabel: {
            // TODO:
            std::stringstream ss;
            ss << "Not supported expression `" << expr->toString() << "' for type deduction.";
            return Status::SemanticError(ss.str());
        }
    }
    return Status::OK();
}

bool Validator::evaluableExpr(const Expression* expr) const {
    EvaluableExprVisitor visitor;
    const_cast<Expression*>(expr)->accept(&visitor);
    return visitor.ok();
}

// static
Status Validator::checkPropNonexistOrDuplicate(const ColsDef& cols,
                                               const folly::StringPiece& prop,
                                               const std::string& validatorName) {
    auto eq = [&](const ColDef& col) { return col.first == prop.str(); };
    auto iter = std::find_if(cols.cbegin(), cols.cend(), eq);
    if (iter == cols.cend()) {
        return Status::SemanticError("%s: prop `%s' not exists",
                                      validatorName.c_str(),
                                      prop.str().c_str());
    }

    iter = std::find_if(iter + 1, cols.cend(), eq);
    if (iter != cols.cend()) {
        return Status::SemanticError("%s: duplicate prop `%s'",
                                      validatorName.c_str(),
                                      prop.str().c_str());
    }

    return Status::OK();
}

StatusOr<std::string> Validator::checkRef(const Expression* ref, Value::Type type) const {
    if (ref->kind() == Expression::Kind::kInputProperty) {
        const auto* propExpr = static_cast<const PropertyExpression*>(ref);
        ColDef col(*propExpr->prop(), type);
        const auto find = std::find(inputs_.begin(), inputs_.end(), col);
        if (find == inputs_.end()) {
            return Status::Error("No input property %s", propExpr->prop()->c_str());
        }
        return std::string();
    } else if (ref->kind() == Expression::Kind::kVarProperty) {
        const auto* propExpr = static_cast<const PropertyExpression*>(ref);
        ColDef col(*propExpr->prop(), type);
        const auto &varName = *propExpr->sym();
        const auto &var = vctx_->getVar(varName);
        if (var.empty()) {
            return Status::Error("No variable %s", varName.c_str());
        }
        const auto find = std::find(var.begin(), var.end(), col);
        if (find == var.end()) {
            return Status::Error("No property %s in variable %s",
                                 propExpr->prop()->c_str(),
                                 varName.c_str());
        }
        return varName;
    } else {
        // it's guranteed by parser
        DLOG(FATAL) << "Unexpected expression " << ref->kind();
        return Status::Error("Unexpected expression.");
    }
}

void ExpressionProps::insertVarProp(const std::string& varName, folly::StringPiece prop) {
    auto& props = varProps_[varName];
    props.emplace(prop);
}

void ExpressionProps::insertInputProp(folly::StringPiece prop) {
    inputProps_.emplace(prop);
}

void ExpressionProps::insertSrcTagProp(TagID tagId, folly::StringPiece prop) {
    auto& props = srcTagProps_[tagId];
    props.emplace(prop);
}

void ExpressionProps::insertDstTagProp(TagID tagId, folly::StringPiece prop) {
    auto& props = dstTagProps_[tagId];
    props.emplace(prop);
}

void ExpressionProps::insertEdgeProp(EdgeType edgeType, folly::StringPiece prop) {
    auto& props = edgeProps_[edgeType];
    props.emplace(prop);
}

void ExpressionProps::insertTagProp(TagID tagId, folly::StringPiece prop) {
    auto& props = tagProps_[tagId];
    props.emplace(prop);
}

bool ExpressionProps::isSubsetOfInput(const std::set<folly::StringPiece>& props) {
    for (auto& prop : props) {
        if (inputProps_.find(prop) == inputProps_.end()) {
            return false;
        }
    }
    return true;
}

bool ExpressionProps::isSubsetOfVar(const VarPropMap& props) {
    for (auto &iter : props) {
        if (varProps_.find(iter.first) == varProps_.end()) {
            return false;
        }
        for (auto& prop : iter.second) {
            if (varProps_[iter.first].find(prop) == varProps_[iter.first].end()) {
                return false;
            }
        }
    }
    return true;
}

void ExpressionProps::unionProps(ExpressionProps exprProps) {
    if (!exprProps.inputProps().empty()) {
        inputProps_.insert(std::make_move_iterator(exprProps.inputProps().begin()),
                           std::make_move_iterator(exprProps.inputProps().end()));
    }
    if (!exprProps.srcTagProps().empty()) {
        for (auto& iter : exprProps.srcTagProps()) {
            srcTagProps_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                            std::make_move_iterator(iter.second.end()));
        }
    }
    if (!exprProps.dstTagProps().empty()) {
        for (auto& iter : exprProps.dstTagProps()) {
            dstTagProps_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                            std::make_move_iterator(iter.second.end()));
        }
    }
    if (!exprProps.tagProps().empty()) {
        for (auto& iter : exprProps.tagProps()) {
            tagProps_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                         std::make_move_iterator(iter.second.end()));
        }
    }
    if (!exprProps.varProps().empty()) {
        for (auto& iter : exprProps.varProps()) {
            varProps_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                         std::make_move_iterator(iter.second.end()));
        }
    }
    if (!exprProps.edgeProps().empty()) {
        for (auto& iter : exprProps.edgeProps()) {
            edgeProps_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                          std::make_move_iterator(iter.second.end()));
        }
    }
}
}   // namespace graph
}   // namespace nebula

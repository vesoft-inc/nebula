/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/DeduceTypeVisitor.h"

#include <sstream>
#include <unordered_map>

#include "common/datatypes/DataSet.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Path.h"
#include "common/datatypes/Set.h"
#include "common/function/FunctionManager.h"
#include "context/QueryContext.h"
#include "context/QueryExpressionContext.h"
#include "context/ValidateContext.h"
#include "util/SchemaUtil.h"
#include "visitor/EvaluableExprVisitor.h"

namespace nebula {
namespace graph {

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

#define DETECT_BIEXPR_TYPE(OP)                                                                     \
    expr->left()->accept(this);                                                                    \
    if (!ok()) return;                                                                             \
    auto left = type_;                                                                             \
    expr->right()->accept(this);                                                                   \
    if (!ok()) return;                                                                             \
    auto right = type_;                                                                            \
    auto detectVal = kConstantValues.at(left) OP kConstantValues.at(right);                        \
    if (detectVal.isBadNull()) {                                                                   \
        std::stringstream ss;                                                                      \
        ss << "`" << expr->toString() << "' is not a valid expression, "                           \
           << "can not apply `" << #OP << "' to `" << left << "' and `" << right << "'.";          \
        status_ = Status::SemanticError(ss.str());                                                 \
        return;                                                                                    \
    }                                                                                              \
    type_ = detectVal.type()

#define DETECT_NARYEXPR_TYPE(OP)                                                                   \
    do {                                                                                           \
        auto &operands = expr->operands();                                                         \
        operands[0]->accept(this);                                                                 \
        if (!ok()) return;                                                                         \
        auto prev = type_;                                                                         \
        for (auto i = 1u; i < operands.size(); i++) {                                              \
            operands[i]->accept(this);                                                             \
            if (!ok()) return;                                                                     \
            auto current = type_;                                                                  \
            auto detectValue = kConstantValues.at(prev) OP kConstantValues.at(current);            \
            if (detectValue.isBadNull()) {                                                         \
                std::stringstream ss;                                                              \
                ss << "`" << expr->toString() << "' is not a valid expression, "                   \
                << "can not apply `" << #OP << "' to `" << prev << "' and `" << current << "'.";   \
                status_ = Status::SemanticError(ss.str());                                         \
                return;                                                                            \
            }                                                                                      \
            prev = detectValue.type();                                                             \
        }                                                                                          \
        type_ = prev;                                                                              \
    } while (false)

#define DETECT_UNARYEXPR_TYPE(OP)                                                                  \
    auto detectVal = OP kConstantValues.at(type_);                                                 \
    if (detectVal.isBadNull()) {                                                                   \
        std::stringstream ss;                                                                      \
        ss << "`" << expr->toString() << "' is not a valid expression, "                           \
           << "can not apply `" << #OP << "' to " << type_ << ".";                                 \
        status_ = Status::SemanticError(ss.str());                                                 \
        return;                                                                                    \
    }                                                                                              \
    type_ = detectVal.type()

DeduceTypeVisitor::DeduceTypeVisitor(QueryContext *qctx,
                                     ValidateContext *vctx,
                                     const ColsDef &inputs,
                                     GraphSpaceID space)
    : qctx_(qctx), vctx_(vctx), inputs_(inputs), space_(space) {
    DCHECK(qctx != nullptr);
    DCHECK(vctx != nullptr);
    // stand alone YIELD queries can be run without a space
    if (!vctx->spaceChosen()) {
        vidType_ = Value::Type::__EMPTY__;
    } else {
        auto vidType = vctx_->whichSpace().spaceDesc.vid_type_ref().value().get_type();
        vidType_ = SchemaUtil::propTypeToValueType(vidType);
    }
}

void DeduceTypeVisitor::visit(ConstantExpression *expr) {
    QueryExpressionContext ctx(nullptr);
    type_ = expr->eval(ctx(nullptr)).type();
}

void DeduceTypeVisitor::visit(UnaryExpression *expr) {
    expr->operand()->accept(this);
    if (!ok()) return;
    switch (expr->kind()) {
        case Expression::Kind::kUnaryPlus:
            break;
        case Expression::Kind::kUnaryNegate: {
            DETECT_UNARYEXPR_TYPE(-);
            break;
        }
        case Expression::Kind::kUnaryNot: {
            DETECT_UNARYEXPR_TYPE(!);
            break;
        }
        case Expression::Kind::kIsNull: {
            type_ = Value::Type::BOOL;
            break;
        }
        case Expression::Kind::kIsNotNull: {
            type_ = Value::Type::BOOL;
            break;
        }
        case Expression::Kind::kIsEmpty: {
            type_ = Value::Type::BOOL;
            break;
        }
        case Expression::Kind::kIsNotEmpty: {
            type_ = Value::Type::BOOL;
            break;
        }
        case Expression::Kind::kUnaryIncr: {
            auto detectVal = kConstantValues.at(type_) + 1;
            if (detectVal.isBadNull()) {
                std::stringstream ss;
                ss << "`" << expr->toString() << "' is not a valid expression, "
                   << "can not apply `++' to " << type_ << ".";
                status_ = Status::SemanticError(ss.str());
                return;
            }
            type_ = detectVal.type();
            break;
        }
        case Expression::Kind::kUnaryDecr: {
            auto detectVal = kConstantValues.at(type_) - 1;
            if (detectVal.isBadNull()) {
                std::stringstream ss;
                ss << "`" << expr->toString() << "' is not a valid expression, "
                   << "can not apply `--' to " << type_ << ".";
                status_ = Status::SemanticError(ss.str());
                return;
            }
            type_ = detectVal.type();
            break;
        }
        default: {
            LOG(FATAL) << "Invalid unary expression kind: " << static_cast<uint8_t>(expr->kind());
            break;
        }
    }
}

void DeduceTypeVisitor::visit(TypeCastingExpression *expr) {
    expr->operand()->accept(this);
    if (!ok()) return;

    // if can't deduce the type of expr's operand, ignore it
    if (type_ == Value::Type::NULLVALUE || type_ == Value::Type::__EMPTY__) {
        type_ = expr->type();
        return;
    }

    EvaluableExprVisitor visitor;
    expr->operand()->accept(&visitor);

    if (!visitor.ok()) {
        if (TypeCastingExpression::validateTypeCast(type_, expr->type())) {
            type_ = expr->type();
            return;
        }
        std::stringstream out;
        out << "Can not convert " << expr->operand()->toString() << " 's type : " << type_ << " to "
            << expr->type();
        status_ = Status::SemanticError(out.str());
        return;
    }
    QueryExpressionContext ctx(nullptr);
    auto val = expr->eval(ctx(nullptr));
    if (val.isNull()) {
        status_ =
            Status::SemanticError("`%s' is not a valid expression ", expr->toString().c_str());
        return;
    }
    type_ = val.type();
    status_ = Status::OK();
}

void DeduceTypeVisitor::visit(LabelExpression *) {
    type_ = Value::Type::__EMPTY__;
}

void DeduceTypeVisitor::visit(ArithmeticExpression *expr) {
    switch (expr->kind()) {
        case Expression::Kind::kAdd: {
            DETECT_BIEXPR_TYPE(+);
            break;
        }
        case Expression::Kind::kMinus: {
            DETECT_BIEXPR_TYPE(-);
            break;
        }
        case Expression::Kind::kMultiply: {
            DETECT_BIEXPR_TYPE(*);
            break;
        }
        case Expression::Kind::kDivision: {
            DETECT_BIEXPR_TYPE(/);
            break;
        }
        case Expression::Kind::kMod: {
            DETECT_BIEXPR_TYPE(%);
            break;
        }
        default: {
            LOG(FATAL) << "Invalid arithmetic expression kind: "
                       << static_cast<uint8_t>(expr->kind());
            break;
        }
    }
}

void DeduceTypeVisitor::visit(RelationalExpression *expr) {
    expr->left()->accept(this);
    if (!ok()) return;
    expr->right()->accept(this);
    if (!ok()) return;

    if (expr->kind() == Expression::Kind::kRelIn || expr->kind() == Expression::Kind::kRelNotIn) {
        auto right = type_;
        if (right != Value::Type::LIST && right != Value::Type::SET && right != Value::Type::MAP &&
            !isSuperiorType(right)) {
            std::stringstream ss;
            ss << "`" << expr->right()->toString()
               << "', expected List/Set/Map, but was "
               << type_;
            status_ = Status::SemanticError(ss.str());
            return;
        }
    }

    type_ = Value::Type::BOOL;
}

void DeduceTypeVisitor::visit(SubscriptExpression *expr) {
    expr->left()->accept(this);
    if (!ok()) return;
    auto leftType = type_;
    static auto leftCandidate =
        Value::Type::LIST | Value::Type::MAP | Value::Type::NULLVALUE | Value::Type::__EMPTY__;
    auto isLeftCandidate = leftType & leftCandidate;
    if (!isLeftCandidate) {
        std::stringstream ss;
        ss << "`" << expr->toString() << "', expected LIST but was " << leftType << ": "
           << expr->left()->toString();
        status_ = Status::SemanticError(ss.str());
        return;
    }

    expr->right()->accept(this);
    if (!ok()) return;
    auto rightType = type_;

    static auto leftListCandidate = Value::Type::LIST;
    static auto rightListSubCandidate =
        Value::Type::INT | Value::Type::NULLVALUE | Value::Type::__EMPTY__;
    auto notValidListCandidate =
        (leftListCandidate & leftType) && !(rightListSubCandidate & rightType);
    if (notValidListCandidate) {
        std::stringstream ss;
        ss << "`" << expr->toString() << "', expected Integer but was " << rightType << ": "
           << expr->right()->toString();
        status_ = Status::SemanticError(ss.str());
        return;
    }

    static auto leftMapCandidate = Value::Type::MAP;
    static auto rightMapSubCandidate =
        Value::Type::STRING | Value::Type::NULLVALUE | Value::Type::__EMPTY__;
    auto notValidMapCandidate =
        (leftMapCandidate & leftType) && !(rightMapSubCandidate & rightType);
    if (notValidMapCandidate) {
        std::stringstream ss;
        ss << "`" << expr->toString() << "', expected Identifier but was " << rightType << ": "
           << expr->right()->toString();
        status_ = Status::SemanticError(ss.str());
        return;
    }

    static auto rightCandidate =
        Value::Type::INT | Value::Type::STRING | Value::Type::NULLVALUE | Value::Type::__EMPTY__;
    if (isSuperiorType(leftType) && !rightCandidate) {
        std::stringstream ss;
        ss << "`" << expr->toString() << "', expected Integer Or Identifier but was " << rightType
           << ": " << expr->right()->toString();
        status_ = Status::SemanticError(ss.str());
        return;
    }

    // Will not deduce the actual type of the value in list.
    type_ = Value::Type::__EMPTY__;
}

void DeduceTypeVisitor::visit(AttributeExpression *expr) {
    expr->left()->accept(this);
    if (!ok()) return;
    switch (type_) {
        case Value::Type::MAP:
        case Value::Type::VERTEX:
        case Value::Type::EDGE:
        case Value::Type::DATE:
        case Value::Type::TIME:
        case Value::Type::DATETIME:
            // nothing
            break;
        default: {
            if (!isSuperiorType(type_)) {
                std::stringstream ss;
                ss << "`" << expr->toString() <<
                    "', expected type with attribute like Date, Time, DateTime, "
                    "Map, Vertex or Edge but was " <<
                    type_ << ": " << expr->left()->toString();
                status_ = Status::SemanticError(ss.str());
                return;
            }
        }
    }

    expr->right()->accept(this);
    if (!ok()) return;
    if (type_ != Value::Type::STRING && !isSuperiorType(type_)) {
        std::stringstream ss;
        ss << "`" << expr->toString()
           << "', expected an valid identifier: " << expr->right()->toString();
        status_ = Status::SemanticError(ss.str());
        return;
    }

    // Will not deduce the actual type of the attribute.
    type_ = Value::Type::__EMPTY__;
}

void DeduceTypeVisitor::visit(LogicalExpression *expr) {
    switch (expr->kind()) {
        case Expression::Kind::kLogicalAnd: {
            DETECT_NARYEXPR_TYPE(&&);
            break;
        }
        case Expression::Kind::kLogicalXor:
        case Expression::Kind::kLogicalOr: {
            DETECT_NARYEXPR_TYPE(||);
            break;
        }
        default: {
            LOG(FATAL) << "Invalid logical expression kind: " << static_cast<uint8_t>(expr->kind());
            break;
        }
    }
}

void DeduceTypeVisitor::visit(LabelAttributeExpression *expr) {
    const_cast<LabelExpression*>(expr->left())->accept(this);
    if (!ok()) return;
    if (type_ != Value::Type::STRING && !isSuperiorType(type_)) {
        std::stringstream ss;
        ss << "`" << expr->toString()
           << "', expected an valid identifier: " << expr->left()->toString();
        status_ = Status::SemanticError(ss.str());
        return;
    }

    const_cast<ConstantExpression*>(expr->right())->accept(this);
    if (!ok()) return;
    if (type_ != Value::Type::STRING && !isSuperiorType(type_)) {
        std::stringstream ss;
        ss << "`" << expr->toString()
           << "', expected an valid identifier: " << expr->right()->toString();
        status_ = Status::SemanticError(ss.str());
        return;
    }

    // Will not deduce the actual type of the attribute.
    type_ = Value::Type::__EMPTY__;
}

void DeduceTypeVisitor::visit(FunctionCallExpression *expr) {
    std::vector<Value::Type> argsTypeList;
    argsTypeList.reserve(expr->args()->numArgs());
    for (auto &arg : expr->args()->args()) {
        arg->accept(this);
        if (!ok()) return;
        argsTypeList.push_back(type_);
    }
    auto funName = expr->name();
    if (funName == "id" || funName == "src" || funName == "dst") {
        type_ = vidType_;
        return;
    }
    auto result = FunctionManager::getReturnType(funName, argsTypeList);
    if (!result.ok()) {
        status_ = Status::SemanticError("`%s' is not a valid expression : %s",
                                        expr->toString().c_str(),
                                        result.status().toString().c_str());
        return;
    }
    type_ = result.value();
}

void DeduceTypeVisitor::visit(AggregateExpression *expr) {
    expr->arg()->accept(this);
    if (!ok()) return;
    type_ = Value::Type::__EMPTY__;
}

void DeduceTypeVisitor::visit(UUIDExpression *) {
    type_ = Value::Type::STRING;
}

void DeduceTypeVisitor::visit(VariableExpression *) {
    // Will not deduce the actual value type of variable expression.
    type_ = Value::Type::__EMPTY__;
}

void DeduceTypeVisitor::visit(VersionedVariableExpression *) {
    // Will not deduce the actual value type of versioned variable expression.
    type_ = Value::Type::__EMPTY__;
}

void DeduceTypeVisitor::visit(ListExpression *) {
    type_ = Value::Type::LIST;
}

void DeduceTypeVisitor::visit(SetExpression *) {
    type_ = Value::Type::SET;
}

void DeduceTypeVisitor::visit(MapExpression *) {
    type_ = Value::Type::MAP;
}

void DeduceTypeVisitor::visit(TagPropertyExpression *expr) {
    visitVertexPropertyExpr(expr);
}

void DeduceTypeVisitor::visit(EdgePropertyExpression *expr) {
    const auto &edge = expr->sym();
    auto edgeType = qctx_->schemaMng()->toEdgeType(space_, edge);
    if (!edgeType.ok()) {
        status_ = edgeType.status();
        return;
    }
    auto schema = qctx_->schemaMng()->getEdgeSchema(space_, edgeType.value());
    if (!schema) {
        status_ = Status::SemanticError(
            "`%s', not found edge `%s'.", expr->toString().c_str(), edge.c_str());
        return;
    }

    const auto &prop = expr->prop();
    auto *field = schema->field(prop);
    if (field == nullptr) {
        status_ = Status::SemanticError(
            "`%s', not found the property `%s'.", expr->toString().c_str(), prop.c_str());
        return;
    }
    type_ = SchemaUtil::propTypeToValueType(field->type());
}

void DeduceTypeVisitor::visit(InputPropertyExpression *expr) {
    const auto &prop = expr->prop();
    auto found = std::find_if(
        inputs_.cbegin(), inputs_.cend(), [&prop](auto &col) { return prop == col.name; });
    if (found == inputs_.cend()) {
        status_ = Status::SemanticError(
            "`%s', not exist prop `%s'", expr->toString().c_str(), prop.c_str());
        return;
    }
    type_ = found->type;
}

void DeduceTypeVisitor::visit(VariablePropertyExpression *expr) {
    const auto &var = expr->sym();
    if (!vctx_->existVar(var)) {
        status_ = Status::SemanticError(
            "`%s', not exist variable `%s'", expr->toString().c_str(), var.c_str());
        return;
    }
    const auto &prop = expr->prop();
    auto cols = vctx_->getVar(var);
    auto found =
        std::find_if(cols.begin(), cols.end(), [&prop](auto &col) { return prop == col.name; });
    if (found == cols.end()) {
        status_ = Status::SemanticError(
            "`%s', not exist prop `%s'", expr->toString().c_str(), prop.c_str());
        return;
    }
    type_ = found->type;
}

void DeduceTypeVisitor::visit(DestPropertyExpression *expr) {
    visitVertexPropertyExpr(expr);
}

void DeduceTypeVisitor::visit(SourcePropertyExpression *expr) {
    visitVertexPropertyExpr(expr);
}

void DeduceTypeVisitor::visit(EdgeSrcIdExpression *) {
    type_ = vidType_;
}

void DeduceTypeVisitor::visit(EdgeTypeExpression *) {
    type_ = Value::Type::INT;
}

void DeduceTypeVisitor::visit(EdgeRankExpression *) {
    type_ = Value::Type::INT;
}

void DeduceTypeVisitor::visit(EdgeDstIdExpression *) {
    type_ = vidType_;
}

void DeduceTypeVisitor::visit(VertexExpression *) {
    type_ = Value::Type::VERTEX;
}

void DeduceTypeVisitor::visit(EdgeExpression *) {
    type_ = Value::Type::EDGE;
}

void DeduceTypeVisitor::visit(ColumnExpression *) {
    type_ = Value::Type::__EMPTY__;
}

void DeduceTypeVisitor::visit(CaseExpression *expr) {
    if (expr->hasCondition()) {
        expr->condition()->accept(this);
        if (!ok()) return;
    }
    if (expr->hasDefault()) {
        expr->defaultResult()->accept(this);
        if (!ok()) return;
    }

    std::unordered_set<Value::Type> types;
    for (const auto &whenThen : expr->cases()) {
        whenThen.when->accept(this);
        if (!ok()) return;
        if (!expr->hasCondition() && type_ != Value::Type::BOOL && !isSuperiorType(type_)) {
            std::stringstream ss;
            ss << "`" << whenThen.when->toString() << "', expected BOOL, but was " << type_;
            status_ = Status::SemanticError(ss.str());
            return;
        }
        whenThen.then->accept(this);
        if (!ok()) return;
        types.emplace(type_);
    }

    if (types.size() == 1) {
        type_ = *types.begin();
    } else {
        type_ = Value::Type::__EMPTY__;
    }
}

void DeduceTypeVisitor::visit(PredicateExpression *expr) {
    if (expr->hasFilter()) {
        expr->filter()->accept(this);
        if (!ok()) {
            return;
        }
    }

    expr->collection()->accept(this);
    if (!ok()) return;
    if (type_ == Value::Type::NULLVALUE || type_ == Value::Type::__EMPTY__) {
        return;
    }
    if (type_ != Value::Type::LIST) {
        std::stringstream ss;
        ss << "`" << expr->collection()->toString()
           << "', expected LIST, but was " << type_;
        status_ = Status::SemanticError(ss.str());
        return;
    }

    type_ = Value::Type::BOOL;
}

void DeduceTypeVisitor::visit(ListComprehensionExpression *expr) {
    if (expr->hasFilter()) {
        expr->filter()->accept(this);
        if (!ok()) {
            return;
        }
    }
    if (expr->hasMapping()) {
        expr->mapping()->accept(this);
        if (!ok()) {
            return;
        }
    }

    expr->collection()->accept(this);
    if (!ok()) {
        return;
    }

    if (type_ == Value::Type::NULLVALUE || type_ == Value::Type::__EMPTY__) {
        return;
    }

    if (type_ != Value::Type::LIST) {
        std::stringstream ss;
        ss << "`" << expr->collection()->toString()
           << "', expected LIST, but was " << type_;
        status_ = Status::SemanticError(ss.str());
        return;
    }

    type_ = Value::Type::LIST;
}

void DeduceTypeVisitor::visit(ReduceExpression *expr) {
    expr->initial()->accept(this);
    if (!ok()) return;
    expr->mapping()->accept(this);
    if (!ok()) return;

    expr->collection()->accept(this);
    if (!ok()) return;

    if (type_ == Value::Type::NULLVALUE || type_ == Value::Type::__EMPTY__) {
        return;
    }

    if (type_ != Value::Type::LIST) {
        std::stringstream ss;
        ss << "`" << expr->collection()->toString()
           << "', expected LIST, but was " << type_;
        status_ = Status::SemanticError(ss.str());
        return;
    }

    // Will not deduce the actual value type returned by reduce expression.
    type_ = Value::Type::__EMPTY__;
}

void DeduceTypeVisitor::visit(SubscriptRangeExpression *expr) {
    expr->list()->accept(this);
    if (!ok()) {
        return;
    }
    if (type_ == Value::Type::NULLVALUE || type_ == Value::Type::__EMPTY__) {
        // deduce failed
        return;
    }
    if (type_ != Value::Type::LIST) {
        status_ = Status::SemanticError("Expect list type for subscript range operator.");
        return;
    }

    if (expr->lo() != nullptr) {
        expr->lo()->accept(this);
        if (!ok()) {
            return;
        }
        if (type_ != Value::Type::INT && type_ != Value::Type::NULLVALUE) {
            status_ = Status::SemanticError("Expect integer type for subscript range bound.");
            return;
        }
    }

    if (expr->hi() != nullptr) {
        expr->hi()->accept(this);
        if (!ok()) {
            return;
        }
        if (type_ != Value::Type::INT && type_ != Value::Type::NULLVALUE) {
            status_ = Status::SemanticError("Expect integer type for subscript range bound.");
            return;
        }
    }
    type_ = Value::Type::LIST;
}

void DeduceTypeVisitor::visitVertexPropertyExpr(PropertyExpression *expr) {
    const auto &tag = expr->sym();
    auto tagId = qctx_->schemaMng()->toTagID(space_, tag);
    if (!tagId.ok()) {
        status_ = tagId.status();
        return;
    }
    auto schema = qctx_->schemaMng()->getTagSchema(space_, tagId.value());
    if (!schema) {
        status_ = Status::SemanticError(
            "`%s', not found tag `%s'.", expr->toString().c_str(), tag.c_str());
        return;
    }
    const auto &prop = expr->prop();
    auto *field = schema->field(prop);
    if (field == nullptr) {
        status_ = Status::SemanticError(
            "`%s', not found the property `%s'.", expr->toString().c_str(), prop.c_str());
        return;
    }
    type_ = SchemaUtil::propTypeToValueType(field->type());
}

void DeduceTypeVisitor::visit(PathBuildExpression *) {
    type_ = Value::Type::PATH;
}
#undef DETECT_NARYEXPR_TYPE
#undef DETECT_UNARYEXPR_TYPE
#undef DETECT_BIEXPR_TYPE

}   // namespace graph
}   // namespace nebula

/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/VidExtractVisitor.h"

namespace nebula {
namespace graph {

/*static*/ VidExtractVisitor::VidPattern VidExtractVisitor::intersect(VidPattern &&left,
                                                                      VidPattern &&right) {
    DCHECK(left.spec == VidPattern::Special::kInUsed);
    DCHECK(right.spec == VidPattern::Special::kInUsed);
    VidPattern v{VidPattern::Special::kInUsed, {std::move(left.nodes)}};
    for (auto &node : right.nodes) {
        DCHECK(node.second.kind == VidPattern::Vids::Kind::kIn);
        auto find = v.nodes.find(node.first);
        if (find == v.nodes.end()) {
            v.nodes.emplace(std::move(*find));
        } else {
            std::sort(find->second.vids.values.begin(), find->second.vids.values.end());
            std::sort(node.second.vids.values.begin(), node.second.vids.values.end());
            std::vector<Value> intersection;
            std::set_intersection(find->second.vids.values.begin(),
                                  find->second.vids.values.end(),
                                  node.second.vids.values.begin(),
                                  node.second.vids.values.end(),
                                  std::back_inserter(intersection));
            find->second.vids.values = std::move(intersection);
        }
    }
    return v;
}

/*static*/ VidExtractVisitor::VidPattern VidExtractVisitor::intersect(
    VidPattern &&left,
    std::pair<std::string, VidPattern::Vids> &&right) {
    auto find = left.nodes.find(right.first);
    if (find == left.nodes.end()) {
        left.nodes.emplace(std::move(right));
    } else {
        std::sort(find->second.vids.values.begin(), find->second.vids.values.end());
        std::sort(right.second.vids.values.begin(), right.second.vids.values.end());
        std::vector<Value> values;
        std::set_intersection(find->second.vids.values.begin(),
                              find->second.vids.values.end(),
                              right.second.vids.values.begin(),
                              right.second.vids.values.end(),
                              std::back_inserter(values));
        find->second.vids.values = std::move(values);
    }
    return std::move(left);
}

/*static*/ VidExtractVisitor::VidPattern VidExtractVisitor::intersect(
    std::pair<std::string, VidPattern::Vids> &&left,
    std::pair<std::string, VidPattern::Vids> &&right) {
    VidPattern v{VidPattern::Special::kInUsed, {}};
    if (left.first != right.first) {
        v.nodes.emplace(std::move(left));
        v.nodes.emplace(std::move(right));
    } else {
        std::sort(left.second.vids.values.begin(), left.second.vids.values.end());
        std::sort(right.second.vids.values.begin(), right.second.vids.values.end());
        std::vector<Value> values;
        std::set_intersection(left.second.vids.values.begin(),
                              left.second.vids.values.end(),
                              right.second.vids.values.begin(),
                              right.second.vids.values.end(),
                              std::back_inserter(values));
        v.nodes[left.first].kind = VidPattern::Vids::Kind::kIn;
        v.nodes[left.first].vids.values.insert(v.nodes[left.first].vids.values.end(),
                                               std::make_move_iterator(values.begin()),
                                               std::make_move_iterator(values.end()));
    }
    return v;
}

void VidExtractVisitor::visit(ConstantExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(UnaryExpression *expr) {
    if (expr->kind() == Expression::Kind::kUnaryNot) {
        //        const auto *expr = static_cast<const UnaryExpression *>(expr);
        expr->operand()->accept(this);
        auto operandResult = moveVidPattern();
        if (operandResult.spec == VidPattern::Special::kInUsed) {
            for (auto &node : operandResult.nodes) {
                switch (node.second.kind) {
                    case VidPattern::Vids::Kind::kOtherSource:
                        break;
                    case VidPattern::Vids::Kind::kIn:
                        node.second.kind = VidPattern::Vids::Kind::kNotIn;
                        break;
                    case VidPattern::Vids::Kind::kNotIn:
                        node.second.kind = VidPattern::Vids::Kind::kIn;
                        break;
                }
            }
        }
        vidPattern_ = std::move(operandResult);
    } else {
        vidPattern_ = VidPattern{};
    }
}

void VidExtractVisitor::visit(TypeCastingExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(LabelExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(LabelAttributeExpression *expr) {
    if (expr->kind() == Expression::Kind::kLabelAttribute) {
        const auto *labelExpr = static_cast<const LabelAttributeExpression *>(expr);
        vidPattern_ = VidPattern{
            VidPattern::Special::kInUsed,
            {{labelExpr->left()->toString(), {VidPattern::Vids::Kind::kOtherSource, {}}}}};
    } else {
        vidPattern_ = VidPattern{};
    }
}

void VidExtractVisitor::visit(ArithmeticExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(RelationalExpression *expr) {
    if (expr->kind() == Expression::Kind::kRelIn) {
        //    const auto *inExpr = static_cast<const RelationalExpression *>(expr);
        if (expr->left()->kind() == Expression::Kind::kLabelAttribute) {
            const auto *labelExpr = static_cast<const LabelAttributeExpression *>(expr->left());
            const auto &label = labelExpr->left()->toString();
            vidPattern_ = VidPattern{VidPattern::Special::kInUsed,
                                     {{label, {VidPattern::Vids::Kind::kOtherSource, {}}}}};
            return;
        }
        if (expr->left()->kind() != Expression::Kind::kFunctionCall ||
            expr->right()->kind() != Expression::Kind::kConstant) {
            vidPattern_ = VidPattern{};
            return;
        }
        const auto *fCallExpr = static_cast<const FunctionCallExpression *>(expr->left());
        if (fCallExpr->name() != "id" && fCallExpr->args()->numArgs() != 1 &&
            fCallExpr->args()->args().front()->kind() != Expression::Kind::kLabel) {
            vidPattern_ = VidPattern{};
            return;
        }
        const auto *constExpr = static_cast<const ConstantExpression *>(expr->right());
        if (constExpr->value().type() != Value::Type::LIST) {
            vidPattern_ = VidPattern{};
            return;
        }
        vidPattern_ = VidPattern{VidPattern::Special::kInUsed,
                                 {{fCallExpr->args()->args().front()->toString(),
                                   {VidPattern::Vids::Kind::kIn, constExpr->value().getList()}}}};
        return;
    } else if (expr->kind() == Expression::Kind::kRelEQ) {
        //        const auto *eqExpr = static_cast<const RelationalExpression *>(expr);
        if (expr->left()->kind() == Expression::Kind::kLabelAttribute) {
            const auto *labelExpr = static_cast<const LabelAttributeExpression *>(expr->left());
            const auto &label = labelExpr->left()->toString();
            vidPattern_ = VidPattern{VidPattern::Special::kInUsed,
                                     {{label, {VidPattern::Vids::Kind::kOtherSource, {}}}}};
            return;
        }
        if (expr->left()->kind() != Expression::Kind::kFunctionCall ||
            expr->right()->kind() != Expression::Kind::kConstant) {
            vidPattern_ = VidPattern{};
            return;
        }
        const auto *fCallExpr = static_cast<const FunctionCallExpression *>(expr->left());
        if (fCallExpr->name() != "id" && fCallExpr->args()->numArgs() != 1 &&
            fCallExpr->args()->args().front()->kind() != Expression::Kind::kLabel) {
            vidPattern_ = VidPattern{};
            return;
        }
        const auto *constExpr = static_cast<const ConstantExpression *>(expr->right());
        if (!SchemaUtil::isValidVid(constExpr->value())) {
            vidPattern_ = VidPattern{};
            return;
        }
        vidPattern_ = VidPattern{VidPattern::Special::kInUsed,
                                 {{fCallExpr->args()->args().front()->toString(),
                                   {VidPattern::Vids::Kind::kIn, List({constExpr->value()})}}}};
        return;
    } else {
        vidPattern_ = VidPattern{};
        return;
    }
}

void VidExtractVisitor::visit(SubscriptExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(AttributeExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(LogicalExpression *expr) {
    if (expr->kind() == Expression::Kind::kLogicalAnd) {
        //        const auto *expr = static_cast<const LogicalExpression *>(expr);
        std::vector<VidPattern> operandsResult;
        operandsResult.reserve(expr->operands().size());
        for (const auto &operand : expr->operands()) {
            //            operandsResult.emplace_back(reverseEvalVids(operand.get()));
            operand->accept(this);
            operandsResult.emplace_back(moveVidPattern());
        }
        VidPattern inResult{VidPattern::Special::kInUsed, {}};
        VidPattern notInResult{VidPattern::Special::kInUsed, {}};
        for (auto &operandResult : operandsResult) {
            if (operandResult.spec == VidPattern::Special::kInUsed) {
                for (auto &node : operandResult.nodes) {
                    if (node.second.kind == VidPattern::Vids::Kind::kNotIn) {
                        notInResult.nodes[node.first].vids.values.insert(
                            notInResult.nodes[node.first].vids.values.end(),
                            std::make_move_iterator(node.second.vids.values.begin()),
                            std::make_move_iterator(node.second.vids.values.end()));
                    }
                }
            }
        }
        // intersect all in list
        std::vector<std::pair<std::string, VidPattern::Vids>> inOperandsResult;
        for (auto &operandResult : operandsResult) {
            if (operandResult.spec == VidPattern::Special::kInUsed) {
                for (auto &node : operandResult.nodes) {
                    if (node.second.kind == VidPattern::Vids::Kind::kIn) {
                        inOperandsResult.emplace_back(std::move(node));
                    }
                }
            }
        }
        if (inOperandsResult.empty()) {
            // noting
        } else if (inOperandsResult.size() == 1) {
            inResult.nodes.emplace(std::move(inOperandsResult.front()));
        } else {
            inResult = intersect(std::move(inOperandsResult[0]), std::move(inOperandsResult[1]));
            for (std::size_t i = 2; i < inOperandsResult.size(); ++i) {
                inResult = intersect(std::move(inResult), std::move(inOperandsResult[i]));
            }
        }
        // remove that not in item
        for (auto &node : inResult.nodes) {
            auto find = notInResult.nodes.find(node.first);
            if (find != notInResult.nodes.end()) {
                List removeNotIn;
                for (auto &v : node.second.vids.values) {
                    if (std::find(find->second.vids.values.begin(),
                                  find->second.vids.values.end(),
                                  v) == find->second.vids.values.end()) {
                        removeNotIn.emplace_back(std::move(v));
                    }
                }
                node.second.vids = std::move(removeNotIn);
            }
        }
        vidPattern_ = std::move(inResult);
        return;
    } else if (expr->kind() == Expression::Kind::kLogicalOr) {
        //        const auto *andExpr = static_cast<const LogicalExpression *>(expr);
        std::vector<VidPattern> operandsResult;
        operandsResult.reserve(expr->operands().size());
        for (const auto &operand : expr->operands()) {
            operand->accept(this);
            operandsResult.emplace_back(moveVidPattern());
        }
        VidPattern inResult{VidPattern::Special::kInUsed, {}};
        for (auto &result : operandsResult) {
            if (result.spec == VidPattern::Special::kInUsed) {
                for (auto &node : result.nodes) {
                    // Can't deduce with outher source (e.g. PropertiesIndex)
                    switch (node.second.kind) {
                        case VidPattern::Vids::Kind::kOtherSource:
                            vidPattern_ = VidPattern{};
                            return;
                        case VidPattern::Vids::Kind::kIn: {
                            inResult.nodes[node.first].kind = VidPattern::Vids::Kind::kIn;
                            inResult.nodes[node.first].vids.values.insert(
                                inResult.nodes[node.first].vids.values.end(),
                                std::make_move_iterator(node.second.vids.values.begin()),
                                std::make_move_iterator(node.second.vids.values.end()));
                        }
                        case VidPattern::Vids::Kind::kNotIn:
                            // nothing
                            break;
                    }
                }
            }
        }
        vidPattern_ = std::move(inResult);
        return;
    } else {
        vidPattern_ = VidPattern{};
        return;
    }
}

// function call
void VidExtractVisitor::visit(FunctionCallExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(UUIDExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

// variable expression
void VidExtractVisitor::visit(VariableExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(VersionedVariableExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

// container expression
void VidExtractVisitor::visit(ListExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(SetExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(MapExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

// property Expression
void VidExtractVisitor::visit(TagPropertyExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(EdgePropertyExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(InputPropertyExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(VariablePropertyExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(DestPropertyExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(SourcePropertyExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(EdgeSrcIdExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(EdgeTypeExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(EdgeRankExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(EdgeDstIdExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(VertexExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(EdgeExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(ColumnExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(CaseExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visitBinaryExpr(BinaryExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(PathBuildExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(ListComprehensionExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(AggregateExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(PredicateExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(ReduceExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

void VidExtractVisitor::visit(SubscriptRangeExpression *expr) {
    UNUSED(expr);
    vidPattern_ = VidPattern{};
}

std::ostream &operator<<(std::ostream &os, const VidExtractVisitor::VidPattern &vp) {
    switch (vp.spec) {
        case VidExtractVisitor::VidPattern::Special::kIgnore:
            os << "Ignore.";
            break;

        case VidExtractVisitor::VidPattern::Special::kInUsed:
            os << "InUsed: " << std::endl;
            for (const auto &node : vp.nodes) {
                os << node.first;
                switch (node.second.kind) {
                    case VidExtractVisitor::VidPattern::Vids::Kind::kOtherSource:
                        os << " is OtherSource";
                        break;
                    case VidExtractVisitor::VidPattern::Vids::Kind::kIn:
                        os << " in " << node.second.vids;
                        break;
                    case VidExtractVisitor::VidPattern::Vids::Kind::kNotIn:
                        os << " not in " << node.second.vids;
                        break;
                }
                os << std::endl;
            }
            break;
    }
    return os;
}

}   // namespace graph
}   // namespace nebula

/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/index/IndexPolicyMaker.h"

namespace nebula {
namespace storage {
cpp2::ErrorCode IndexPolicyMaker::policyPrepare(const std::string& filter) {
    auto ret = optimizeFilter(filter);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    ret = prepareExpression(exp_.get());
    return ret;
}

void IndexPolicyMaker::policyGenerate() {
    switch (policyType_) {
        case PolicyType::SIMPLE_POLICY : {
            break;
        }
        case PolicyType::OPTIMIZED_POLICY : {
            initPolicy();
            break;
        }
    }
}

void IndexPolicyMaker::initPolicy() {
    decltype(operatorList_.size()) hintNum = 0;
    bool hasStr = false;
    for (auto& col : index_.get_cols()) {
        auto it = std::find_if(operatorList_.begin(), operatorList_.end(),
                               [&col] (const auto& op) {
                                   return col.get_name() == op.first.first;
                               });
        if (it != operatorList_.end()) {
            if (it->second) {
                hintNum++;
                auto v = it->first.second;
                hasStr = (v.which() == VAR_STR);
                policyScanType_ = PolicyScanType::PREFIX_SCAN;
                policies_.emplace_back(std::move(v));
            } else {
                break;
            }
        } else {
            break;
        }
    }
    if (hintNum == operatorList_.size() && !hasStr) {
        /**
         * TODO sky : drop the sub-exp from root expression tree.
         */
        policyScanType_ = PolicyScanType::ACCURATE_SCAN;
    }
}

std::string IndexPolicyMaker::prepareAPE(const Expression* expr) {
    auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(expr);
    return aExpr->prop()->c_str();
}

StatusOr<VariantType> IndexPolicyMaker::preparePE(const Expression* expr) {
    auto* pExpr = dynamic_cast<const PrimaryExpression*>(expr);
    Getters getters;
    auto value = pExpr->eval(getters);
    if (!value.ok()) {
        return Status::SyntaxError("eval expression error : %s", pExpr->toString().data());
    }
    return value.value();
}

cpp2::ErrorCode IndexPolicyMaker::prepareRFE(const Expression* expr) {
    cpp2::ErrorCode code = cpp2::ErrorCode::E_INVALID_FILTER;
    switch (expr->kind()) {
        case nebula::Expression::kRelational : {
            std::string prop;
            VariantType v;
            auto* rExpr = dynamic_cast<const RelationalExpression*>(expr);
            auto* left = rExpr->left();
            auto* right = rExpr->right();
            if (left->kind() == nebula::Expression::kAliasProp
                && right->kind() == nebula::Expression::kPrimary) {
                prop = prepareAPE(left);
                auto ret = preparePE(right);
                if (!ret.ok()) {
                    return code;
                }
                v = ret.value();
            } else if (left->kind() == nebula::Expression::kPrimary
                       && right->kind() == nebula::Expression::kAliasProp) {
                auto ret = preparePE(right);
                if (!ret.ok()) {
                    return code;
                }
                v = ret.value();
                prop = prepareAPE(left);
            } else {
                return code;
            }
            operatorList_.emplace_back(std::make_pair(std::move(prop), std::move(v)),
                                       rExpr->op() == RelationalExpression::Operator::EQ);
            break;
        }
        case nebula::Expression::kFunctionCall : {
            /**
             * TODO sky : Parse the prop name from function expresion,
             *            because only check the 'Operator::EQ' ,
             *            so the prop name can be ignore.
             */
            VariantType v;
            std::string prop = ".";
            auto* fExpr = dynamic_cast<const FunctionCallExpression*>(expr);
            prop.append(*fExpr->name());
            operatorList_.emplace_back(std::make_pair(std::move(prop), std::move(v)), false);
            break;
        }
        case nebula::Expression::kLogical : {
            auto* lExpr = dynamic_cast<const LogicalExpression*>(expr);
            auto* left = lExpr->left();
            code = prepareRFE(left);
            if (code != cpp2::ErrorCode::SUCCEEDED) {
                return code;
            }
            auto* right = lExpr->right();
            code = prepareRFE(right);
            if (code != cpp2::ErrorCode::SUCCEEDED) {
                return code;
            }
            break;
        }
        default: {
            return code;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode IndexPolicyMaker::prepareLE(const LogicalExpression* expr) {
    cpp2::ErrorCode code = cpp2::ErrorCode::SUCCEEDED;
    if (policyType_ == PolicyType::SIMPLE_POLICY) {
        return code;
    }

    if (expr->op() == LogicalExpression::Operator::XOR) {
        return cpp2::ErrorCode::E_INVALID_FILTER;
    } else if (expr->op() == LogicalExpression::Operator::OR) {
        policyType_ = PolicyType::SIMPLE_POLICY;
        return code;
    }
    auto* left = expr->left();
    code = prepareExpression(left);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    auto* right = expr->right();
    code = prepareExpression(right);
    return code;
}

cpp2::ErrorCode IndexPolicyMaker::prepareExpression(const Expression* expr) {
    cpp2::ErrorCode code = cpp2::ErrorCode::SUCCEEDED;
    if (policyType_ == PolicyType::SIMPLE_POLICY) {
        return code;
    }
    if (expr->kind() == nebula::Expression::kLogical) {
        auto* lExpr = dynamic_cast<const LogicalExpression*>(expr);
        code = prepareLE(lExpr);
    } else {
        code = prepareRFE(expr);
    }
    return code;
}

bool IndexPolicyMaker::exprEval(Getters &getters) {
    if (exp_ != nullptr) {
        auto value = exp_->eval(getters);
        return (value.ok() && !Expression::asBool(value.value()));
    }
    return true;
}
}  // namespace storage
}  // namespace nebula


/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/index/IndexPolicyMaker.h"

namespace nebula {
namespace storage {
cpp2::ErrorCode IndexPolicyMaker::policyPrepare(const std::string& filter) {
    auto expRet = Expression::decode(filter);
    if (!expRet.ok()) {
        VLOG(1) << "Can't decode the filter " << filter;
        return cpp2::ErrorCode::E_INVALID_FILTER;
    }
    exp_ = std::move(expRet).value();
    if (expCtx_ == nullptr) {
        expCtx_ = std::make_unique<ExpressionContext>();
    }
    exp_->setContext(this->expCtx_.get());
    auto status = exp_->prepare();
    if (!status.ok()) {
        return cpp2::ErrorCode::E_INVALID_FILTER;
    }
    auto ret = prepareExpr(exp_.get());
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

cpp2::ErrorCode IndexPolicyMaker::prepareExpr(const Expression* expr) {
    cpp2::ErrorCode code = cpp2::ErrorCode::SUCCEEDED;
    Getters getters;
    switch (expr->kind()) {
        case nebula::Expression::kLogical : {
            auto* lExpr = dynamic_cast<const LogicalExpression*>(expr);
            if (policyType_ == PolicyType::SIMPLE_POLICY) {
                return code;
            }

            if (lExpr->op() == LogicalExpression::Operator::XOR) {
                return cpp2::ErrorCode::E_INVALID_FILTER;
            } else if (lExpr->op() == LogicalExpression::Operator::OR) {
                policyType_ = PolicyType::SIMPLE_POLICY;
                return code;
            }
            auto* left = lExpr->left();
            prepareExpr(left);
            auto* right = lExpr->right();
            prepareExpr(right);
            break;
        }
        case nebula::Expression::kRelational : {
            std::string prop;
            VariantType v;
            auto* rExpr = dynamic_cast<const RelationalExpression*>(expr);
            auto* left = rExpr->left();
            auto* right = rExpr->right();
            if (left->kind() == nebula::Expression::kAliasProp
                && right->kind() == nebula::Expression::kPrimary) {
                auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(left);
                prop = aExpr->prop()->c_str();
                auto* pExpr = dynamic_cast<const PrimaryExpression*>(right);
                auto value = pExpr->eval(getters);
                if (!value.ok()) {
                    return cpp2::ErrorCode::E_INVALID_FILTER;
                }
                v = value.value();
            } else if (left->kind() == nebula::Expression::kPrimary
                       && right->kind() == nebula::Expression::kAliasProp) {
                auto* pExpr = dynamic_cast<const PrimaryExpression*>(left);
                auto value = pExpr->eval(getters);
                if (!value.ok()) {
                    return cpp2::ErrorCode::E_INVALID_FILTER;
                }
                v = value.value();
                auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(right);
                prop = aExpr->prop()->c_str();
            } else {
                prepareExpr(left);
                prepareExpr(right);
                break;
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
        default : {
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
    }
    return code;
}

bool IndexPolicyMaker::exprEval(Getters &getters) {
    if (exp_ != nullptr) {
        auto value = exp_->eval(getters);
        return (value.ok() && Expression::asBool(value.value()));
    }
    return true;
}
}  // namespace storage
}  // namespace nebula


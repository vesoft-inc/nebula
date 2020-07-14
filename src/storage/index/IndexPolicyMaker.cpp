/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/index/IndexPolicyMaker.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {
cpp2::ErrorCode IndexPolicyMaker::preparePolicy(const std::string &filter) {
    auto ret = decodeExpression(filter);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    /**
     * Traverse the expression tree and
     * collect the relationship for execution policies.
     */
    ret = traversalExpression(exp_.get());
    return ret;
}

cpp2::ErrorCode IndexPolicyMaker::decodeExpression(const std::string &filter) {
    cpp2::ErrorCode code = cpp2::ErrorCode::SUCCEEDED;
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
    return code;
}

void IndexPolicyMaker::buildPolicy() {
    prefix_.reserve(256);
    decltype(operatorList_.size()) hintNum = 0;
    bool hasStr = false;
    for (auto& col : index_->get_fields()) {
        auto it = std::find_if(operatorList_.begin(), operatorList_.end(),
                               [&col] (const auto& tup) {
                                   return col.get_name() == std::get<0>(tup);;
                               });
        if (it != operatorList_.end()) {
            if (std::get<2>(*it) == RelationalExpression::Operator::EQ) {
                /**
                 * TODO sky : drop the sub-exp from root expression tree.
                 */
                hintNum++;
                auto v = std::get<1>(*it);
                hasStr = (v.which() == VAR_STR);
                prefix_.append(NebulaKeyUtils::encodeVariant(v));
            } else {
                break;
            }
        } else {
            break;
        }
    }
    if (optimizedPolicy_ && hintNum == operatorList_.size() && !hasStr) {
        requiredFilter_ = false;
    }
}

cpp2::ErrorCode IndexPolicyMaker::traversalExpression(const Expression *expr) {
    cpp2::ErrorCode code = cpp2::ErrorCode::SUCCEEDED;
    if (!optimizedPolicy_) {
        return code;
    }
    Getters getters;

    /**
     *  TODO (sky) :
     *  Handler error for FuncExpr or  ArithmeticExpr contains
     *  AliasPropExpr , for example :
     *  "tag1.col1 > tag1.col2" or "tag1.col2 > (tag1.col3 - 100)" , etc.
     */
    getters.getAliasProp = [](const std::string&,
                              const std::string&) -> OptVariantType {
        return OptVariantType(Status::Error("Alias expression cannot be evaluated"));
    };
    switch (expr->kind()) {
        case nebula::Expression::kLogical : {
            auto* lExpr = dynamic_cast<const LogicalExpression*>(expr);
            if (lExpr->op() == LogicalExpression::Operator::XOR) {
                return cpp2::ErrorCode::E_INVALID_FILTER;
            } else if (lExpr->op() == LogicalExpression::Operator::OR) {
                optimizedPolicy_ = false;
            }
            auto* left = lExpr->left();
            traversalExpression(left);
            auto* right = lExpr->right();
            traversalExpression(right);
            break;
        }
        case nebula::Expression::kRelational : {
            std::string prop;
            VariantType v;
            auto* rExpr = dynamic_cast<const RelationalExpression*>(expr);
            auto* left = rExpr->left();
            auto* right = rExpr->right();
            if (left->kind() == nebula::Expression::kAliasProp) {
                auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(left);
                prop = *aExpr->prop();
                auto value = right->eval(getters);
                if (!value.ok()) {
                    VLOG(1) << "Can't evaluate the expression " << right->toString();
                    return cpp2::ErrorCode::E_INVALID_FILTER;
                }
                v = value.value();
            } else if (right->kind() == nebula::Expression::kAliasProp) {
                auto value = left->eval(getters);
                if (!value.ok()) {
                    VLOG(1) << "Can't evaluate the expression " << left->toString();
                    return cpp2::ErrorCode::E_INVALID_FILTER;
                }
                v = value.value();
                auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(right);
                prop = *aExpr->prop();
            } else {
                optimizedPolicy_ = false;
                break;
            }
            operatorList_.emplace_back(std::make_tuple(std::move(prop), std::move(v), rExpr->op()));
            break;
        }
        case nebula::Expression::kFunctionCall : {
            optimizedPolicy_ = false;
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


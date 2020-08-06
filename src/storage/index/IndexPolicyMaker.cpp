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

bool IndexPolicyMaker::buildPolicy() {
    decltype(operatorList_.size()) hintNum = 0;
    for (auto& col : index_->get_fields()) {
        auto itr = operatorList_.begin();
        while (itr != operatorList_.end()) {
            if (col.get_name() == std::get<0>(*itr)) {
                /**
                 * TODO sky : drop the sub-exp from root expression tree.
                 */
                hintNum++;
                if (!writeScanItem(col.get_name(), *itr)) {
                    return false;
                }
                // string field still need filter.
                if (col.get_type().type == nebula::cpp2::SupportedType::STRING) {
                    requiredFilter_ = true;
                }
                // Delete operator item if hint.
                operatorList_.erase(itr);
            } else {
                ++itr;
            }
        }
        /**
         * If index field does not hit the filter condition,
         * means there is no need to loop the index fields. for example :
         * index (c1, c2, c3)
         * where c1 > 1 and c3 == 1
         * Field c2 is missing from the operatorList_,
         * So we just need using c1 to range scan and filter c3.
         */
        auto exist = scanItems_.find(col.get_name());
        if (exist == scanItems_.end()) {
            break;
        }
    }
    if (operatorList_.size() > 0) {
        requiredFilter_ = true;
    }
    return true;
}

cpp2::ErrorCode IndexPolicyMaker::traversalExpression(const Expression *expr) {
    cpp2::ErrorCode code = cpp2::ErrorCode::SUCCEEDED;
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
            // OR logical expression is not allowed in graph layer.
            // Make sure all logical expression is 'AND' at here.
            auto* lExpr = dynamic_cast<const LogicalExpression*>(expr);
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
            RelationalExpression::Operator op;
            if (left->kind() == nebula::Expression::kAliasProp) {
                auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(left);
                prop = *aExpr->prop();
                auto value = right->eval(getters);
                if (!value.ok()) {
                    VLOG(1) << "Can't evaluate the expression " << right->toString();
                    return cpp2::ErrorCode::E_INVALID_FILTER;
                }
                v = value.value();
                op = rExpr->op();
            } else if (right->kind() == nebula::Expression::kAliasProp) {
                auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(right);
                prop = *aExpr->prop();
                auto value = left->eval(getters);
                if (!value.ok()) {
                    VLOG(1) << "Can't evaluate the expression " << left->toString();
                    return cpp2::ErrorCode::E_INVALID_FILTER;
                }
                v = value.value();
                op = reversalRelationalExprOP(rExpr->op());
            }
            operatorList_.emplace_back(std::make_tuple(std::move(prop), std::move(v), op));
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

RelationalExpression::Operator
IndexPolicyMaker::reversalRelationalExprOP(RelationalExpression::Operator op) {
    switch (op) {
        case RelationalExpression::Operator::LT: {
            return RelationalExpression::Operator::GT;
        }
        case RelationalExpression::Operator::LE: {
            return RelationalExpression::Operator::GE;
        }
        case RelationalExpression::Operator::GT: {
            return RelationalExpression::Operator::LT;
        }
        case RelationalExpression::Operator::GE: {
            return RelationalExpression::Operator::LE;
        }
        default : {
            return op;
        }
    }
}

bool IndexPolicyMaker::writeScanItem(const std::string& prop, const OperatorItem& item) {
    auto op = std::get<2>(item);
    switch (op) {
        case RelationalExpression::Operator::GE :
        case RelationalExpression::Operator::GT : {
            auto endThan = op == RelationalExpression::Operator::GT;
            auto v = scanItems_.find(prop);
            if (v == scanItems_.end()) {
                scanItems_[prop] = std::make_tuple(endThan, std::get<1>(item),
                                                   false, Status::Error());
            } else {
                if (!std::get<1>(v->second).ok()) {
                    std::get<1>(v->second) = std::get<1>(item);
                } else if (std::get<1>(v->second).value() < std::get<1>(item)) {
                    // This might be the case where c1 > 1 and c1 > 5 , so the 5 should be save.
                    std::get<1>(v->second) = std::get<1>(item);
                }
            }
            break;
        }
        case RelationalExpression::Operator::LE :
        case RelationalExpression::Operator::LT : {
            auto beginThan = op == RelationalExpression::Operator::LT;
            auto v = scanItems_.find(prop);
            if (v == scanItems_.end()) {
                scanItems_[prop] = std::make_tuple(false, Status::Error(),
                                                   beginThan, std::get<1>(item));
            } else {
                if (!std::get<3>(v->second).ok()) {
                    std::get<3>(v->second) = std::get<1>(item);
                } else if (std::get<1>(v->second).value() > std::get<1>(item)) {
                    // This might be the case where c1 < 1 and c1 < 5 , so the 1 should be save.
                    std::get<3>(v->second) = std::get<1>(item);
                }
            }
            break;
        }
        default : {
            auto v = scanItems_.find(prop);
            if (v == scanItems_.end()) {
                scanItems_[prop] = std::make_tuple(false, std::get<1>(item),
                                                   false, std::get<1>(item));
            } else {
                // If this field appears in scanItems_ ,
                // means that the filter conditions are wrong, for example :
                // c1 == 1 and c2 == 2
                VLOG(1) << "Repeated conditional expression for field : " << prop;
                return false;
            }
        }
    }
    return true;
}
}  // namespace storage
}  // namespace nebula


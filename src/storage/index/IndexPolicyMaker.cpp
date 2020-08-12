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
                 * TODO (sky) : String range scan was disabled on graph layer.
                 *              it is not support for storage layer .
                 */
                hintNum++;
                if (!writeScanItem(col.get_name(), *itr)) {
                    return false;
                }
                // string field still need filter.
                if (col.get_type().type == nebula::cpp2::SupportedType::STRING) {
                    requiredFilter_ = true;
                }
                if (std::get<2>(*itr) == RelationalExpression::Operator::NE) {
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

RelationType IndexPolicyMaker::toRel(RelationalExpression::Operator op) {
    switch (op) {
        case RelationalExpression::Operator::LT :
            return RelationType::kLTRel;
        case RelationalExpression::Operator::LE :
            return RelationType::kLERel;
        case RelationalExpression::Operator::GT :
            return RelationType::kGTRel;
        case RelationalExpression::Operator::GE:
            return RelationType::kGERel;
        case RelationalExpression::Operator::EQ :
            return RelationType::kEQRel;
        case RelationalExpression::Operator::NE :
            return RelationType::kNERel;
        default :
            return RelationType::kNull;
    }
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
        // for example col > 1, means the operator is GT. if col >= 1 ,means the opertor is GE.
        // if operator is GT or GE . the 1 should a begin value.
        case RelationalExpression::Operator::GE :
        case RelationalExpression::Operator::GT : {
            auto v = scanItems_.find(prop);
            if (v == scanItems_.end()) {
                // if the field did not exist in scanItems_, add an new one.
                // default value is invalid VariantType.
                scanItems_[prop] = ScanBound(Bound(toRel(op), std::get<1>(item)), Bound());
            } else {
                if (v->second.beginBound_.rel_ == RelationType::kNull) {
                    // if value is invalid VariantType, reset it.
                    v->second.beginBound_ = Bound(toRel(op), std::get<1>(item));
                } else if (v->second.beginBound_.val_ < std::get<1>(item)) {
                    // This might be the case where c1 > 1 and c1 > 5 , so the 5 should be save.
                    v->second.beginBound_.val_ = std::get<1>(item);
                }
            }
            break;
        }
        /**
         * if col < 1, means the operator is LT. if col <= 1 ,means the opertor is LE.
         * if operator is LT or LE . the 1 should a end value.
         **/
        case RelationalExpression::Operator::LE :
        case RelationalExpression::Operator::LT : {
            auto v = scanItems_.find(prop);
            if (v == scanItems_.end()) {
                scanItems_[prop] = ScanBound(Bound(), Bound(toRel(op), std::get<1>(item)));
            } else {
                if (v->second.endBound_.rel_ == RelationType::kNull) {
                    v->second.endBound_ = Bound(toRel(op), std::get<1>(item));
                } else if (v->second.endBound_.val_ > std::get<1>(item)) {
                    // This might be the case where c1 < 1 and c1 < 5 , so the 1 should be save.
                    v->second.endBound_.val_ = std::get<1>(item);
                }
            }
            break;
        }
        case RelationalExpression::Operator::NE : {
            break;
        }
        default : {
            auto v = scanItems_.find(prop);
            if (v == scanItems_.end()) {
                scanItems_[prop] = ScanBound(Bound(toRel(op), std::get<1>(item)),
                                             Bound(toRel(op), std::get<1>(item)));
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


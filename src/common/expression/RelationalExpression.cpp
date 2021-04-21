/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/RelationalExpression.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Set.h"
#include "common/datatypes/Map.h"
#include "common/expression/ExprVisitor.h"

namespace nebula {
const Value& RelationalExpression::eval(ExpressionContext& ctx) {
    auto& lhs = lhs_->eval(ctx);
    auto& rhs = rhs_->eval(ctx);

    switch (kind_) {
        case Kind::kRelEQ:
            result_ = lhs.equal(rhs);
            break;
        case Kind::kRelNE:
            result_ = !lhs.equal(rhs);
            break;
        case Kind::kRelLT:
            result_ = lhs.lessThan(rhs);
            break;
        case Kind::kRelLE:
            result_ = lhs.lessThan(rhs) || lhs.equal(rhs);
            break;
        case Kind::kRelGT:
            result_ = !lhs.lessThan(rhs) && !lhs.equal(rhs);
            break;
        case Kind::kRelGE:
            result_ = !lhs.lessThan(rhs) || lhs.equal(rhs);
            break;
        case Kind::kRelREG: {
            if (lhs.isBadNull() || rhs.isBadNull()) {
                result_ = Value::kNullBadType;
            } else if ((!lhs.isNull() && !lhs.isStr()) || (!rhs.isNull() && !rhs.isStr())) {
                result_ = Value::kNullBadType;
            } else if (lhs.isStr() && rhs.isStr()) {
                try {
                    const auto& r = ctx.getRegex(rhs.getStr());
                    result_ = std::regex_match(lhs.getStr(), r);
                } catch (const std::exception& ex) {
                    LOG(ERROR) << "Regex match error: " << ex.what();
                    result_ = Value::kNullBadType;
                }
            } else {
                result_ = Value::kNullValue;
            }
            break;
        }
        case Kind::kRelIn: {
            if (rhs.isNull() && !rhs.isBadNull()) {
                result_ = Value::kNullValue;
            } else if (rhs.isList()) {
                auto& list = rhs.getList();
                result_ = list.contains(lhs);
                if (UNLIKELY(result_.isBool() &&
                             !result_.getBool() &&
                             list.contains(Value::kNullValue))) {
                    result_ = Value::kNullValue;
                }
            } else if (rhs.isSet()) {
                auto& set = rhs.getSet();
                result_ = set.contains(lhs);
                if (UNLIKELY(result_.isBool() &&
                             !result_.getBool() &&
                             set.contains(Value::kNullValue))) {
                    result_ = Value::kNullValue;
                }
            } else if (rhs.isMap()) {
                auto& map = rhs.getMap();
                result_ = map.contains(lhs);
                if (UNLIKELY(result_.isBool() &&
                             !result_.getBool() &&
                             map.contains(Value::kNullValue))) {
                    result_ = Value::kNullValue;
                }
            } else {
                result_ = Value(NullType::BAD_TYPE);
            }

            if (UNLIKELY(!result_.isBadNull() && lhs.isNull())) {
                result_ = Value::kNullValue;
            }
            break;
        }
        case Kind::kRelNotIn: {
            if (rhs.isNull() && !rhs.isBadNull()) {
                result_ = Value::kNullValue;
            } else if (rhs.isList()) {
                auto& list = rhs.getList();
                result_ = !list.contains(lhs);
                if (UNLIKELY(result_.isBool() &&
                             result_.getBool() &&
                             list.contains(Value::kNullValue))) {
                    result_ = Value::kNullValue;
                }
            } else if (rhs.isSet()) {
                auto& set = rhs.getSet();
                result_ = !set.contains(lhs);
                if (UNLIKELY(result_.isBool() &&
                             result_.getBool() &&
                             set.contains(Value::kNullValue))) {
                    result_ = Value::kNullValue;
                }
            } else if (rhs.isMap()) {
                auto& map = rhs.getMap();
                result_ = !map.contains(lhs);
                if (UNLIKELY(result_.isBool() &&
                             result_.getBool() &&
                             map.contains(Value::kNullValue))) {
                    result_ = Value::kNullValue;
                }
            } else {
                result_ = Value(NullType::BAD_TYPE);
            }

            if (UNLIKELY(!result_.isBadNull() && lhs.isNull())) {
                result_ = Value::kNullValue;
            }
            break;
        }
        case Kind::kContains: {
            if (lhs.isBadNull() || rhs.isBadNull()) {
                result_ = Value::kNullBadType;
            } else if ((!lhs.isNull() && !lhs.isStr()) || (!rhs.isNull() && !rhs.isStr())) {
                result_ = Value::kNullBadType;
            } else if (lhs.isStr() && rhs.isStr()) {
                result_ = lhs.getStr().size() >= rhs.getStr().size() &&
                          lhs.getStr().find(rhs.getStr()) != std::string::npos;
            } else {
                result_ = Value::kNullValue;
            }
            break;
        }
        case Kind::kNotContains: {
            if (lhs.isBadNull() || rhs.isBadNull()) {
                result_ = Value::kNullBadType;
            } else if ((!lhs.isNull() && !lhs.isStr()) || (!rhs.isNull() && !rhs.isStr())) {
                result_ = Value::kNullBadType;
            } else if (lhs.isStr() && rhs.isStr()) {
                result_ = !(lhs.getStr().size() >= rhs.getStr().size() &&
                            lhs.getStr().find(rhs.getStr()) != std::string::npos);
            } else {
                result_ = Value::kNullValue;
            }
            break;
        }
        case Kind::kStartsWith: {
            if (lhs.isBadNull() || rhs.isBadNull()) {
                result_ = Value::kNullBadType;
            } else if ((!lhs.isNull() && !lhs.isStr()) || (!rhs.isNull() && !rhs.isStr())) {
                result_ = Value::kNullBadType;
            } else if (lhs.isStr() && rhs.isStr()) {
                result_ = lhs.getStr().size() >= rhs.getStr().size() &&
                          lhs.getStr().find(rhs.getStr()) == 0;
            } else {
                result_ = Value::kNullValue;
            }
            break;
        }
        case Kind::kNotStartsWith: {
            if (lhs.isBadNull() || rhs.isBadNull()) {
                result_ = Value::kNullBadType;
            } else if ((!lhs.isNull() && !lhs.isStr()) || (!rhs.isNull() && !rhs.isStr())) {
                result_ = Value::kNullBadType;
            } else if (lhs.isStr() && rhs.isStr()) {
                result_ = !(lhs.getStr().size() >= rhs.getStr().size() &&
                            lhs.getStr().find(rhs.getStr()) == 0);
            } else {
                result_ = Value::kNullValue;
            }
            break;
        }
        case Kind::kEndsWith: {
            if (lhs.isBadNull() || rhs.isBadNull()) {
                result_ = Value::kNullBadType;
            } else if ((!lhs.isNull() && !lhs.isStr()) || (!rhs.isNull() && !rhs.isStr())) {
                result_ = Value::kNullBadType;
            } else if (lhs.isStr() && rhs.isStr()) {
                result_ = lhs.getStr().size() >= rhs.getStr().size() &&
                          lhs.getStr().compare(lhs.getStr().size() - rhs.getStr().size(),
                                               rhs.getStr().size(),
                                               rhs.getStr()) == 0;
            } else {
                result_ = Value::kNullValue;
            }
            break;
        }
        case Kind::kNotEndsWith: {
            if (lhs.isBadNull() || rhs.isBadNull()) {
                result_ = Value::kNullBadType;
            } else if ((!lhs.isNull() && !lhs.isStr()) || (!rhs.isNull() && !rhs.isStr())) {
                result_ = Value::kNullBadType;
            } else if (lhs.isStr() && rhs.isStr()) {
                result_ = !(lhs.getStr().size() >= rhs.getStr().size() &&
                            lhs.getStr().compare(lhs.getStr().size() - rhs.getStr().size(),
                                                 rhs.getStr().size(),
                                                 rhs.getStr()) == 0);
            } else {
                result_ = Value::kNullValue;
            }
            break;
        }
        default:
            LOG(FATAL) << "Unknown type: " << kind_;
    }
    return result_;
}

std::string RelationalExpression::toString() const {
    std::string op;
    switch (kind_) {
        case Kind::kRelLT:
            op = "<";
            break;
        case Kind::kRelLE:
            op = "<=";
            break;
        case Kind::kRelGT:
            op = ">";
            break;
        case Kind::kRelGE:
            op = ">=";
            break;
        case Kind::kRelEQ:
            op = "==";
            break;
        case Kind::kRelNE:
            op = "!=";
            break;
        case Kind::kRelREG:
            op = "=~";
            break;
        case Kind::kRelIn:
            op = " IN ";
            break;
        case Kind::kRelNotIn:
            op = " NOT IN ";
            break;
        case Kind::kContains:
            op = " CONTAINS ";
            break;
        case Kind::kNotContains:
            op = " NOT CONTAINS ";
            break;
        case Kind::kStartsWith:
            op = " STARTS WITH ";
            break;
        case Kind::kNotStartsWith:
            op = " NOT STARTS WITH ";
            break;
        case Kind::kEndsWith:
            op = " ENDS WITH ";
            break;
        case Kind::kNotEndsWith:
            op = " NOT ENDS WITH ";
            break;
        default:
            op = " illegal symbol ";
    }
    std::stringstream out;
    out << "(" << lhs_->toString() << op << rhs_->toString() << ")";
    return out.str();
}

void RelationalExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

}  // namespace nebula

/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/SubscriptExpression.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/List.h"
#include "common/expression/ExprVisitor.h"
#include "common/base/CheckPointer.h"

namespace nebula {

const Value& SubscriptExpression::eval(ExpressionContext &ctx) {
    auto &lvalue = left()->eval(ctx);
    auto &rvalue = right()->eval(ctx);

    result_ = Value::kNullValue;
    do {
        if (lvalue.isList()) {
            if (!rvalue.isInt()) {
                result_ = Value::kNullBadType;
                break;
            }
            auto size = static_cast<int64_t>(lvalue.getList().size());
            auto index = rvalue.getInt();
            if (index >= size || index < -size) {
                result_ = Value::kNullOutOfRange;
                break;
            }
            // list[0] === list[-size], list[-1] === list[size-1]
            if (index < 0) {
                index += size;
            }
            result_ = lvalue.getList()[index];
            break;
        }
        if (lvalue.isMap()) {
            if (!rvalue.isStr()) {
                break;
            }
            result_ = lvalue.getMap().at(rvalue.getStr());
            break;
        }
        if (lvalue.isDataSet()) {
            if (!rvalue.isInt()) {
                result_ = Value::kNullBadType;
                break;
            }
            auto size = static_cast<int64_t>(lvalue.getDataSet().rowSize());
            auto rowIndex = rvalue.getInt();
            if (rowIndex >= size || rowIndex < 0) {
                result_ = Value::kNullOutOfRange;
                break;
            }
            result_ = lvalue.getDataSet().rows[rowIndex];
            break;
        }
        if (lvalue.isVertex()) {
            if (!rvalue.isStr()) {
                break;
            }
            if (rvalue.getStr() == kVid) {
                result_ = lvalue.getVertex().vid;
                return result_;
            }
            for (auto &tag : lvalue.getVertex().tags) {
                auto iter = tag.props.find(rvalue.getStr());
                if (iter != tag.props.end()) {
                    return iter->second;
                }
            }
            return Value::kNullValue;
        }
        if (lvalue.isEdge()) {
            if (!rvalue.isStr()) {
                break;
            }
            DCHECK(!rvalue.getStr().empty());
            if (rvalue.getStr()[0] == '_') {
                if (rvalue.getStr() == kSrc) {
                    result_ = lvalue.getEdge().src;
                } else if (rvalue.getStr() == kDst) {
                    result_ = lvalue.getEdge().dst;
                } else if (rvalue.getStr() == kRank) {
                    result_ = lvalue.getEdge().ranking;
                } else if (rvalue.getStr() == kType) {
                    result_ = lvalue.getEdge().name;
                }
                return result_;
            }
            auto iter = lvalue.getEdge().props.find(rvalue.getStr());
            if (iter == lvalue.getEdge().props.end()) {
                return Value::kNullValue;
            }
            return iter->second;
        }
        result_ = Value::kNullBadType;
    } while (false);

    return result_;
}


std::string SubscriptExpression::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += left()->toString();
    buf += '[';
    buf += right()->toString();
    buf += ']';
    return buf;
}

void SubscriptExpression::accept(ExprVisitor *visitor) {
    visitor->visit(this);
}

// For the positive range bound it start from begin,
// for the negative range bound it start from end
const Value& SubscriptRangeExpression::eval(ExpressionContext &ctx) {
    const auto& listValue = DCHECK_NOTNULL(list_)->eval(ctx);
    if (!listValue.isList()) {
        result_ = Value::kNullBadType;
        return result_;
    }
    const auto& list = listValue.getList();
    size_t size = list.size();
    int64_t lo, hi;
    if (lo_ == nullptr) {
        lo = 0;
    } else {
        auto loValue = DCHECK_NOTNULL(lo_)->eval(ctx);
        if (loValue.isNull()) {
            result_ = Value::kNullValue;
            return result_;
        }
        if (!loValue.isInt()) {
            result_ = Value::kNullBadType;
            return result_;
        }
        lo = loValue.getInt();
        if (lo < 0) {
            lo += size;
        }
    }

    if (hi_ == nullptr) {
        hi = size;
    } else {
        auto hiValue = DCHECK_NOTNULL(hi_)->eval(ctx);
        if (hiValue.isNull()) {
            result_ = Value::kNullValue;
            return result_;
        }
        if (!hiValue.isInt()) {
            result_ = Value::kNullBadType;
            return result_;
        }
        hi = hiValue.getInt();
        if (hi < 0) {
            hi += size;
        }
    }

    // Out-of-bound slices are simply truncated
    if (lo < 0) {
        lo = 0;
    }
    if (lo >= hi) {
        result_ = List();
        return result_;
    }
    if (static_cast<size_t>(lo) >= size) {
        result_ = List();
        return result_;
    }
    if (static_cast<size_t>(hi) >= size) {
        hi = size;
    }

    List r;
    r.values = {list.values.begin()+lo, list.values.begin()+hi};
    result_ = std::move(r);
    return result_;
}

std::string SubscriptRangeExpression::toString() const {
    std::string buf;
    buf.reserve(32);
    buf += list_->toString();
    buf += '[';
    if (lo_ != nullptr) {
        buf += lo_->toString();
    }
    buf += "..";
    if (hi_ != nullptr) {
        buf += hi_->toString();
    }
    buf += ']';
    return buf;
}

void SubscriptRangeExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

bool SubscriptRangeExpression::operator==(const Expression& rhs) const {
    if (kind() != rhs.kind()) {
        return false;
    }
    const auto &rhsSub = static_cast<const SubscriptRangeExpression&>(rhs);
    if (*list_ != *rhsSub.list_) {
        return false;
    }
    if (!checkPointer(lo_, rhsSub.lo_)) {
        return false;
    }
    if (!checkPointer(hi_, rhsSub.hi_)) {
        return false;
    }
    return true;
}

void SubscriptRangeExpression::writeTo(Encoder& encoder) const {
    encoder << kind();
    encoder << *list_;
    encoder << ((lo_ != nullptr && hi_ != nullptr) ? 2 : 1);
    if (lo_ != nullptr) {
        encoder << 0UL;
        encoder << *lo_;
    }
    if (hi_ != nullptr) {
        encoder << 1UL;
        encoder << *hi_;
    }
}

void SubscriptRangeExpression::resetFrom(Decoder& decoder) {
    list_ = decoder.readExpression(pool_);
    auto size = decoder.readSize();
    auto index = decoder.readSize();
    if (index == 0) {
        lo_ = decoder.readExpression(pool_);
    } else if (index == 1) {
        DCHECK_EQ(size, 1);
        hi_ = decoder.readExpression(pool_);
        return;
    }
    if (size == 2) {
        index = decoder.readSize();
        DCHECK_EQ(index, 1);
        hi_ = decoder.readExpression(pool_);
    }
}

}   // namespace nebula

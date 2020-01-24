/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "datatypes/Value.h"

namespace nebula {

Value::Value(Value&& rhs) : type_(Value::Type::__EMPTY__) {
    if (this == &rhs) { return; }
    if (rhs.type_ == Type::__EMPTY__) { return; }
    switch (rhs.type_) {
        case Type::nVal:
        {
            value_.nVal = std::move(rhs.value_.nVal);
            break;
        }
        case Type::vVal:
        {
            value_.vVal = std::move(rhs.value_.vVal);
            break;
        }
        case Type::bVal:
        {
            value_.bVal = std::move(rhs.value_.bVal);
            break;
        }
        case Type::iVal:
        {
            value_.iVal = std::move(rhs.value_.iVal);
            break;
        }
        case Type::fVal:
        {
            value_.fVal = std::move(rhs.value_.fVal);
            break;
        }
        case Type::sVal:
        {
            value_.sVal = std::move(rhs.value_.sVal);
            break;
        }
        case Type::tVal:
        {
            value_.tVal = std::move(rhs.value_.tVal);
            break;
        }
        case Type::dVal:
        {
            value_.dVal = std::move(rhs.value_.dVal);
            break;
        }
        case Type::dtVal:
        {
            value_.dtVal = std::move(rhs.value_.dtVal);
            break;
        }
        case Type::pathVal:
        {
            value_.pVal = std::move(rhs.value_.pVal);
            break;
        }
        case Type::listVal:
        {
            value_.listVal = std::move(rhs.value_.listVal);
            break;
        }
        case Type::mapVal:
        {
            value_.mapVal = std::move(rhs.value_.mapVal);
            break;
        }
        default:
        {
            assert(false);
            break;
        }
    }
    type_ = rhs.type_;
    rhs.clear();
}


Value::Value(const Value& rhs) : type_(Value::Type::__EMPTY__) {
    if (this == &rhs) { return; }
    if (rhs.type_ == Type::__EMPTY__) { return; }
    switch (rhs.type_) {
        case Type::nVal:
        {
            value_.nVal = rhs.value_.nVal;
            break;
        }
        case Type::vVal:
        {
            value_.vVal = rhs.value_.vVal;
            break;
        }
        case Type::bVal:
        {
            value_.bVal = rhs.value_.bVal;
            break;
        }
        case Type::iVal:
        {
            value_.iVal = rhs.value_.iVal;
            break;
        }
        case Type::fVal:
        {
            value_.fVal = rhs.value_.fVal;
            break;
        }
        case Type::sVal:
        {
            value_.sVal = rhs.value_.sVal;
            break;
        }
        case Type::tVal:
        {
            value_.tVal = rhs.value_.tVal;
            break;
        }
        case Type::dVal:
        {
            value_.dVal = rhs.value_.dVal;
            break;
        }
        case Type::dtVal:
        {
            value_.dtVal = rhs.value_.dtVal;
            break;
        }
        case Type::pathVal:
        {
            value_.pVal = rhs.value_.pVal;
            break;
        }
        case Type::listVal:
        {
            value_.listVal = std::make_unique<List>();
            *value_.listVal = *rhs.value_.listVal;
            break;
        }
        case Type::mapVal:
        {
            value_.mapVal = std::make_unique<Map>();
            *value_.mapVal = *rhs.value_.mapVal;
            break;
        }
        default:
        {
            assert(false);
            break;
        }
    }
    type_ = rhs.type_;
}


bool Value::operator==(const Value& rhs) const {
    if (type_ != rhs.type_) { return false; }
    switch (type_) {
        case Type::nVal:
        {
          return value_.nVal == rhs.value_.nVal;
        }
        case Type::vVal:
        {
          return value_.vVal == rhs.value_.vVal;
        }
        case Type::bVal:
        {
          return value_.bVal == rhs.value_.bVal;
        }
        case Type::iVal:
        {
          return value_.iVal == rhs.value_.iVal;
        }
        case Type::fVal:
        {
          return value_.fVal == rhs.value_.fVal;
        }
        case Type::sVal:
        {
          return value_.sVal == rhs.value_.sVal;
        }
        case Type::tVal:
        {
          return value_.tVal == rhs.value_.tVal;
        }
        case Type::dVal:
        {
          return value_.dVal == rhs.value_.dVal;
        }
        case Type::dtVal:
        {
          return value_.dtVal == rhs.value_.dtVal;
        }
        case Type::pathVal:
        {
          return value_.pVal == rhs.value_.pVal;
        }
        case Type::listVal:
        {
          return *value_.listVal == *rhs.value_.listVal;
        }
        case Type::mapVal:
        {
          return *value_.mapVal == *rhs.value_.mapVal;
        }
        default:
        {
          return true;
        }
    }
}


void Value::clear() {
    switch (type_) {
        case Type::nVal:
        {
            destruct(value_.nVal);
            break;
        }
        case Type::vVal:
        {
            destruct(value_.vVal);
            break;
        }
        case Type::bVal:
        {
            destruct(value_.bVal);
            break;
        }
        case Type::iVal:
        {
            destruct(value_.iVal);
            break;
        }
        case Type::fVal:
        {
            destruct(value_.fVal);
            break;
        }
        case Type::sVal:
        {
            destruct(value_.sVal);
            break;
        }
        case Type::tVal:
        {
            destruct(value_.tVal);
            break;
        }
        case Type::dVal:
        {
            destruct(value_.dVal);
            break;
        }
        case Type::dtVal:
        {
            destruct(value_.dtVal);
            break;
        }
        case Type::pathVal:
        {
            destruct(value_.pVal);
            break;
        }
        case Type::listVal:
        {
            destruct(value_.listVal);
            break;
        }
        case Type::mapVal:
        {
            destruct(value_.mapVal);
            break;
        }
        default:
        {
            assert(false);
            break;
        }
    }
    type_ = Type::__EMPTY__;
}


Value& Value::operator=(Value&& rhs) {
    if (this == &rhs) { return *this; }
    clear();
    if (rhs.type_ == Type::__EMPTY__) { return *this; }
    switch (rhs.type_) {
        case Type::nVal:
        {
            value_.nVal = std::move(rhs.value_.nVal);
            break;
        }
        case Type::vVal:
        {
            value_.vVal = std::move(rhs.value_.vVal);
            break;
        }
        case Type::bVal:
        {
            value_.bVal = std::move(rhs.value_.bVal);
            break;
        }
        case Type::iVal:
        {
            value_.iVal = std::move(rhs.value_.iVal);
            break;
        }
        case Type::fVal:
        {
            value_.fVal = std::move(rhs.value_.fVal);
            break;
        }
        case Type::sVal:
        {
            value_.sVal = std::move(rhs.value_.sVal);
            break;
        }
        case Type::tVal:
        {
            value_.tVal = std::move(rhs.value_.tVal);
            break;
        }
        case Type::dVal:
        {
            value_.dVal = std::move(rhs.value_.dVal);
            break;
        }
        case Type::dtVal:
        {
            value_.dtVal = std::move(rhs.value_.dtVal);
            break;
        }
        case Type::pathVal:
        {
            value_.pVal = std::move(rhs.value_.pVal);
            break;
        }
        case Type::listVal:
        {
            value_.listVal = std::move(rhs.value_.listVal);
            break;
        }
        case Type::mapVal:
        {
            value_.mapVal = std::move(rhs.value_.mapVal);
            break;
        }
        default:
        {
            assert(false);
            break;
        }
    }
    type_ = rhs.type_;
    rhs.clear();
    return *this;
}


Value& Value::operator=(const Value& rhs) {
    if (this == &rhs) { return *this; }
    clear();
    if (rhs.type_ == Type::__EMPTY__) { return *this; }
    switch (rhs.type_) {
        case Type::nVal:
        {
            value_.nVal = rhs.value_.nVal;
            break;
        }
        case Type::vVal:
        {
            value_.vVal = rhs.value_.vVal;
            break;
        }
        case Type::bVal:
        {
            value_.bVal = rhs.value_.bVal;
            break;
        }
        case Type::iVal:
        {
            value_.iVal = rhs.value_.iVal;
            break;
        }
        case Type::fVal:
        {
            value_.fVal = rhs.value_.fVal;
            break;
        }
        case Type::sVal:
        {
            value_.sVal = rhs.value_.sVal;
            break;
        }
        case Type::tVal:
        {
            value_.tVal = rhs.value_.tVal;
            break;
        }
        case Type::dVal:
        {
            value_.dVal = rhs.value_.dVal;
            break;
        }
        case Type::dtVal:
        {
            value_.dtVal = rhs.value_.dtVal;
            break;
        }
        case Type::pathVal:
        {
            value_.pVal = rhs.value_.pVal;
            break;
        }
        case Type::listVal:
        {
            value_.listVal = std::make_unique<List>();
            *value_.listVal = *rhs.value_.listVal;
            break;
        }
        case Type::mapVal:
        {
            value_.mapVal = std::make_unique<Map>();
            *value_.mapVal = *rhs.value_.mapVal;
            break;
        }
        default:
        {
            assert(false);
            break;
        }
    }
    type_ = rhs.type_;
    return *this;
}


void swap(Value& a, Value& b) {
  Value temp(std::move(a));
  a = std::move(b);
  b = std::move(temp);
}

}  // namespace nebula



/* Copyright (c) 2020 vesoft inc. All rights reserved.
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
        case Type::NULLVALUE:
        {
            value_.nVal = std::move(rhs.value_.nVal);
            break;
        }
        case Type::BOOL:
        {
            value_.bVal = std::move(rhs.value_.bVal);
            break;
        }
        case Type::INT:
        {
            value_.iVal = std::move(rhs.value_.iVal);
            break;
        }
        case Type::FLOAT:
        {
            value_.fVal = std::move(rhs.value_.fVal);
            break;
        }
        case Type::STRING:
        {
            value_.sVal = std::move(rhs.value_.sVal);
            break;
        }
        case Type::DATE:
        {
            value_.dVal = std::move(rhs.value_.dVal);
            break;
        }
        case Type::DATETIME:
        {
            value_.tVal = std::move(rhs.value_.tVal);
            break;
        }
        case Type::PATH:
        {
            value_.pVal = std::move(rhs.value_.pVal);
            break;
        }
        case Type::LIST:
        {
            value_.lVal = std::move(rhs.value_.lVal);
            break;
        }
        case Type::MAP:
        {
            value_.mVal = std::move(rhs.value_.mVal);
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
        case Type::NULLVALUE:
        {
            value_.nVal = rhs.value_.nVal;
            break;
        }
        case Type::BOOL:
        {
            value_.bVal = rhs.value_.bVal;
            break;
        }
        case Type::INT:
        {
            value_.iVal = rhs.value_.iVal;
            break;
        }
        case Type::FLOAT:
        {
            value_.fVal = rhs.value_.fVal;
            break;
        }
        case Type::STRING:
        {
            value_.sVal = rhs.value_.sVal;
            break;
        }
        case Type::DATE:
        {
            value_.dVal = rhs.value_.dVal;
            break;
        }
        case Type::DATETIME:
        {
            value_.tVal = rhs.value_.tVal;
            break;
        }
        case Type::PATH:
        {
            value_.pVal = rhs.value_.pVal;
            break;
        }
        case Type::LIST:
        {
            value_.lVal = std::make_unique<List>();
            *value_.lVal = *rhs.value_.lVal;
            break;
        }
        case Type::MAP:
        {
            value_.mVal = std::make_unique<Map>();
            *value_.mVal = *rhs.value_.mVal;
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

Value::Value(NullType v) : type_(Value::Type::NULLVALUE) {
    value_.nVal = v;
}

Value::Value(bool v) : type_(Value::Type::BOOL) {
    value_.bVal = v;
}

Value::Value(int64_t v) : type_(Value::Type::INT) {
    value_.iVal = v;
}

Value::Value(double v) : type_(Value::Type::FLOAT) {
    value_.fVal = v;
}

Value::Value(const std::string& v) : type_(Value::Type::STRING) {
    value_.sVal = v;
}

Value::Value(std::string&& v) : type_(Value::Type::STRING) {
    value_.sVal = std::move(v);
}

Value::Value(const Date& v) : type_(Value::Type::DATE) {
    value_.dVal = v;
}

Value::Value(Date&& v) : type_(Value::Type::DATE) {
    value_.dVal = std::move(v);
}

Value::Value(const DateTime& v) : type_(Value::Type::DATETIME) {
    value_.tVal = v;
}

Value::Value(DateTime&& v) : type_(Value::Type::DATETIME) {
    value_.tVal = std::move(v);
}

Value::Value(const Path& v) : type_(Value::Type::PATH) {
    value_.pVal = v;
}

Value::Value(Path&& v) : type_(Value::Type::PATH) {
    value_.pVal = std::move(v);
}

Value::Value(const List& v) : type_(Value::Type::LIST) {
    value_.lVal = std::make_unique<List>(v);
}

Value::Value(List&& v) : type_(Value::Type::LIST) {
    value_.lVal = std::make_unique<List>(std::move(v));
}

Value::Value(const Map& v) : type_(Value::Type::MAP) {
    value_.mVal = std::make_unique<Map>(v);
}

Value::Value(Map&& v) : type_(Value::Type::MAP) {
    value_.mVal = std::make_unique<Map>(std::move(v));
}


Value& Value::operator=(NullType v) {
    set(v);
    return *this;
}

Value& Value::operator=(bool v) {
    set(v);
    return *this;
}

Value& Value::operator=(int64_t v) {
    set(v);
    return *this;
}

Value& Value::operator=(double v) {
    set(v);
    return *this;
}

Value& Value::operator=(const std::string& v) {
    set(v);
    return *this;
}

Value& Value::operator=(std::string&& v) {
    set(std::move(v));
    return *this;
}

Value& Value::operator=(const Date& v) {
    set(v);
    return *this;
}

Value& Value::operator=(Date&& v) {
    set(std::move(v));
    return *this;
}

Value& Value::operator=(const DateTime& v) {
    set(v);
    return *this;
}

Value& Value::operator=(DateTime&& v) {
    set(std::move(v));
    return *this;
}

Value& Value::operator=(const Path& v) {
    set(v);
    return *this;
}

Value& Value::operator=(Path&& v) {
    set(std::move(v));
    return *this;
}

Value& Value::operator=(const List& v) {
    set(v);
    return *this;
}

Value& Value::operator=(List&& v) {
    set(std::move(v));
    return *this;
}

Value& Value::operator=(const Map& v) {
    set(v);
    return *this;
}

Value& Value::operator=(Map&& v) {
    set(std::move(v));
    return *this;
}


void Value::set(NullType v) {
    clear();
    type_ = Type::NULLVALUE;
    value_.nVal = v;
}

void Value::set(bool v) {
    clear();
    type_ = Type::BOOL;
    value_.bVal = v;
}

void Value::set(int64_t v) {
    clear();
    type_ = Type::INT;
    value_.iVal = v;
}

void Value::set(double v) {
    clear();
    type_ = Type::FLOAT;
    value_.fVal = v;
}

void Value::set(const std::string& v) {
    clear();
    type_ = Type::STRING;
    value_.sVal = v;
}

void Value::set(std::string&& v) {
    clear();
    type_ = Type::STRING;
    value_.sVal = std::move(v);
}

void Value::set(const Date& v) {
    clear();
    type_ = Type::DATE;
    value_.dVal = v;
}

void Value::set(Date&& v) {
    clear();
    type_ = Type::DATE;
    value_.dVal = std::move(v);
}

void Value::set(const DateTime& v) {
    clear();
    type_ = Type::DATETIME;
    value_.tVal = v;
}

void Value::set(DateTime&& v) {
    clear();
    type_ = Type::DATETIME;
    value_.tVal = v;
}

void Value::set(const Path& v) {
    clear();
    type_ = Type::PATH;
    value_.pVal = v;
}

void Value::set(Path&& v) {
    clear();
    type_ = Type::PATH;
    value_.pVal = std::move(v);
}

void Value::set(const List& v) {
    clear();
    type_ = Type::LIST;
    value_.lVal = std::make_unique<List>(v);
}

void Value::set(List&& v) {
    clear();
    type_ = Type::LIST;
    value_.lVal = std::make_unique<List>(std::move(v));
}

void Value::set(const Map& v) {
    clear();
    type_ = Type::MAP;
    value_.mVal = std::make_unique<Map>(v);
}

void Value::set(Map&& v) {
    clear();
    type_ = Type::MAP;
    value_.mVal = std::make_unique<Map>(std::move(v));
}


NullType Value::getNullType() const {
    CHECK_EQ(type_, Type::NULLVALUE);
    return value_.nVal;
}

bool Value::getBool() const {
    CHECK_EQ(type_, Type::BOOL);
    return value_.bVal;
}

int64_t Value::getInt() const {
    CHECK_EQ(type_, Type::INT);
    return value_.iVal;
}

double Value::getDouble() const {
    CHECK_EQ(type_, Type::FLOAT);
    return value_.fVal;
}

const std::string& Value::getString() const {
    CHECK_EQ(type_, Type::STRING);
    return value_.sVal;
}

const Date& Value::getDate() const {
    CHECK_EQ(type_, Type::DATE);
    return value_.dVal;
}

const DateTime& Value::getDateTime() const {
    CHECK_EQ(type_, Type::DATETIME);
    return value_.tVal;
}

const Path& Value::getPath() const {
    CHECK_EQ(type_, Type::PATH);
    return value_.pVal;
}

const List& Value::getList() const {
    CHECK_EQ(type_, Type::LIST);
    return *(value_.lVal);
}

const Map& Value::getMap() const {
    CHECK_EQ(type_, Type::MAP);
    return *(value_.mVal);
}


std::string Value::moveString() {
    CHECK_EQ(type_, Type::STRING);
    type_ = Type::__EMPTY__;
    return std::move(value_.sVal);
}

Date Value::moveDate() {
    CHECK_EQ(type_, Type::DATE);
    type_ = Type::__EMPTY__;
    return std::move(value_.dVal);
}

DateTime Value::moveDateTime() {
    CHECK_EQ(type_, Type::DATETIME);
    type_ = Type::__EMPTY__;
    return std::move(value_.tVal);
}

Path Value::movePath() {
    CHECK_EQ(type_, Type::PATH);
    type_ = Type::__EMPTY__;
    return std::move(value_.pVal);
}

List Value::moveList() {
    CHECK_EQ(type_, Type::LIST);
    type_ = Type::__EMPTY__;
    List list = std::move(*(value_.lVal));
    destruct(value_.lVal);
    return list;
}

Map Value::moveMap() {
    CHECK_EQ(type_, Type::MAP);
    type_ = Type::__EMPTY__;
    Map map = std::move(*(value_.mVal));
    destruct(value_.mVal);
    return map;
}


bool Value::operator==(const Value& rhs) const {
    if (type_ != rhs.type_) { return false; }
    switch (type_) {
        case Type::NULLVALUE:
        {
          return value_.nVal == rhs.value_.nVal;
        }
        case Type::BOOL:
        {
          return value_.bVal == rhs.value_.bVal;
        }
        case Type::INT:
        {
          return value_.iVal == rhs.value_.iVal;
        }
        case Type::FLOAT:
        {
          return value_.fVal == rhs.value_.fVal;
        }
        case Type::STRING:
        {
          return value_.sVal == rhs.value_.sVal;
        }
        case Type::DATE:
        {
          return value_.dVal == rhs.value_.dVal;
        }
        case Type::DATETIME:
        {
          return value_.tVal == rhs.value_.tVal;
        }
        case Type::PATH:
        {
          return value_.pVal == rhs.value_.pVal;
        }
        case Type::LIST:
        {
          return *value_.lVal == *rhs.value_.lVal;
        }
        case Type::MAP:
        {
          return *value_.mVal == *rhs.value_.mVal;
        }
        default:
        {
          return true;
        }
    }
}


void Value::clear() {
    switch (type_) {
        case Type::NULLVALUE:
        {
            destruct(value_.nVal);
            break;
        }
        case Type::BOOL:
        {
            destruct(value_.bVal);
            break;
        }
        case Type::INT:
        {
            destruct(value_.iVal);
            break;
        }
        case Type::FLOAT:
        {
            destruct(value_.fVal);
            break;
        }
        case Type::STRING:
        {
            destruct(value_.sVal);
            break;
        }
        case Type::DATE:
        {
            destruct(value_.dVal);
            break;
        }
        case Type::DATETIME:
        {
            destruct(value_.tVal);
            break;
        }
        case Type::PATH:
        {
            destruct(value_.pVal);
            break;
        }
        case Type::LIST:
        {
            destruct(value_.lVal);
            break;
        }
        case Type::MAP:
        {
            destruct(value_.mVal);
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
        case Type::NULLVALUE:
        {
            value_.nVal = std::move(rhs.value_.nVal);
            break;
        }
        case Type::BOOL:
        {
            value_.bVal = std::move(rhs.value_.bVal);
            break;
        }
        case Type::INT:
        {
            value_.iVal = std::move(rhs.value_.iVal);
            break;
        }
        case Type::FLOAT:
        {
            value_.fVal = std::move(rhs.value_.fVal);
            break;
        }
        case Type::STRING:
        {
            value_.sVal = std::move(rhs.value_.sVal);
            break;
        }
        case Type::DATE:
        {
            value_.dVal = std::move(rhs.value_.dVal);
            break;
        }
        case Type::DATETIME:
        {
            value_.tVal = std::move(rhs.value_.tVal);
            break;
        }
        case Type::PATH:
        {
            value_.pVal = std::move(rhs.value_.pVal);
            break;
        }
        case Type::LIST:
        {
            value_.lVal = std::move(rhs.value_.lVal);
            break;
        }
        case Type::MAP:
        {
            value_.mVal = std::move(rhs.value_.mVal);
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
        case Type::NULLVALUE:
        {
            value_.nVal = rhs.value_.nVal;
            break;
        }
        case Type::BOOL:
        {
            value_.bVal = rhs.value_.bVal;
            break;
        }
        case Type::INT:
        {
            value_.iVal = rhs.value_.iVal;
            break;
        }
        case Type::FLOAT:
        {
            value_.fVal = rhs.value_.fVal;
            break;
        }
        case Type::STRING:
        {
            value_.sVal = rhs.value_.sVal;
            break;
        }
        case Type::DATE:
        {
            value_.dVal = rhs.value_.dVal;
            break;
        }
        case Type::DATETIME:
        {
            value_.tVal = rhs.value_.tVal;
            break;
        }
        case Type::PATH:
        {
            value_.pVal = rhs.value_.pVal;
            break;
        }
        case Type::LIST:
        {
            value_.lVal = std::make_unique<List>();
            *value_.lVal = *rhs.value_.lVal;
            break;
        }
        case Type::MAP:
        {
            value_.mVal = std::make_unique<Map>();
            *value_.mVal = *rhs.value_.mVal;
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


std::ostream& operator<<(std::ostream& os, const Value::Type& type) {
    switch (type) {
        case Value::Type::__EMPTY__: {
            os << "__EMPTY__";
            break;
        }
        case Value::Type::NULLVALUE: {
            os << "NULL";
            break;
        }
        case Value::Type::BOOL: {
            os << "BOOL";
            break;
        }
        case Value::Type::INT: {
            os << "INT";
            break;
        }
        case Value::Type::FLOAT: {
            os << "FLOAT";
            break;
        }
        case Value::Type::STRING: {
            os << "STRING";
            break;
        }
        case Value::Type::DATE: {
            os << "DATE";
            break;
        }
        case Value::Type::DATETIME: {
            os << "DATETIME";
            break;
        }
        case Value::Type::PATH: {
            os << "PATH";
            break;
        }
        case Value::Type::LIST: {
            os << "LIST";
            break;
        }
        case Value::Type::MAP: {
            os << "MAP";
            break;
        }
        default: {
            os << "__UNKNOWN__";
            break;
        }
    }

    return os;
}

}  // namespace nebula



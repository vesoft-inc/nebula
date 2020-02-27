/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "datatypes/Value.h"
#include "datatypes/Map.h"
#include "datatypes/List.h"

namespace nebula {

Value::Value(Value&& rhs) : type_(Value::Type::__EMPTY__) {
    if (this == &rhs) { return; }
    if (rhs.type_ == Type::__EMPTY__) { return; }
    switch (rhs.type_) {
        case Type::NULLVALUE:
        {
            setN(std::move(rhs.value_.nVal));
            break;
        }
        case Type::BOOL:
        {
            setB(std::move(rhs.value_.bVal));
            break;
        }
        case Type::INT:
        {
            setI(std::move(rhs.value_.iVal));
            break;
        }
        case Type::FLOAT:
        {
            setF(std::move(rhs.value_.fVal));
            break;
        }
        case Type::STRING:
        {
            setS(std::move(rhs.value_.sVal));
            break;
        }
        case Type::DATE:
        {
            setD(std::move(rhs.value_.dVal));
            break;
        }
        case Type::DATETIME:
        {
            setT(std::move(rhs.value_.tVal));
            break;
        }
        case Type::PATH:
        {
            setP(std::move(rhs.value_.pVal));
            break;
        }
        case Type::LIST:
        {
            setL(std::move(rhs.value_.lVal));
            break;
        }
        case Type::MAP:
        {
            setM(std::move(rhs.value_.mVal));
            break;
        }
        default:
        {
            assert(false);
            break;
        }
    }
    rhs.clear();
}


Value::Value(const Value& rhs) : type_(Value::Type::__EMPTY__) {
    if (this == &rhs) { return; }
    if (rhs.type_ == Type::__EMPTY__) { return; }
    switch (rhs.type_) {
        case Type::NULLVALUE:
        {
            setN(rhs.value_.nVal);
            break;
        }
        case Type::BOOL:
        {
            setB(rhs.value_.bVal);
            break;
        }
        case Type::INT:
        {
            setI(rhs.value_.iVal);
            break;
        }
        case Type::FLOAT:
        {
            setF(rhs.value_.fVal);
            break;
        }
        case Type::STRING:
        {
            setS(rhs.value_.sVal);
            break;
        }
        case Type::DATE:
        {
            setD(rhs.value_.dVal);
            break;
        }
        case Type::DATETIME:
        {
            setT(rhs.value_.tVal);
            break;
        }
        case Type::PATH:
        {
            setP(rhs.value_.pVal);
            break;
        }
        case Type::LIST:
        {
            setL(rhs.value_.lVal);
            break;
        }
        case Type::MAP:
        {
            setM(rhs.value_.mVal);
            break;
        }
        default:
        {
            assert(false);
            break;
        }
    }
}

Value::Value(const NullType& v) {
    setN(v);
}

Value::Value(NullType&& v) {
    setN(std::move(v));
}

Value::Value(const bool& v) {
    setB(v);
}

Value::Value(bool&& v) {
    setB(std::move(v));
}

Value::Value(const double& v) {
    setF(v);
}

Value::Value(double&& v) {
    setF(std::move(v));
}

Value::Value(const std::string& v) {
    setS(v);
}

Value::Value(std::string&& v) {
    setS(std::move(v));
}

Value::Value(const Date& v) {
    setD(v);
}

Value::Value(Date&& v) {
    setD(std::move(v));
}

Value::Value(const DateTime& v) {
    setT(v);
}

Value::Value(DateTime&& v) {
    setT(std::move(v));
}

Value::Value(const Path& v) {
    setP(v);
}

Value::Value(Path&& v) {
    setP(std::move(v));
}

Value::Value(const List& v) {
    auto c = std::make_unique<List>(v);
    setL(std::move(c));
}

Value::Value(List&& v) {
    setL(std::make_unique<List>(std::move(v)));
}

Value::Value(const Map& v) {
    auto c = std::make_unique<Map>(v);
    setM(std::move(c));
}

Value::Value(Map&& v) {
    setM(std::make_unique<Map>(std::move(v)));
}


void Value::setNull(const NullType& v) {
    clear();
    setN(v);
}

void Value::setNull(NullType&& v) {
    clear();
    setN(std::move(v));
}

void Value::setBool(const bool& v) {
    clear();
    setB(v);
}

void Value::setBool(bool&& v) {
    clear();
    setB(std::move(v));
}

void Value::setInt(const int64_t& v) {
    clear();
    setI(v);
}

void Value::setInt(int64_t&& v) {
    clear();
    setI(std::move(v));
}

void Value::setFloat(const double& v) {
    clear();
    setF(v);
}

void Value::setFloat(double&& v) {
    clear();
    setF(std::move(v));
}

void Value::setStr(const std::string& v) {
    clear();
    setS(v);
}

void Value::setStr(std::string&& v) {
    clear();
    setS(std::move(v));
}

void Value::setDate(const Date& v) {
    clear();
    setD(v);
}

void Value::setDate(Date&& v) {
    clear();
    setD(std::move(v));
}

void Value::setDateTime(const DateTime& v) {
    clear();
    setT(v);
}

void Value::setDateTime(DateTime&& v) {
    clear();
    setT(std::move(v));
}

void Value::setPath(const Path& v) {
    clear();
    setP(v);
}

void Value::setPath(Path&& v) {
    clear();
    setP(std::move(v));
}

void Value::setList(const List& v) {
    clear();
    setL(v);
}

void Value::setList(List&& v) {
    clear();
    setL(std::move(v));
}

void Value::setMap(const Map& v) {
    clear();
    setM(v);
}

void Value::setMap(Map&& v) {
    clear();
    setM(std::move(v));
}


const NullType& Value::getNull() const {
    CHECK_EQ(type_, Type::NULLVALUE);
    return value_.nVal;
}

const bool& Value::getBool() const {
    CHECK_EQ(type_, Type::BOOL);
    return value_.bVal;
}

const int64_t& Value::getInt() const {
    CHECK_EQ(type_, Type::INT);
    return value_.iVal;
}

const double& Value::getFloat() const {
    CHECK_EQ(type_, Type::FLOAT);
    return value_.fVal;
}

const std::string& Value::getStr() const {
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


NullType& Value::mutableNull() {
    CHECK_EQ(type_, Type::NULLVALUE);
    return value_.nVal;
}

bool& Value::mutableBool() {
    CHECK_EQ(type_, Type::BOOL);
    return value_.bVal;
}

int64_t& Value::mutableInt() {
    CHECK_EQ(type_, Type::INT);
    return value_.iVal;
}

double& Value::mutableFloat() {
    CHECK_EQ(type_, Type::FLOAT);
    return value_.fVal;
}

std::string& Value::mutableStr() {
    CHECK_EQ(type_, Type::STRING);
    return value_.sVal;
}

Date& Value::mutableDate() {
    CHECK_EQ(type_, Type::DATE);
    return value_.dVal;
}

DateTime& Value::mutableDateTime() {
    CHECK_EQ(type_, Type::DATETIME);
    return value_.tVal;
}

Path& Value::mutablePath() {
    CHECK_EQ(type_, Type::PATH);
    return value_.pVal;
}

List& Value::mutableList() {
    CHECK_EQ(type_, Type::LIST);
    return *(value_.lVal);
}

Map& Value::mutableMap() {
    CHECK_EQ(type_, Type::MAP);
    return *(value_.mVal);
}


NullType Value::moveNull() {
    CHECK_EQ(type_, Type::NULLVALUE);
    NullType v = std::move(value_.nVal);
    clear();
    return std::move(v);
}

bool Value::moveBool() {
    CHECK_EQ(type_, Type::BOOL);
    bool v = std::move(value_.bVal);
    clear();
    return std::move(v);
}

int64_t Value::moveInt() {
    CHECK_EQ(type_, Type::INT);
    int64_t v = std::move(value_.iVal);
    clear();
    return std::move(v);
}

double Value::moveFloat() {
    CHECK_EQ(type_, Type::FLOAT);
    double v = std::move(value_.fVal);
    clear();
    return std::move(v);
}

std::string Value::moveStr() {
    CHECK_EQ(type_, Type::STRING);
    std::string v = std::move(value_.sVal);
    clear();
    return std::move(v);
}

Date Value::moveDate() {
    CHECK_EQ(type_, Type::DATE);
    Date v = std::move(value_.dVal);
    clear();
    return std::move(v);
}

DateTime Value::moveDateTime() {
    CHECK_EQ(type_, Type::DATETIME);
    DateTime v = std::move(value_.tVal);
    clear();
    return std::move(v);
}

Path Value::movePath() {
    CHECK_EQ(type_, Type::PATH);
    Path v = std::move(value_.pVal);
    clear();
    return std::move(v);
}

List Value::moveList() {
    CHECK_EQ(type_, Type::LIST);
    List list = std::move(*(value_.lVal));
    clear();
    return std::move(list);
}

Map Value::moveMap() {
    CHECK_EQ(type_, Type::MAP);
    Map map = std::move(*(value_.mVal));
    clear();
    return std::move(map);
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
        case Type::__EMPTY__:
        {
            return;
        }
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
            setN(std::move(rhs.value_.nVal));
            break;
        }
        case Type::BOOL:
        {
            setB(std::move(rhs.value_.bVal));
            break;
        }
        case Type::INT:
        {
            setI(std::move(rhs.value_.iVal));
            break;
        }
        case Type::FLOAT:
        {
            setF(std::move(rhs.value_.fVal));
            break;
        }
        case Type::STRING:
        {
            setS(std::move(rhs.value_.sVal));
            break;
        }
        case Type::DATE:
        {
            setD(std::move(rhs.value_.dVal));
            break;
        }
        case Type::DATETIME:
        {
            setT(std::move(rhs.value_.tVal));
            break;
        }
        case Type::PATH:
        {
            setP(std::move(rhs.value_.pVal));
            break;
        }
        case Type::LIST:
        {
            setL(std::move(rhs.value_.lVal));
            break;
        }
        case Type::MAP:
        {
            setM(std::move(rhs.value_.mVal));
            break;
        }
        default:
        {
            assert(false);
            break;
        }
    }
    rhs.clear();
    return *this;
}


Value& Value::operator=(const Value& rhs) {
    if (this == &rhs) { return *this; }
    if (this == &rhs) { return *this; }
    clear();
    if (rhs.type_ == Type::__EMPTY__) { return *this; }
    switch (rhs.type_) {
        case Type::NULLVALUE:
        {
            setN(rhs.value_.nVal);
            break;
        }
        case Type::BOOL:
        {
            setB(rhs.value_.bVal);
            break;
        }
        case Type::INT:
        {
            setI(rhs.value_.iVal);
            break;
        }
        case Type::FLOAT:
        {
            setF(rhs.value_.fVal);
            break;
        }
        case Type::STRING:
        {
            setS(rhs.value_.sVal);
            break;
        }
        case Type::DATE:
        {
            setD(rhs.value_.dVal);
            break;
        }
        case Type::DATETIME:
        {
            setT(rhs.value_.tVal);
            break;
        }
        case Type::PATH:
        {
            setP(rhs.value_.pVal);
            break;
        }
        case Type::LIST:
        {
            setL(rhs.value_.lVal);
            break;
        }
        case Type::MAP:
        {
            setM(rhs.value_.mVal);
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


void Value::setN(const NullType& v) {
    type_ = Type::NULLVALUE;
    new (std::addressof(value_.nVal)) NullType(v);
}

void Value::setN(NullType&& v) {
    type_ = Type::NULLVALUE;
    new (std::addressof(value_.nVal)) NullType(std::move(v));
}

void Value::setB(const bool& v) {
    type_ = Type::BOOL;
    new (std::addressof(value_.bVal)) bool(v);                  // NOLINT
}

void Value::setB(bool&& v) {
    type_ = Type::BOOL;
    new (std::addressof(value_.bVal)) bool(std::move(v));       // NOLINT
}

void Value::setI(const int64_t& v) {
    type_ = Type::INT;
    new (std::addressof(value_.iVal)) int64_t(v);               // NOLINT
}

void Value::setI(int64_t&& v) {
    type_ = Type::INT;
    new (std::addressof(value_.iVal)) int64_t(std::move(v));    // NOLINT
}

void Value::setF(const double& v) {
    type_ = Type::FLOAT;
    new (std::addressof(value_.fVal)) double(v);                // NOLINT
}

void Value::setF(double&& v) {
    type_ = Type::FLOAT;
    new (std::addressof(value_.fVal)) double(std::move(v));     // NOLINT
}

void Value::setS(const std::string& v) {
    type_ = Type::STRING;
    new (std::addressof(value_.sVal)) std::string(v);
}

void Value::setS(std::string&& v) {
    type_ = Type::STRING;
    new (std::addressof(value_.sVal)) std::string(std::move(v));
}

void Value::setD(const Date& v) {
    type_ = Type::DATE;
    new (std::addressof(value_.dVal)) Date(v);
}

void Value::setD(Date&& v) {
    type_ = Type::DATE;
    new (std::addressof(value_.dVal)) Date(std::move(v));
}

void Value::setT(const DateTime& v) {
    type_ = Type::DATETIME;
    new (std::addressof(value_.tVal)) DateTime(v);
}

void Value::setT(DateTime&& v) {
    type_ = Type::DATETIME;
    new (std::addressof(value_.tVal)) DateTime(std::move(v));
}

void Value::setP(const Path& v) {
    type_ = Type::PATH;
    new (std::addressof(value_.pVal)) Path(v);
}

void Value::setP(Path&& v) {
    type_ = Type::PATH;
    new (std::addressof(value_.pVal)) Path(std::move(v));
}

void Value::setL(const std::unique_ptr<List>& v) {
    type_ = Type::LIST;
    new (std::addressof(value_.lVal)) std::unique_ptr<List>(new List(*v));
}

void Value::setL(std::unique_ptr<List>&& v) {
    type_ = Type::LIST;
    new (std::addressof(value_.lVal)) std::unique_ptr<List>(std::move(v));
}

void Value::setL(const List& v) {
    type_ = Type::LIST;
    new (std::addressof(value_.lVal)) std::unique_ptr<List>(new List(v));
}

void Value::setL(List&& v) {
    type_ = Type::LIST;
    new (std::addressof(value_.lVal)) std::unique_ptr<List>(new List(std::move(v)));
}

void Value::setM(const std::unique_ptr<Map>& v) {
    type_ = Type::MAP;
    new (std::addressof(value_.mVal)) std::unique_ptr<Map>(new Map(*v));
}

void Value::setM(std::unique_ptr<Map>&& v) {
    type_ = Type::MAP;
    new (std::addressof(value_.mVal)) std::unique_ptr<Map>(std::move(v));
}

void Value::setM(const Map& v) {
    type_ = Type::MAP;
    new (std::addressof(value_.mVal)) std::unique_ptr<Map>(new Map(v));
}

void Value::setM(Map&& v) {
    type_ = Type::MAP;
    new (std::addressof(value_.mVal)) std::unique_ptr<Map>(new Map(std::move(v)));
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



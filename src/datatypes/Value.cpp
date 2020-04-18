/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "datatypes/Value.h"
#include <folly/hash/Hash.h>
#include "datatypes/List.h"
#include "datatypes/Map.h"
#include "datatypes/Set.h"
#include "datatypes/DataSet.h"
#include "datatypes/Vertex.h"
#include "datatypes/Edge.h"
#include "datatypes/Path.h"

namespace std {

std::size_t hash<nebula::Value>::operator()(const nebula::Value& v) const noexcept {
    switch (v.type()) {
        case nebula::Value::Type::__EMPTY__: {
            return 0;
        }
        case nebula::Value::Type::NULLVALUE: {
            return folly::hash::fnv64_buf(reinterpret_cast<const void*>(&v.getNull()),
                                          sizeof(nebula::NullType));
        }
        case nebula::Value::Type::BOOL: {
            return folly::hash::fnv64_buf(reinterpret_cast<const void*>(&v.getBool()),
                                          sizeof(bool));
        }
        case nebula::Value::Type::INT: {
            return folly::hash::fnv64_buf(reinterpret_cast<const void*>(&v.getInt()),
                                          sizeof(int64_t));
        }
        case nebula::Value::Type::FLOAT: {
            return folly::hash::fnv64_buf(reinterpret_cast<const void*>(&v.getFloat()),
                                          sizeof(double));
        }
        case nebula::Value::Type::STRING: {
            return folly::hash::fnv64(v.getStr());
        }
        case nebula::Value::Type::DATE: {
            return hash<nebula::Date>()(v.getDate());
        }
        case nebula::Value::Type::DATETIME: {
            return hash<nebula::DateTime>()(v.getDateTime());
        }
        case nebula::Value::Type::VERTEX: {
            return hash<nebula::Vertex>()(v.getVertex());
        }
        case nebula::Value::Type::EDGE: {
            return hash<nebula::Edge>()(v.getEdge());
        }
        case nebula::Value::Type::PATH: {
            return hash<nebula::Path>()(v.getPath());
        }
        case nebula::Value::Type::LIST: {
            LOG(FATAL) << "Hash for LIST has not been implemented";
        }
        case nebula::Value::Type::MAP: {
            LOG(FATAL) << "Hash for MAP has not been implemented";
        }
        case nebula::Value::Type::SET: {
            LOG(FATAL) << "Hash for SET has not been implemented";
        }
        case nebula::Value::Type::DATASET: {
            LOG(FATAL) << "Hash for DATASET has not been implemented";
        }
        default: {
            LOG(FATAL) << "Unknown type";
        }
    }
}

}  // namespace std


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
        case Type::VERTEX:
        {
            setV(std::move(rhs.value_.vVal));
            break;
        }
        case Type::EDGE:
        {
            setE(std::move(rhs.value_.eVal));
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
        case Type::SET:
        {
            setU(std::move(rhs.value_.uVal));
            break;
        }
        case Type::DATASET:
        {
            setG(std::move(rhs.value_.gVal));
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
        case Type::VERTEX:
        {
            setV(rhs.value_.vVal);
            break;
        }
        case Type::EDGE:
        {
            setE(rhs.value_.eVal);
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
        case Type::SET:
        {
            setU(rhs.value_.uVal);
            break;
        }
        case Type::DATASET:
        {
            setG(rhs.value_.gVal);
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

Value::Value(const int8_t& v) {
    setI(v);
}

Value::Value(int8_t&& v) {
    setI(std::move(v));
}

Value::Value(const int16_t& v) {
    setI(v);
}

Value::Value(int16_t&& v) {
    setI(std::move(v));
}

Value::Value(const int32_t& v) {
    setI(v);
}

Value::Value(int32_t&& v) {
    setI(std::move(v));
}

Value::Value(const int64_t& v) {
    setI(v);
}

Value::Value(int64_t&& v) {
    setI(std::move(v));
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

Value::Value(const char* v) {
    setS(v);
}

Value::Value(folly::StringPiece v) {
    setS(v);
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

Value::Value(const Vertex& v) {
    setV(v);
}

Value::Value(Vertex&& v) {
    setV(std::move(v));
}

Value::Value(const Edge& v) {
    setE(v);
}

Value::Value(Edge&& v) {
    setE(std::move(v));
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

Value::Value(const Set& v) {
    auto c = std::make_unique<Set>(v);
    setU(std::move(c));
}

Value::Value(Set&& v) {
    setU(std::make_unique<Set>(std::move(v)));
}

Value::Value(const DataSet& v) {
    auto c = std::make_unique<DataSet>(v);
    setG(std::move(c));
}

Value::Value(DataSet&& v) {
    setG(std::make_unique<DataSet>(std::move(v)));
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

void Value::setInt(const int8_t& v) {
    clear();
    setI(v);
}

void Value::setInt(int8_t&& v) {
    clear();
    setI(std::move(v));
}

void Value::setInt(const int16_t& v) {
    clear();
    setI(v);
}

void Value::setInt(int16_t&& v) {
    clear();
    setI(std::move(v));
}

void Value::setInt(const int32_t& v) {
    clear();
    setI(v);
}

void Value::setInt(int32_t&& v) {
    clear();
    setI(std::move(v));
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

void Value::setStr(const char* v) {
    clear();
    setS(v);
}

void Value::setStr(folly::StringPiece v) {
    clear();
    setS(v);
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

void Value::setVertex(const Vertex& v) {
    clear();
    setV(v);
}

void Value::setVertex(Vertex&& v) {
    clear();
    setV(std::move(v));
}

void Value::setVertex(std::unique_ptr<Vertex>&& v) {
    clear();
    setV(std::move(v));
}

void Value::setEdge(const Edge& v) {
    clear();
    setE(v);
}

void Value::setEdge(Edge&& v) {
    clear();
    setE(std::move(v));
}

void Value::setEdge(std::unique_ptr<Edge>&& v) {
    clear();
    setE(std::move(v));
}

void Value::setPath(const Path& v) {
    clear();
    setP(v);
}

void Value::setPath(Path&& v) {
    clear();
    setP(std::move(v));
}

void Value::setPath(std::unique_ptr<Path>&& v) {
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

void Value::setList(std::unique_ptr<List>&& v) {
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

void Value::setMap(std::unique_ptr<Map>&& v) {
    clear();
    setM(std::move(v));
}

void Value::setSet(const Set& v) {
    clear();
    setU(v);
}

void Value::setSet(Set&& v) {
    clear();
    setU(std::move(v));
}

void Value::setSet(std::unique_ptr<Set>&& v) {
    clear();
    setU(std::move(v));
}

void Value::setDataSet(const DataSet& v) {
    clear();
    setG(v);
}

void Value::setDataSet(DataSet&& v) {
    clear();
    setG(std::move(v));
}

void Value::setDataSet(std::unique_ptr<DataSet>&& v) {
    clear();
    setG(std::move(v));
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

const Vertex& Value::getVertex() const {
    CHECK_EQ(type_, Type::VERTEX);
    return *(value_.vVal);
}

const Vertex* Value::getVertexPtr() const {
    CHECK_EQ(type_, Type::VERTEX);
    return value_.vVal.get();
}

const Edge& Value::getEdge() const {
    CHECK_EQ(type_, Type::EDGE);
    return *(value_.eVal);
}

const Edge* Value::getEdgePtr() const {
    CHECK_EQ(type_, Type::EDGE);
    return value_.eVal.get();
}

const Path& Value::getPath() const {
    CHECK_EQ(type_, Type::PATH);
    return *(value_.pVal);
}

const Path* Value::getPathPtr() const {
    CHECK_EQ(type_, Type::PATH);
    return value_.pVal.get();
}

const List& Value::getList() const {
    CHECK_EQ(type_, Type::LIST);
    return *(value_.lVal);
}

const List* Value::getListPtr() const {
    CHECK_EQ(type_, Type::LIST);
    return value_.lVal.get();
}

const Map& Value::getMap() const {
    CHECK_EQ(type_, Type::MAP);
    return *(value_.mVal);
}

const Map* Value::getMapPtr() const {
    CHECK_EQ(type_, Type::MAP);
    return value_.mVal.get();
}

const Set& Value::getSet() const {
    CHECK_EQ(type_, Type::SET);
    return *(value_.uVal);
}

const Set* Value::getSetPtr() const {
    CHECK_EQ(type_, Type::SET);
    return value_.uVal.get();
}

const DataSet& Value::getDataSet() const {
    CHECK_EQ(type_, Type::DATASET);
    return *(value_.gVal);
}

const DataSet* Value::getDataSetPtr() const {
    CHECK_EQ(type_, Type::DATASET);
    return value_.gVal.get();
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

Vertex& Value::mutableVertex() {
    CHECK_EQ(type_, Type::VERTEX);
    return *(value_.vVal);
}

Edge& Value::mutableEdge() {
    CHECK_EQ(type_, Type::EDGE);
    return *(value_.eVal);
}

Path& Value::mutablePath() {
    CHECK_EQ(type_, Type::PATH);
    return *(value_.pVal);
}

List& Value::mutableList() {
    CHECK_EQ(type_, Type::LIST);
    return *(value_.lVal);
}

Map& Value::mutableMap() {
    CHECK_EQ(type_, Type::MAP);
    return *(value_.mVal);
}

Set& Value::mutableSet() {
    CHECK_EQ(type_, Type::SET);
    return *(value_.uVal);
}

DataSet& Value::mutableDataSet() {
    CHECK_EQ(type_, Type::DATASET);
    return *(value_.gVal);
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
    return v;
}

Date Value::moveDate() {
    CHECK_EQ(type_, Type::DATE);
    Date v = std::move(value_.dVal);
    clear();
    return v;
}

DateTime Value::moveDateTime() {
    CHECK_EQ(type_, Type::DATETIME);
    DateTime v = std::move(value_.tVal);
    clear();
    return v;
}

Vertex Value::moveVertex() {
    CHECK_EQ(type_, Type::VERTEX);
    Vertex v = std::move(*(value_.vVal));
    clear();
    return v;
}

Edge Value::moveEdge() {
    CHECK_EQ(type_, Type::EDGE);
    Edge v = std::move(*(value_.eVal));
    clear();
    return v;
}

Path Value::movePath() {
    CHECK_EQ(type_, Type::PATH);
    Path v = std::move(*(value_.pVal));
    clear();
    return v;
}

List Value::moveList() {
    CHECK_EQ(type_, Type::LIST);
    List list = std::move(*(value_.lVal));
    clear();
    return list;
}

Map Value::moveMap() {
    CHECK_EQ(type_, Type::MAP);
    Map map = std::move(*(value_.mVal));
    clear();
    return map;
}

Set Value::moveSet() {
    CHECK_EQ(type_, Type::SET);
    Set set = std::move(*(value_.uVal));
    clear();
    return set;
}

DataSet Value::moveDataSet() {
    CHECK_EQ(type_, Type::DATASET);
    DataSet ds = std::move(*(value_.gVal));
    clear();
    return std::move(ds);
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
        case Type::VERTEX:
        {
          return *value_.vVal == *rhs.value_.vVal;
        }
        case Type::EDGE:
        {
          return *value_.eVal == *rhs.value_.eVal;
        }
        case Type::PATH:
        {
          return *value_.pVal == *rhs.value_.pVal;
        }
        case Type::LIST:
        {
          return *value_.lVal == *rhs.value_.lVal;
        }
        case Type::MAP:
        {
          return *value_.mVal == *rhs.value_.mVal;
        }
        case Type::SET:
        {
          return *value_.uVal == *rhs.value_.uVal;
        }
        case Type::DATASET:
        {
          return *value_.gVal == *rhs.value_.gVal;
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
        case Type::VERTEX:
        {
            destruct(value_.vVal);
            break;
        }
        case Type::EDGE:
        {
            destruct(value_.eVal);
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
        case Type::SET:
        {
            destruct(value_.uVal);
            break;
        }
        case Type::DATASET:
        {
            destruct(value_.gVal);
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
        case Type::VERTEX:
        {
            setV(std::move(rhs.value_.vVal));
            break;
        }
        case Type::EDGE:
        {
            setE(std::move(rhs.value_.eVal));
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
        case Type::SET:
        {
            setU(std::move(rhs.value_.uVal));
            break;
        }
        case Type::DATASET:
        {
            setG(std::move(rhs.value_.gVal));
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
        case Type::VERTEX:
        {
            setV(rhs.value_.vVal);
            break;
        }
        case Type::EDGE:
        {
            setE(rhs.value_.eVal);
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
        case Type::SET:
        {
            setU(rhs.value_.uVal);
            break;
        }
        case Type::DATASET:
        {
            setG(rhs.value_.gVal);
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

void Value::setS(const char* v) {
    type_ = Type::STRING;
    new (std::addressof(value_.sVal)) std::string(v);
}

void Value::setS(folly::StringPiece v) {
    type_ = Type::STRING;
    new (std::addressof(value_.sVal)) std::string(v.data(), v.size());
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

void Value::setV(const std::unique_ptr<Vertex>& v) {
    type_ = Type::VERTEX;
    new (std::addressof(value_.vVal)) std::unique_ptr<Vertex>(new Vertex(*v));
}

void Value::setV(std::unique_ptr<Vertex>&& v) {
    type_ = Type::VERTEX;
    new (std::addressof(value_.vVal)) std::unique_ptr<Vertex>(std::move(v));
}

void Value::setV(const Vertex& v) {
    type_ = Type::VERTEX;
    new (std::addressof(value_.vVal)) std::unique_ptr<Vertex>(new Vertex(v));
}

void Value::setV(Vertex&& v) {
    type_ = Type::VERTEX;
    new (std::addressof(value_.vVal)) std::unique_ptr<Vertex>(new Vertex(std::move(v)));
}

void Value::setE(const std::unique_ptr<Edge>& v) {
    type_ = Type::EDGE;
    new (std::addressof(value_.eVal)) std::unique_ptr<Edge>(new Edge(*v));
}

void Value::setE(std::unique_ptr<Edge>&& v) {
    type_ = Type::EDGE;
    new (std::addressof(value_.eVal)) std::unique_ptr<Edge>(std::move(v));
}

void Value::setE(const Edge& v) {
    type_ = Type::EDGE;
    new (std::addressof(value_.eVal)) std::unique_ptr<Edge>(new Edge(v));
}

void Value::setE(Edge&& v) {
    type_ = Type::EDGE;
    new (std::addressof(value_.eVal)) std::unique_ptr<Edge>(new Edge(std::move(v)));
}

void Value::setP(const std::unique_ptr<Path>& v) {
    type_ = Type::PATH;
    new (std::addressof(value_.pVal)) std::unique_ptr<Path>(new Path(*v));
}

void Value::setP(std::unique_ptr<Path>&& v) {
    type_ = Type::PATH;
    new (std::addressof(value_.pVal)) std::unique_ptr<Path>(std::move(v));
}

void Value::setP(const Path& v) {
    type_ = Type::PATH;
    new (std::addressof(value_.pVal)) std::unique_ptr<Path>(new Path(v));
}

void Value::setP(Path&& v) {
    type_ = Type::PATH;
    new (std::addressof(value_.pVal)) std::unique_ptr<Path>(new Path(std::move(v)));
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

void Value::setU(const std::unique_ptr<Set>& v) {
    type_ = Type::SET;
    new (std::addressof(value_.uVal)) std::unique_ptr<Set>(new Set(*v));
}

void Value::setU(std::unique_ptr<Set>&& v) {
    type_ = Type::SET;
    new (std::addressof(value_.uVal)) std::unique_ptr<Set>(std::move(v));
}

void Value::setU(const Set& v) {
    type_ = Type::SET;
    new (std::addressof(value_.uVal)) std::unique_ptr<Set>(new Set(v));
}

void Value::setU(Set&& v) {
    type_ = Type::SET;
    new (std::addressof(value_.vVal)) std::unique_ptr<Set>(new Set(std::move(v)));
}

void Value::setG(const std::unique_ptr<DataSet>& v) {
    type_ = Type::DATASET;
    new (std::addressof(value_.gVal)) std::unique_ptr<DataSet>(new DataSet(*v));
}

void Value::setG(std::unique_ptr<DataSet>&& v) {
    type_ = Type::DATASET;
    new (std::addressof(value_.gVal)) std::unique_ptr<DataSet>(std::move(v));
}

void Value::setG(const DataSet& v) {
    type_ = Type::DATASET;
    new (std::addressof(value_.gVal)) std::unique_ptr<DataSet>(new DataSet(v));
}

void Value::setG(DataSet&& v) {
    type_ = Type::DATASET;
    new (std::addressof(value_.gVal)) std::unique_ptr<DataSet>(new DataSet(std::move(v)));
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
        case Value::Type::VERTEX: {
            os << "VERTEX";
            break;
        }
        case Value::Type::EDGE: {
            os << "EDGE";
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
        case Value::Type::SET: {
            os << "SET";
            break;
        }
        case Value::Type::DATASET: {
            os << "DATASET";
            break;
        }
        default: {
            os << "__UNKNOWN__";
            break;
        }
    }

    return os;
}


Value operator+(const Value& left, const Value& right) {
    if (left.isNull() || right.isNull()) {
        return Value(NullType::NaN);
    }

    switch (left.type()) {
        case Value::Type::BOOL: {
            switch (right.type()) {
                case Value::Type::STRING: {
                    return folly::stringPrintf("%s%s",
                                               left.getBool() ? "true" : "false",
                                               right.getStr().c_str());
                }
                default: {
                    return Value(NullType::BAD_TYPE);
                }
            }
        }
        case Value::Type::INT: {
            switch (right.type()) {
                case Value::Type::INT: {
                    return left.getInt() + right.getInt();
                }
                case Value::Type::FLOAT: {
                    return left.getInt() + right.getFloat();
                }
                case Value::Type::STRING: {
                    return folly::stringPrintf("%ld%s",
                                               left.getInt(),
                                               right.getStr().c_str());
                }
                case Value::Type::DATE: {
                    return right.getDate() + left.getInt();
                }
                default: {
                    return Value(NullType::BAD_TYPE);
                }
            }
        }
        case Value::Type::FLOAT: {
            switch (right.type()) {
                case Value::Type::INT: {
                    return left.getFloat() + right.getInt();
                }
                case Value::Type::FLOAT: {
                    return left.getFloat() + right.getFloat();
                }
                case Value::Type::STRING: {
                    return folly::stringPrintf("%lf%s",
                                               left.getFloat(),
                                               right.getStr().c_str());
                }
                default: {
                    return Value(NullType::BAD_TYPE);
                }
            }
        }
        case Value::Type::STRING: {
            switch (right.type()) {
                case Value::Type::BOOL: {
                    return folly::stringPrintf("%s%s",
                                               left.getStr().c_str(),
                                               right.getBool() ? "true" : "false");
                }
                case Value::Type::INT: {
                    return folly::stringPrintf("%s%ld",
                                               left.getStr().c_str(),
                                               right.getInt());
                }
                case Value::Type::FLOAT: {
                    return folly::stringPrintf("%s%lf",
                                               left.getStr().c_str(),
                                               right.getFloat());
                }
                case Value::Type::STRING: {
                    return left.getStr() + right.getStr();
                }
                case Value::Type::DATE: {
                    return left.getStr() + right.getDate().toString();
                }
                case Value::Type::DATETIME: {
                    return left.getStr() + right.getDateTime().toString();
                }
                default: {
                    return Value(NullType::BAD_TYPE);
                }
            }
        }
        case Value::Type::DATE: {
            switch (right.type()) {
                case Value::Type::INT: {
                    return left.getDate() + right.getInt();
                }
                case Value::Type::STRING: {
                    return left.getDate().toString() + right.getStr();
                }
                default: {
                    return Value(NullType::BAD_TYPE);
                }
            }
        }
        case Value::Type::DATETIME: {
            switch (right.type()) {
                case Value::Type::STRING: {
                    return left.getDateTime().toString() + right.getStr();
                }
                default: {
                    return Value(NullType::BAD_TYPE);
                }
            }
        }
        default: {
            return Value(NullType::BAD_TYPE);
        }
    }
}


Value operator-(const Value& left, const Value& right) {
    if (left.isNull() || right.isNull()) {
        return Value(NullType::NaN);
    }

    switch (left.type()) {
        case Value::Type::INT: {
            switch (right.type()) {
                case Value::Type::INT: {
                    return left.getInt() - right.getInt();
                }
                case Value::Type::FLOAT: {
                    return left.getInt() - right.getFloat();
                }
                default: {
                    return Value(NullType::BAD_TYPE);
                }
            }
        }
        case Value::Type::FLOAT: {
            switch (right.type()) {
                case Value::Type::INT: {
                    return left.getFloat() - right.getInt();
                }
                case Value::Type::FLOAT: {
                    return left.getFloat() - right.getFloat();
                }
                default: {
                    return Value(NullType::BAD_TYPE);
                }
            }
        }
        case Value::Type::DATE: {
            switch (right.type()) {
                case Value::Type::INT: {
                    return left.getDate() - right.getInt();
                }
                case Value::Type::DATE: {
                    return left.getDate().toInt() - right.getDate().toInt();
                }
                default: {
                    return Value(NullType::BAD_TYPE);
                }
            }
        }
        default: {
            return Value(NullType::BAD_TYPE);
        }
    }
}


Value operator*(const Value& left, const Value& right) {
    if (left.isNull() || right.isNull()) {
        return Value(NullType::NaN);
    }

    switch (left.type()) {
        case Value::Type::INT: {
            switch (right.type()) {
                case Value::Type::INT: {
                    return left.getInt() * right.getInt();
                }
                case Value::Type::FLOAT: {
                    return left.getInt() * right.getFloat();
                }
                default: {
                    return Value(NullType::BAD_TYPE);
                }
            }
        }
        case Value::Type::FLOAT: {
            switch (right.type()) {
                case Value::Type::INT: {
                    return left.getFloat() * right.getInt();
                }
                case Value::Type::FLOAT: {
                    return left.getFloat() * right.getFloat();
                }
                default: {
                    return Value(NullType::BAD_TYPE);
                }
            }
        }
        default: {
            return Value(NullType::BAD_TYPE);
        }
    }
}


Value operator/(const Value& left, const Value& right) {
    if (left.isNull() || right.isNull()) {
        return Value(NullType::NaN);
    }

    switch (left.type()) {
        case Value::Type::INT: {
            switch (right.type()) {
                case Value::Type::INT: {
                    int64_t denom = right.getInt();
                    if (denom != 0) {
                        return left.getInt() / denom;
                    } else {
                        return Value(NullType::DIV_BY_ZERO);
                    }
                }
                case Value::Type::FLOAT: {
                    double denom = right.getFloat();
                    if (denom != 0.0) {
                        return left.getInt() / denom;
                    } else {
                        return Value(NullType::DIV_BY_ZERO);
                    }
                }
                default: {
                    return Value(NullType::BAD_TYPE);
                }
            }
        }
        case Value::Type::FLOAT: {
            switch (right.type()) {
                case Value::Type::INT: {
                    int64_t denom = right.getInt();
                    if (denom != 0) {
                        return left.getFloat() / denom;
                    } else {
                        return Value(NullType::DIV_BY_ZERO);
                    }
                }
                case Value::Type::FLOAT: {
                    double denom = right.getFloat();
                    if (denom != 0.0) {
                        return left.getFloat() / denom;
                    } else {
                        return Value(NullType::DIV_BY_ZERO);
                    }
                }
                default: {
                    return Value(NullType::BAD_TYPE);
                }
            }
        }
        default: {
            return Value(NullType::BAD_TYPE);
        }
    }
}

}  // namespace nebula



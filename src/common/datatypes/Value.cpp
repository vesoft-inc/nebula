/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/datatypes/Value.h"

#include <folly/String.h>
#include <folly/hash/Hash.h>
#include <glog/logging.h>

#include <memory>
#include <string>
#include <utility>

#include "common/datatypes/DataSet.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/Geography.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Path.h"
#include "common/datatypes/Set.h"
#include "common/datatypes/Vertex.h"

namespace nebula {

const Value Value::kEmpty;
const Value Value::kNullValue(NullType::__NULL__);
const Value Value::kNullNaN(NullType::NaN);
const Value Value::kNullBadData(NullType::BAD_DATA);
const Value Value::kNullBadType(NullType::BAD_TYPE);
const Value Value::kNullOverflow(NullType::ERR_OVERFLOW);
const Value Value::kNullUnknownProp(NullType::UNKNOWN_PROP);
const Value Value::kNullDivByZero(NullType::DIV_BY_ZERO);
const Value Value::kNullOutOfRange(NullType::OUT_OF_RANGE);

const uint64_t Value::kEmptyNullType = Value::Type::__EMPTY__ | Value::Type::NULLVALUE;
const uint64_t Value::kNumericType = Value::Type::INT | Value::Type::FLOAT;

Value::Value(Value&& rhs) noexcept : type_(Value::Type::__EMPTY__) {
  memcpy(reinterpret_cast<void*>(this), reinterpret_cast<const void*>(&rhs), sizeof(*this));
  rhs.type_ = Type::__EMPTY__;
}

Value::Value(const Value& rhs) : type_(Value::Type::__EMPTY__) {
  if (this == &rhs) {
    return;
  }
  if (rhs.type_ == Type::__EMPTY__) {
    return;
  }
  switch (rhs.type_) {
    case Type::NULLVALUE: {
      setN(rhs.value_.nVal);
      break;
    }
    case Type::BOOL: {
      setB(rhs.value_.bVal);
      break;
    }
    case Type::INT: {
      setI(rhs.value_.iVal);
      break;
    }
    case Type::FLOAT: {
      setF(rhs.value_.fVal);
      break;
    }
    case Type::STRING: {
      setS(*rhs.value_.sVal);
      break;
    }
    case Type::DATE: {
      setD(rhs.value_.dVal);
      break;
    }
    case Type::TIME: {
      setT(rhs.value_.tVal);
      break;
    }
    case Type::DATETIME: {
      setDT(rhs.value_.dtVal);
      break;
    }
    case Type::VERTEX: {
      setV(rhs.value_.vVal);
      break;
    }
    case Type::EDGE: {
      setE(rhs.value_.eVal);
      break;
    }
    case Type::PATH: {
      setP(rhs.value_.pVal);
      break;
    }
    case Type::LIST: {
      setL(rhs.value_.lVal);
      break;
    }
    case Type::MAP: {
      setM(rhs.value_.mVal);
      break;
    }
    case Type::SET: {
      setU(rhs.value_.uVal);
      break;
    }
    case Type::DATASET: {
      setG(rhs.value_.gVal);
      break;
    }
    case Type::GEOGRAPHY: {
      setGG(rhs.value_.ggVal);
      break;
    }
    case Type::DURATION: {
      setDU(rhs.value_.duVal);
      break;
    }
    default: {
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

Value::Value(const Date& v) {
  setD(v);
}

Value::Value(Date&& v) {
  setD(std::move(v));
}

Value::Value(const Time& v) {
  setT(v);
}

Value::Value(Time&& v) {
  setT(std::move(v));
}

Value::Value(const DateTime& v) {
  setDT(v);
}

Value::Value(DateTime&& v) {
  setDT(std::move(v));
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

Value::Value(const Geography& v) {
  setGG(std::make_unique<Geography>(v));
}

Value::Value(Geography&& v) {
  setGG(std::make_unique<Geography>(std::move(v)));
}

Value::Value(const Duration& v) {
  setDU(std::make_unique<Duration>(v));
}

Value::Value(Duration&& v) {
  setDU(std::make_unique<Duration>(std::move(v)));
}

const std::string& Value::typeName() const {
  static const std::unordered_map<Type, std::string> typeNames = {
      {Type::__EMPTY__, "__EMPTY__"},
      {Type::NULLVALUE, "__NULL__"},
      {Type::BOOL, "bool"},
      {Type::INT, "int"},
      {Type::FLOAT, "float"},
      {Type::STRING, "string"},
      {Type::DATE, "date"},
      {Type::TIME, "time"},
      {Type::DATETIME, "datetime"},
      {Type::VERTEX, "vertex"},
      {Type::EDGE, "edge"},
      {Type::PATH, "path"},
      {Type::LIST, "list"},
      {Type::MAP, "map"},
      {Type::SET, "set"},
      {Type::DATASET, "dataset"},
      {Type::GEOGRAPHY, "geography"},
      {Type::DURATION, "duration"},
  };

  static const std::unordered_map<NullType, std::string> nullTypes = {
      {NullType::__NULL__, "__NULL__"},
      {NullType::NaN, "NaN"},
      {NullType::BAD_DATA, "BAD_DATA"},
      {NullType::BAD_TYPE, "BAD_TYPE"},
      {NullType::ERR_OVERFLOW, "ERR_OVERFLOW"},
      {NullType::UNKNOWN_PROP, "UNKNOWN_PROP"},
      {NullType::DIV_BY_ZERO, "DIV_BY_ZERO"},
  };

  static const std::string unknownType = "__UNKNOWN__";

  auto find = typeNames.find(type_);
  if (find == typeNames.end()) {
    return unknownType;
  }
  if (find->first == Type::NULLVALUE) {
    auto nullFind = nullTypes.find(value_.nVal);
    if (nullFind == nullTypes.end()) {
      return unknownType;
    }
    return nullFind->second;
  }
  return find->second;
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

void Value::setDate(const Date& v) {
  clear();
  setD(v);
}

void Value::setDate(Date&& v) {
  clear();
  setD(std::move(v));
}

void Value::setTime(const Time& v) {
  clear();
  setT(v);
}

void Value::setTime(Time&& v) {
  clear();
  setT(std::move(v));
}

void Value::setDateTime(const DateTime& v) {
  clear();
  setDT(v);
}

void Value::setDateTime(DateTime&& v) {
  clear();
  setDT(std::move(v));
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

void Value::setGeography(const Geography& v) {
  clear();
  setGG(v);
}

void Value::setGeography(Geography&& v) {
  clear();
  setGG(std::move(v));
}

void Value::setGeography(std::unique_ptr<Geography>&& v) {
  clear();
  setGG(std::move(v));
}

void Value::setDuration(const Duration& v) {
  clear();
  setDU(v);
}

void Value::setDuration(Duration&& v) {
  clear();
  setDU(std::move(v));
}

void Value::setDuration(std::unique_ptr<Duration>&& v) {
  clear();
  setDU(std::move(v));
}

const double& Value::getFloat() const {
  CHECK_EQ(type_, Type::FLOAT);
  return value_.fVal;
}

const std::string& Value::getStr() const {
  CHECK_EQ(type_, Type::STRING);
  return *value_.sVal;
}

const Date& Value::getDate() const {
  CHECK_EQ(type_, Type::DATE);
  return value_.dVal;
}

const Time& Value::getTime() const {
  CHECK_EQ(type_, Type::TIME);
  return value_.tVal;
}

const DateTime& Value::getDateTime() const {
  CHECK_EQ(type_, Type::DATETIME);
  return value_.dtVal;
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

const Geography& Value::getGeography() const {
  CHECK_EQ(type_, Type::GEOGRAPHY);
  return *(value_.ggVal);
}

const Geography* Value::getGeographyPtr() const {
  CHECK_EQ(type_, Type::GEOGRAPHY);
  return value_.ggVal.get();
}

const Duration& Value::getDuration() const {
  CHECK_EQ(type_, Type::DURATION);
  return *value_.duVal;
}

const Duration* Value::getDurationPtr() const {
  CHECK_EQ(type_, Type::DURATION);
  return value_.duVal.get();
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
  return *value_.sVal;
}

Date& Value::mutableDate() {
  CHECK_EQ(type_, Type::DATE);
  return value_.dVal;
}

Time& Value::mutableTime() {
  CHECK_EQ(type_, Type::TIME);
  return value_.tVal;
}

DateTime& Value::mutableDateTime() {
  CHECK_EQ(type_, Type::DATETIME);
  return value_.dtVal;
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

Geography& Value::mutableGeography() {
  CHECK_EQ(type_, Type::GEOGRAPHY);
  return *(value_.ggVal);
}

Duration& Value::mutableDuration() {
  CHECK_EQ(type_, Type::DURATION);
  return *value_.duVal;
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
  std::string v = std::move(*value_.sVal);
  clear();
  return v;
}

Date Value::moveDate() {
  CHECK_EQ(type_, Type::DATE);
  Date v = std::move(value_.dVal);
  clear();
  return v;
}

Time Value::moveTime() {
  CHECK_EQ(type_, Type::TIME);
  Time v = std::move(value_.tVal);
  clear();
  return v;
}

DateTime Value::moveDateTime() {
  CHECK_EQ(type_, Type::DATETIME);
  DateTime v = std::move(value_.dtVal);
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
  return ds;
}

Geography Value::moveGeography() {
  CHECK_EQ(type_, Type::GEOGRAPHY);
  Geography v = std::move(*(value_.ggVal));
  clear();
  return v;
}

Duration Value::moveDuration() {
  CHECK_EQ(type_, Type::DURATION);
  Duration v = std::move(*value_.duVal);
  clear();
  return v;
}

void Value::clearSlow() {
  switch (type_) {
    case Type::__EMPTY__: {
      return;
    }
    case Type::NULLVALUE: {
      destruct(value_.nVal);
      break;
    }
    case Type::BOOL: {
      destruct(value_.bVal);
      break;
    }
    case Type::INT: {
      destruct(value_.iVal);
      break;
    }
    case Type::FLOAT: {
      destruct(value_.fVal);
      break;
    }
    case Type::STRING: {
      destruct(value_.sVal);
      break;
    }
    case Type::DATE: {
      destruct(value_.dVal);
      break;
    }
    case Type::TIME: {
      destruct(value_.tVal);
      break;
    }
    case Type::DATETIME: {
      destruct(value_.dtVal);
      break;
    }
    case Type::VERTEX: {
      destruct(value_.vVal);
      break;
    }
    case Type::EDGE: {
      destruct(value_.eVal);
      break;
    }
    case Type::PATH: {
      destruct(value_.pVal);
      break;
    }
    case Type::LIST: {
      destruct(value_.lVal);
      break;
    }
    case Type::MAP: {
      destruct(value_.mVal);
      break;
    }
    case Type::SET: {
      destruct(value_.uVal);
      break;
    }
    case Type::DATASET: {
      destruct(value_.gVal);
      break;
    }
    case Type::GEOGRAPHY: {
      destruct(value_.ggVal);
      break;
    }
    case Type::DURATION: {
      destruct(value_.duVal);
      break;
    }
  }
  type_ = Type::__EMPTY__;
}

Value& Value::operator=(Value&& rhs) noexcept {
  if (this == &rhs) {
    return *this;
  }
  clear();
  if (rhs.type_ == Type::__EMPTY__) {
    return *this;
  }
  switch (rhs.type_) {
    case Type::NULLVALUE: {
      setN(std::move(rhs.value_.nVal));
      break;
    }
    case Type::BOOL: {
      setB(std::move(rhs.value_.bVal));
      break;
    }
    case Type::INT: {
      setI(std::move(rhs.value_.iVal));
      break;
    }
    case Type::FLOAT: {
      setF(std::move(rhs.value_.fVal));
      break;
    }
    case Type::STRING: {
      setS(std::move(rhs.value_.sVal));
      break;
    }
    case Type::DATE: {
      setD(std::move(rhs.value_.dVal));
      break;
    }
    case Type::TIME: {
      setT(std::move(rhs.value_.tVal));
      break;
    }
    case Type::DATETIME: {
      setDT(std::move(rhs.value_.dtVal));
      break;
    }
    case Type::VERTEX: {
      setV(std::move(rhs.value_.vVal));
      break;
    }
    case Type::EDGE: {
      setE(std::move(rhs.value_.eVal));
      break;
    }
    case Type::PATH: {
      setP(std::move(rhs.value_.pVal));
      break;
    }
    case Type::LIST: {
      setL(std::move(rhs.value_.lVal));
      break;
    }
    case Type::MAP: {
      setM(std::move(rhs.value_.mVal));
      break;
    }
    case Type::SET: {
      setU(std::move(rhs.value_.uVal));
      break;
    }
    case Type::DATASET: {
      setG(std::move(rhs.value_.gVal));
      break;
    }
    case Type::GEOGRAPHY: {
      setGG(std::move(rhs.value_.ggVal));
      break;
    }
    case Type::DURATION: {
      setDU(std::move(rhs.value_.duVal));
      break;
    }
    default: {
      assert(false);
      break;
    }
  }
  rhs.clear();
  return *this;
}

Value& Value::operator=(const Value& rhs) {
  if (this == &rhs) {
    return *this;
  }
  clear();
  if (rhs.type_ == Type::__EMPTY__) {
    return *this;
  }
  switch (rhs.type_) {
    case Type::NULLVALUE: {
      setN(rhs.value_.nVal);
      break;
    }
    case Type::BOOL: {
      setB(rhs.value_.bVal);
      break;
    }
    case Type::INT: {
      setI(rhs.value_.iVal);
      break;
    }
    case Type::FLOAT: {
      setF(rhs.value_.fVal);
      break;
    }
    case Type::STRING: {
      setS(*rhs.value_.sVal);
      break;
    }
    case Type::DATE: {
      setD(rhs.value_.dVal);
      break;
    }
    case Type::TIME: {
      setT(rhs.value_.tVal);
      break;
    }
    case Type::DATETIME: {
      setDT(rhs.value_.dtVal);
      break;
    }
    case Type::VERTEX: {
      setV(rhs.value_.vVal);
      break;
    }
    case Type::EDGE: {
      setE(rhs.value_.eVal);
      break;
    }
    case Type::PATH: {
      setP(rhs.value_.pVal);
      break;
    }
    case Type::LIST: {
      setL(rhs.value_.lVal);
      break;
    }
    case Type::MAP: {
      setM(rhs.value_.mVal);
      break;
    }
    case Type::SET: {
      setU(rhs.value_.uVal);
      break;
    }
    case Type::DATASET: {
      setG(rhs.value_.gVal);
      break;
    }
    case Type::GEOGRAPHY: {
      setGG(rhs.value_.ggVal);
      break;
    }
    case Type::DURATION: {
      setDU(rhs.value_.duVal);
      break;
    }
    default: {
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
  new (std::addressof(value_.bVal)) bool(v);  // NOLINT
}

void Value::setB(bool&& v) {
  type_ = Type::BOOL;
  new (std::addressof(value_.bVal)) bool(std::move(v));  // NOLINT
}

void Value::setI(const int64_t& v) {
  type_ = Type::INT;
  new (std::addressof(value_.iVal)) int64_t(v);  // NOLINT
}

void Value::setI(int64_t&& v) {
  type_ = Type::INT;
  new (std::addressof(value_.iVal)) int64_t(std::move(v));  // NOLINT
}

void Value::setF(const double& v) {
  type_ = Type::FLOAT;
  new (std::addressof(value_.fVal)) double(v);  // NOLINT
}

void Value::setF(double&& v) {
  type_ = Type::FLOAT;
  new (std::addressof(value_.fVal)) double(std::move(v));  // NOLINT
}

void Value::setS(std::unique_ptr<std::string> v) {
  type_ = Type::STRING;
  new (std::addressof(value_.sVal)) std::unique_ptr<std::string>(std::move(v));
}

void Value::setS(const std::string& v) {
  type_ = Type::STRING;
  new (std::addressof(value_.sVal)) std::unique_ptr<std::string>(new std::string(v));
}

void Value::setS(std::string&& v) {
  type_ = Type::STRING;
  new (std::addressof(value_.sVal)) std::unique_ptr<std::string>(new std::string(std::move(v)));
}

void Value::setS(const char* v) {
  type_ = Type::STRING;
  new (std::addressof(value_.sVal)) std::unique_ptr<std::string>(new std::string(v));
}

void Value::setD(const Date& v) {
  type_ = Type::DATE;
  new (std::addressof(value_.dVal)) Date(v);
}

void Value::setD(Date&& v) {
  type_ = Type::DATE;
  new (std::addressof(value_.dVal)) Date(std::move(v));
}

void Value::setT(const Time& v) {
  type_ = Type::TIME;
  new (std::addressof(value_.tVal)) Time(v);
}

void Value::setT(Time&& v) {
  type_ = Type::TIME;
  new (std::addressof(value_.tVal)) Time(std::move(v));
}

void Value::setDT(const DateTime& v) {
  type_ = Type::DATETIME;
  new (std::addressof(value_.dtVal)) DateTime(v);
}

void Value::setDT(DateTime&& v) {
  type_ = Type::DATETIME;
  new (std::addressof(value_.dtVal)) DateTime(std::move(v));
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

void Value::setGG(const std::unique_ptr<Geography>& v) {
  type_ = Type::GEOGRAPHY;
  new (std::addressof(value_.ggVal)) std::unique_ptr<Geography>(new Geography(*v));
}

void Value::setGG(std::unique_ptr<Geography>&& v) {
  type_ = Type::GEOGRAPHY;
  new (std::addressof(value_.ggVal)) std::unique_ptr<Geography>(std::move(v));
}

void Value::setGG(const Geography& v) {
  type_ = Type::GEOGRAPHY;
  new (std::addressof(value_.ggVal)) std::unique_ptr<Geography>(new Geography(v));
}

void Value::setGG(Geography&& v) {
  type_ = Type::GEOGRAPHY;
  new (std::addressof(value_.ggVal)) std::unique_ptr<Geography>(new Geography(std::move(v)));
}

void Value::setDU(const std::unique_ptr<Duration>& v) {
  type_ = Type::DURATION;
  new (std::addressof(value_.duVal)) std::unique_ptr<Duration>(new Duration(*v));
}

void Value::setDU(std::unique_ptr<Duration>&& v) {
  type_ = Type::DURATION;
  new (std::addressof(value_.duVal)) std::unique_ptr<Duration>(std::move(v));
}

void Value::setDU(const Duration& v) {
  type_ = Type::DURATION;
  new (std::addressof(value_.duVal)) std::unique_ptr<Duration>(new Duration(v));
}

void Value::setDU(Duration&& v) {
  type_ = Type::DURATION;
  new (std::addressof(value_.duVal)) std::unique_ptr<Duration>(new Duration(std::move(v)));
}

// Convert Nebula::Value to a value compatible with Json standard
// DATE, TIME, DATETIME will be converted to strings in UTC
// VERTEX, EDGES, PATH will be converted to objects
folly::dynamic Value::toJson() const {
  switch (type_) {
    case Value::Type::__EMPTY__: {
      return "__EMPTY__";
    }
    // Json null
    case Value::Type::NULLVALUE: {
      return folly::dynamic(nullptr);
    }
    // Json bool
    case Value::Type::BOOL: {
      return folly::dynamic(getBool());
    }
    // Json int
    case Value::Type::INT: {
      return folly::dynamic(getInt());
    }
    // json double
    case Value::Type::FLOAT: {
      return folly::dynamic(getFloat());
    }
    // json string
    case Value::Type::STRING: {
      return folly::dynamic(getStr());
    }
    // Json array
    case Value::Type::LIST: {
      return getList().toJson();
    }
    case Value::Type::SET: {
      return getSet().toJson();
    }
    // Json object
    case Value::Type::MAP: {
      return getMap().toJson();
    }
    case Value::Type::DATE: {
      return getDate().toJson();
    }
    case Value::Type::TIME: {
      return getTime().toJson();
    }
    case Value::Type::DATETIME: {
      return getDateTime().toJson();
    }
    case Value::Type::EDGE: {
      return getEdge().toJson();
    }
    case Value::Type::VERTEX: {
      return getVertex().toJson();
    }
    case Value::Type::PATH: {
      return getPath().toJson();
    }
    case Value::Type::DATASET: {
      return getDataSet().toJson();
    }
    case Value::Type::GEOGRAPHY: {
      return getGeography().toJson();
    }
    case Value::Type::DURATION: {
      return getDuration().toJson();
    }
      // no default so the compiler will warning when lack
  }

  LOG(FATAL) << "Unknown value type " << static_cast<int>(type_);
}

folly::dynamic Value::getMetaData() const {
  auto dynamicObj = folly::dynamic();
  switch (type_) {
    // Privative datatypes has no meta data
    case Value::Type::__EMPTY__:
    case Value::Type::BOOL:
    case Value::Type::INT:
    case Value::Type::FLOAT:
    case Value::Type::STRING:
    case Value::Type::DATASET:
    case Value::Type::NULLVALUE: {
      return folly::dynamic(nullptr);
    }
    // Extract the meta info of each element as the metadata of the container
    case Value::Type::LIST: {
      return getList().getMetaData();
    }
    case Value::Type::SET: {
      return getSet().getMetaData();
    }
    case Value::Type::MAP: {
      return getMap().getMetaData();
    }
    case Value::Type::DURATION:
    case Value::Type::DATE:
    case Value::Type::TIME:
    case Value::Type::DATETIME: {
      return folly::dynamic::object("type", typeName());
    }
    case Value::Type::VERTEX: {
      return getVertex().getMetaData();
    }
    case Value::Type::EDGE: {
      return getEdge().getMetaData();
    }
    case Value::Type::PATH: {
      return getPath().getMetaData();
    }
    default:
      break;
  }

  LOG(FATAL) << "Unknown value type " << static_cast<int>(type_);
}

std::string Value::toString() const {
  switch (type_) {
    case Value::Type::__EMPTY__: {
      return "__EMPTY__";
    }
    case Value::Type::NULLVALUE: {
      switch (getNull()) {
        case NullType::__NULL__:
          return "NULL";
        case NullType::BAD_DATA:
          return "__NULL_BAD_DATA__";
        case NullType::BAD_TYPE:
          return "__NULL_BAD_TYPE__";
        case NullType::DIV_BY_ZERO:
          return "__NULL_DIV_BY_ZERO__";
        case NullType::ERR_OVERFLOW:
          return "__NULL_OVERFLOW__";
        case NullType::NaN:
          return "__NULL_NaN__";
        case NullType::UNKNOWN_PROP:
          return "__NULL_UNKNOWN_PROP__";
        case NullType::OUT_OF_RANGE:
          return "__NULL_OUT_OF_RANGE__";
      }
      LOG(FATAL) << "Unknown Null type " << static_cast<int>(getNull());
    }
    case Value::Type::BOOL: {
      return getBool() ? "true" : "false";
    }
    case Value::Type::INT: {
      return folly::to<std::string>(getInt());
    }
    case Value::Type::FLOAT: {
      return folly::to<std::string>(getFloat());
    }
    case Value::Type::STRING: {
      return "\"" + getStr() + "\"";
    }
    case Value::Type::DATE: {
      return getDate().toString();
    }
    case Value::Type::TIME: {
      return getTime().toString();
    }
    case Value::Type::DATETIME: {
      return getDateTime().toString();
    }
    case Value::Type::EDGE: {
      return getEdge().toString();
    }
    case Value::Type::VERTEX: {
      return getVertex().toString();
    }
    case Value::Type::PATH: {
      return getPath().toString();
    }
    case Value::Type::LIST: {
      return getList().toString();
    }
    case Value::Type::SET: {
      return getSet().toString();
    }
    case Value::Type::MAP: {
      return getMap().toString();
    }
    case Value::Type::DATASET: {
      return getDataSet().toString();
    }
    case Value::Type::GEOGRAPHY: {
      return getGeography().toString();
    }
    case Value::Type::DURATION: {
      return getDuration().toString();
    }
      // no default so the compiler will warning when lack
  }

  LOG(FATAL) << "Unknown value type " << static_cast<int>(type_);
}

Value Value::toBool() const {
  switch (type_) {
    case Value::Type::__EMPTY__:
    case Value::Type::NULLVALUE: {
      return Value::kNullValue;
    }
    case Value::Type::BOOL: {
      return *this;
    }
    case Value::Type::STRING: {
      auto str = getStr();
      std::transform(str.begin(), str.end(), str.begin(), ::tolower);
      if (str.compare("true") == 0) {
        return true;
      }
      if (str.compare("false") == 0) {
        return false;
      }
      return Value::kNullValue;
    }
    default: {
      return Value::kNullBadType;
    }
  }
}

Value Value::toFloat() const {
  switch (type_) {
    case Value::Type::__EMPTY__:
    case Value::Type::NULLVALUE: {
      return Value::kNullValue;
    }
    case Value::Type::INT: {
      return static_cast<double>(getInt());
    }
    case Value::Type::FLOAT: {
      return *this;
    }
    case Value::Type::STRING: {
      const auto& str = getStr();
      char* pEnd;
      double val = strtod(str.c_str(), &pEnd);
      if (*pEnd != '\0') {
        return Value::kNullValue;
      }
      return val;
    }
    default: {
      return Value::kNullBadType;
    }
  }
}

Value Value::toInt() const {
  switch (type_) {
    case Value::Type::__EMPTY__:
    case Value::Type::NULLVALUE: {
      return Value::kNullValue;
    }
    case Value::Type::INT: {
      return *this;
    }
    case Value::Type::FLOAT: {
      // Check if float value is in the range of int_64
      // Return min/max int_64 value and false to accommodate Cypher
      if (getFloat() <= std::numeric_limits<int64_t>::min()) {
        return std::numeric_limits<int64_t>::min();
      } else if (getFloat() >= static_cast<double>(std::numeric_limits<int64_t>::max())) {
        return std::numeric_limits<int64_t>::max();
      }
      return static_cast<int64_t>(getFloat());
    }
    case Value::Type::STRING: {
      const auto& str = getStr();
      errno = 0;
      char* pEnd;
      auto it = std::find(str.begin(), str.end(), '.');

      auto checkSciNotation = [](std::string s) {
        auto it1 = std::find(s.begin(), s.end(), 'e');
        auto it2 = std::find(s.begin(), s.end(), 'E');

        return (it1 != s.end() || it2 != s.end());
      };

      auto isSciNotation = checkSciNotation(str);
      int64_t val = (it != str.end() ||
                     isSciNotation)  // if the string contains '.' or 'e' or 'E', parse as a double
                        ? int64_t(strtod(str.c_str(), &pEnd))        // string representing double
                        : int64_t(strtoll(str.c_str(), &pEnd, 10));  // string representing int
      if (*pEnd != '\0') {
        return Value::kNullValue;
      }

      // Check overflow
      if ((val == std::numeric_limits<int64_t>::max() ||
           val == std::numeric_limits<int64_t>::min()) &&
          errno == ERANGE) {
        return kNullOverflow;
      }
      return val;
    }
    default: {
      return Value::kNullBadType;
    }
  }
}

Value Value::toSet() const {
  switch (type_) {
    case Value::Type::__EMPTY__:
    case Value::Type::NULLVALUE: {
      return Value::kNullValue;
    }
    case Value::Type::SET: {
      return *this;
    }
    case Value::Type::LIST: {
      Set set;
      for (auto& item : getList().values) {
        set.values.emplace(item);
      }
      return set;
    }
    default: {
      return Value::kNullBadType;
    }
  }
}
Value Value::lessThan(const Value& v) const {
  if (empty() || v.empty()) {
    return (v.isNull() || isNull()) ? Value::kNullValue : Value::kEmpty;
  }
  auto vType = v.type();
  auto hasNull = (type_ | vType) & Value::Type::NULLVALUE;
  auto notSameType = type_ != vType;
  auto notBothNumeric = ((type_ | vType) & Value::kNumericType) != Value::kNumericType;
  if (hasNull || (notSameType && notBothNumeric)) {
    return Value::kNullValue;
  }

  switch (type_) {
    case Value::Type::BOOL: {
      return getBool() < v.getBool();
    }
    case Value::Type::INT: {
      switch (vType) {
        case Value::Type::INT: {
          return getInt() < v.getInt();
        }
        case Value::Type::FLOAT: {
          return (std::abs(getInt() - v.getFloat()) >= kEpsilon && getInt() < v.getFloat());
        }
        default: {
          return kNullBadType;
        }
      }
    }
    case Value::Type::FLOAT: {
      switch (vType) {
        case Value::Type::INT: {
          return (std::abs(getFloat() - v.getInt()) >= kEpsilon && getFloat() < v.getInt());
        }
        case Value::Type::FLOAT: {
          return (std::abs(getFloat() - v.getFloat()) >= kEpsilon && getFloat() < v.getFloat());
        }
        default: {
          return kNullBadType;
        }
      }
    }
    case Value::Type::STRING: {
      return getStr() < v.getStr();
    }
    case Value::Type::DATE: {
      return getDate() < v.getDate();
    }
    case Value::Type::TIME: {
      // TODO(shylock) convert to UTC then compare
      return getTime() < v.getTime();
    }
    case Value::Type::DATETIME: {
      // TODO(shylock) convert to UTC then compare
      return getDateTime() < v.getDateTime();
    }
    case Value::Type::VERTEX: {
      return getVertex() < v.getVertex();
    }
    case Value::Type::EDGE: {
      return getEdge() < v.getEdge();
    }
    case Value::Type::PATH: {
      return getPath() < v.getPath();
    }
    case Value::Type::LIST: {
      return getList() < v.getList();
    }
    case Value::Type::MAP: {
      return getMap() < v.getMap();
    }
    case Value::Type::SET: {
      return getSet() < v.getSet();
    }
    case Value::Type::DATASET: {
      return getDataSet() < v.getDataSet();
    }
    case Value::Type::GEOGRAPHY: {
      return getGeography() < v.getGeography();
    }
    case Value::Type::DURATION: {
      // Duration can't compare,
      // e.g. What is the result of `duration('P1M') < duration('P30D')`?
      return kNullBadType;
    }
    case Value::Type::NULLVALUE:
    case Value::Type::__EMPTY__: {
      return kNullBadType;
    }
  }
  DLOG(FATAL) << "Unknown type " << static_cast<int>(v.type());
  return Value::kNullBadType;
}

Value Value::equal(const Value& v) const {
  if (empty()) {
    return v.isNull() ? Value::kNullValue : v.empty();
  }
  auto vType = v.type();
  auto hasNull = (type_ | vType) & Value::Type::NULLVALUE;
  auto notSameType = type_ != vType;
  auto notBothNumeric = ((type_ | vType) & Value::kNumericType) != Value::kNumericType;
  if (hasNull) return Value::kNullValue;
  if (notSameType && notBothNumeric) {
    return false;
  }

  switch (type_) {
    case Value::Type::BOOL: {
      return getBool() == v.getBool();
    }
    case Value::Type::INT: {
      switch (vType) {
        case Value::Type::INT: {
          return getInt() == v.getInt();
        }
        case Value::Type::FLOAT: {
          return std::abs(getInt() - v.getFloat()) < kEpsilon;
        }
        default: {
          return kNullBadType;
        }
      }
    }
    case Value::Type::FLOAT: {
      switch (vType) {
        case Value::Type::INT: {
          return std::abs(getFloat() - v.getInt()) < kEpsilon;
        }
        case Value::Type::FLOAT: {
          return std::abs(getFloat() - v.getFloat()) < kEpsilon;
        }
        default: {
          return kNullBadType;
        }
      }
    }
    case Value::Type::STRING: {
      return getStr() == v.getStr();
    }
    case Value::Type::DATE: {
      return getDate() == v.getDate();
    }
    case Value::Type::TIME: {
      // TODO(shylock) convert to UTC then compare
      return getTime() == v.getTime();
    }
    case Value::Type::DATETIME: {
      // TODO(shylock) convert to UTC then compare
      return getDateTime() == v.getDateTime();
    }
    case Value::Type::VERTEX: {
      return getVertex() == v.getVertex();
    }
    case Value::Type::EDGE: {
      return getEdge() == v.getEdge();
    }
    case Value::Type::PATH: {
      return getPath() == v.getPath();
    }
    case Value::Type::LIST: {
      return getList() == v.getList();
    }
    case Value::Type::MAP: {
      return getMap() == v.getMap();
    }
    case Value::Type::SET: {
      return getSet() == v.getSet();
    }
    case Value::Type::DATASET: {
      return getDataSet() == v.getDataSet();
    }
    case Value::Type::GEOGRAPHY: {
      return getGeography() == v.getGeography();
    }
    case Value::Type::DURATION: {
      return getDuration() == v.getDuration();
    }
    case Value::Type::NULLVALUE:
    case Value::Type::__EMPTY__: {
      return false;
    }
  }
  DLOG(FATAL) << "Unknown type " << static_cast<int>(v.type());
  return Value::kNullBadType;
}

bool Value::isImplicitBool() const {
  switch (type_) {
    case Type::BOOL:
    case Type::LIST:
      return true;
    default:
      return false;
  }
}

bool Value::implicitBool() const {
  DCHECK(isImplicitBool());
  switch (type_) {
    case Type::BOOL:
      return getBool();
    case Type::LIST:
      return !getList().empty();
    default:
      LOG(FATAL) << "Impossible to reach here!";
  }
}

void swap(Value& a, Value& b) {
  Value temp(std::move(a));
  a = std::move(b);
  b = std::move(temp);
}

/*static*/ const std::string Value::toString(Type type) {
  switch (type) {
    case Value::Type::__EMPTY__: {
      return "__EMPTY__";
    }
    case Value::Type::NULLVALUE: {
      return "NULL";
    }
    case Value::Type::BOOL: {
      return "BOOL";
    }
    case Value::Type::INT: {
      return "INT";
    }
    case Value::Type::FLOAT: {
      return "FLOAT";
    }
    case Value::Type::STRING: {
      return "STRING";
    }
    case Value::Type::DATE: {
      return "DATE";
    }
    case Value::Type::TIME: {
      return "TIME";
    }
    case Value::Type::DATETIME: {
      return "DATETIME";
    }
    case Value::Type::VERTEX: {
      return "VERTEX";
    }
    case Value::Type::EDGE: {
      return "EDGE";
    }
    case Value::Type::PATH: {
      return "PATH";
    }
    case Value::Type::LIST: {
      return "LIST";
    }
    case Value::Type::MAP: {
      return "MAP";
    }
    case Value::Type::SET: {
      return "SET";
    }
    case Value::Type::DATASET: {
      return "DATASET";
    }
    case Value::Type::GEOGRAPHY: {
      return "GEOGRAPHY";
    }
    case Value::Type::DURATION: {
      return "DURATION";
    }
    default: {
      return "__UNKNOWN__";
    }
  }
}

std::ostream& operator<<(std::ostream& os, const Value::Type& type) {
  os << Value::toString(type);
  return os;
}

Value operator+(const Value& lhs, const Value& rhs) {
  if (lhs.isNull() || (lhs.empty() && !rhs.isNull())) {
    return lhs;
  }
  if (rhs.isNull() || rhs.empty()) {
    return rhs;
  }

  switch (lhs.type()) {
    case Value::Type::BOOL: {
      switch (rhs.type()) {
        case Value::Type::STRING: {
          return folly::stringPrintf(
              "%s%s", lhs.getBool() ? "true" : "false", rhs.getStr().c_str());
        }
        case Value::Type::LIST: {
          auto ret = rhs.getList();
          ret.values.insert(ret.values.begin(), lhs);
          return ret;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::INT: {
      switch (rhs.type()) {
        case Value::Type::INT: {
          int64_t lVal = lhs.getInt();
          int64_t rVal = rhs.getInt();
          int64_t sum;
          if (__builtin_add_overflow(lVal, rVal, &sum)) {
            return Value::kNullOverflow;
          }
          return sum;
        }
        case Value::Type::FLOAT: {
          return lhs.getInt() + rhs.getFloat();
        }
        case Value::Type::STRING: {
          return folly::stringPrintf("%ld%s", lhs.getInt(), rhs.getStr().c_str());
        }
        case Value::Type::DATE: {
          return rhs.getDate() + lhs.getInt();
        }
        case Value::Type::LIST: {
          auto ret = rhs.getList();
          ret.values.insert(ret.values.begin(), lhs);
          return ret;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::FLOAT: {
      switch (rhs.type()) {
        case Value::Type::INT: {
          return lhs.getFloat() + rhs.getInt();
        }
        case Value::Type::FLOAT: {
          return lhs.getFloat() + rhs.getFloat();
        }
        case Value::Type::STRING: {
          return folly::to<std::string>(lhs.getFloat()) + rhs.getStr();
        }
        case Value::Type::LIST: {
          auto ret = rhs.getList();
          ret.values.insert(ret.values.begin(), lhs);
          return ret;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::STRING: {
      switch (rhs.type()) {
        case Value::Type::BOOL: {
          return folly::stringPrintf(
              "%s%s", lhs.getStr().c_str(), rhs.getBool() ? "true" : "false");
        }
        case Value::Type::INT: {
          return folly::stringPrintf("%s%ld", lhs.getStr().c_str(), rhs.getInt());
        }
        case Value::Type::FLOAT: {
          return lhs.getStr() + folly::to<std::string>(rhs.getFloat());
        }
        case Value::Type::STRING: {
          return lhs.getStr() + rhs.getStr();
        }
        case Value::Type::DATE: {
          return lhs.getStr() + rhs.getDate().toString();
        }
        case Value::Type::TIME: {
          return lhs.getStr() + rhs.getTime().toString();
        }
        case Value::Type::DATETIME: {
          return lhs.getStr() + rhs.getDateTime().toString();
        }
        case Value::Type::LIST: {
          auto ret = rhs.getList();
          ret.values.insert(ret.values.begin(), lhs);
          return ret;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::DATE: {
      switch (rhs.type()) {
        case Value::Type::INT: {
          return lhs.getDate() + rhs.getInt();
        }
        case Value::Type::STRING: {
          return lhs.getDate().toString() + rhs.getStr();
        }
        case Value::Type::LIST: {
          auto ret = rhs.getList();
          ret.values.insert(ret.values.begin(), lhs);
          return ret;
        }
        case Value::Type::DURATION: {
          return lhs.getDate() + rhs.getDuration();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::TIME: {
      switch (rhs.type()) {
        case Value::Type::STRING: {
          return lhs.getTime().toString() + rhs.getStr();
        }
        case Value::Type::LIST: {
          auto ret = rhs.getList();
          ret.values.insert(ret.values.begin(), lhs);
          return ret;
        }
        case Value::Type::DURATION: {
          return lhs.getTime() + rhs.getDuration();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::DATETIME: {
      switch (rhs.type()) {
        case Value::Type::STRING: {
          return lhs.getDateTime().toString() + rhs.getStr();
        }
        case Value::Type::LIST: {
          auto ret = rhs.getList();
          ret.values.insert(ret.values.begin(), lhs);
          return ret;
        }
        case Value::Type::DURATION: {
          return lhs.getDateTime() + rhs.getDuration();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::LIST: {
      switch (rhs.type()) {
        case Value::Type::LIST: {
          auto ret = lhs.getList();
          auto& list = rhs.getList();
          ret.values.insert(ret.values.end(), list.values.begin(), list.values.end());
          return ret;
        }
        case Value::Type::BOOL:
        case Value::Type::INT:
        case Value::Type::FLOAT:
        case Value::Type::STRING:
        case Value::Type::DATE:
        case Value::Type::TIME:
        case Value::Type::DATETIME:
        case Value::Type::DURATION:
        case Value::Type::VERTEX:
        case Value::Type::EDGE:
        case Value::Type::PATH:
        case Value::Type::MAP:
        case Value::Type::SET: {
          auto ret = lhs.getList();
          ret.emplace_back(rhs);
          return ret;
        }
        case Value::Type::DATASET: {
          return Value::kNullBadType;
        }
        case Value::Type::GEOGRAPHY: {
          return Value::kNullBadType;
        }
        case Value::Type::__EMPTY__: {
          return Value::kEmpty;
        }
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
      }
      LOG(FATAL) << "Unknown type: " << rhs.type();
    }
    case Value::Type::VERTEX: {
      switch (rhs.type()) {
        case Value::Type::LIST: {
          auto ret = rhs.getList();
          ret.values.insert(ret.values.begin(), lhs);
          return ret;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::EDGE: {
      switch (rhs.type()) {
        case Value::Type::LIST: {
          auto ret = rhs.getList();
          ret.values.insert(ret.values.begin(), lhs);
          return ret;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::PATH: {
      switch (rhs.type()) {
        case Value::Type::LIST: {
          auto ret = rhs.getList();
          ret.values.insert(ret.values.begin(), lhs);
          return ret;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::MAP: {
      switch (rhs.type()) {
        case Value::Type::LIST: {
          auto ret = rhs.getList();
          ret.values.insert(ret.values.begin(), lhs);
          return ret;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::SET: {
      switch (rhs.type()) {
        case Value::Type::LIST: {
          auto ret = rhs.getList();
          ret.values.insert(ret.values.begin(), lhs);
          return ret;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::DURATION: {
      switch (rhs.type()) {
        case Value::Type::DURATION: {
          return lhs.getDuration() + rhs.getDuration();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    default: {
      return Value::kNullBadType;
    }
  }
}

Value operator-(const Value& lhs, const Value& rhs) {
  if (lhs.isNull() || (lhs.empty() && !rhs.isNull())) {
    return lhs;
  }
  if (rhs.isNull() || rhs.empty()) {
    return rhs;
  }

  switch (lhs.type()) {
    case Value::Type::INT: {
      switch (rhs.type()) {
        case Value::Type::INT: {
          int64_t lVal = lhs.getInt();
          int64_t rVal = rhs.getInt();
          int64_t res;
          if (__builtin_sub_overflow(lVal, rVal, &res)) {
            return Value::kNullOverflow;
          }
          return res;
        }
        case Value::Type::FLOAT: {
          return lhs.getInt() - rhs.getFloat();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::FLOAT: {
      switch (rhs.type()) {
        case Value::Type::INT: {
          return lhs.getFloat() - rhs.getInt();
        }
        case Value::Type::FLOAT: {
          return lhs.getFloat() - rhs.getFloat();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::DATE: {
      switch (rhs.type()) {
        case Value::Type::INT: {
          return lhs.getDate() - rhs.getInt();
        }
        case Value::Type::DATE: {
          return lhs.getDate().toInt() - rhs.getDate().toInt();
        }
        case Value::Type::DURATION: {
          return lhs.getDate() - rhs.getDuration();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::TIME: {
      switch (rhs.type()) {
        case Value::Type::DURATION: {
          return lhs.getTime() - rhs.getDuration();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::DATETIME: {
      switch (rhs.type()) {
        case Value::Type::DURATION: {
          return lhs.getDateTime() - rhs.getDuration();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::DURATION: {
      switch (rhs.type()) {
        case Value::Type::DURATION: {
          return lhs.getDuration() - rhs.getDuration();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    default: {
      return Value::kNullBadType;
    }
  }
}

Value operator*(const Value& lhs, const Value& rhs) {
  if (lhs.isNull() || (lhs.empty() && !rhs.isNull())) {
    return lhs;
  }
  if (rhs.isNull() || rhs.empty()) {
    return rhs;
  }

  switch (lhs.type()) {
    case Value::Type::INT: {
      switch (rhs.type()) {
        case Value::Type::INT: {
          int64_t lVal = lhs.getInt();
          int64_t rVal = rhs.getInt();
          // -1 * min causes overflow
          if ((lVal == -1 && rVal == INT64_MIN) || (rVal == -1 && lVal == INT64_MIN)) {
            return Value::kNullOverflow;
          }
          int64_t res;
          if (__builtin_mul_overflow(lVal, rVal, &res)) {
            return Value::kNullOverflow;
          }
          return res;
        }
        case Value::Type::FLOAT: {
          return lhs.getInt() * rhs.getFloat();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::FLOAT: {
      switch (rhs.type()) {
        case Value::Type::INT: {
          return lhs.getFloat() * rhs.getInt();
        }
        case Value::Type::FLOAT: {
          return lhs.getFloat() * rhs.getFloat();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    default: {
      return Value::kNullBadType;
    }
  }
}

Value operator/(const Value& lhs, const Value& rhs) {
  if (lhs.isNull() || (lhs.empty() && !rhs.isNull())) {
    return lhs;
  }
  if (rhs.isNull() || rhs.empty()) {
    return rhs;
  }

  switch (lhs.type()) {
    case Value::Type::INT: {
      switch (rhs.type()) {
        case Value::Type::INT: {
          int64_t denom = rhs.getInt();
          if (denom == 0) {
            return Value::kNullDivByZero;
          }
          int64_t lVal = lhs.getInt();
          // INT_MIN/-1 causes overflow
          if (lVal == INT64_MIN && denom == -1) {
            return Value::kNullOverflow;
          }
          return lVal / denom;
        }
        case Value::Type::FLOAT: {
          double denom = rhs.getFloat();
          if (std::abs(denom) > kEpsilon) {
            return lhs.getInt() / denom;
          } else {
            return Value::kNullDivByZero;
          }
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::FLOAT: {
      switch (rhs.type()) {
        case Value::Type::INT: {
          int64_t denom = rhs.getInt();
          if (denom != 0) {
            return lhs.getFloat() / denom;
          } else {
            return Value::kNullDivByZero;
          }
        }
        case Value::Type::FLOAT: {
          double denom = rhs.getFloat();
          if (std::abs(denom) > kEpsilon) {
            return lhs.getFloat() / denom;
          } else {
            return Value::kNullDivByZero;
          }
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    default: {
      return Value::kNullBadType;
    }
  }
}

Value operator%(const Value& lhs, const Value& rhs) {
  if (lhs.isNull() || (lhs.empty() && !rhs.isNull())) {
    return lhs;
  }
  if (rhs.isNull() || rhs.empty()) {
    return rhs;
  }

  switch (lhs.type()) {
    case Value::Type::INT: {
      switch (rhs.type()) {
        case Value::Type::INT: {
          int64_t denom = rhs.getInt();
          if (denom != 0) {
            return lhs.getInt() % denom;
          } else {
            return Value::kNullDivByZero;
          }
        }
        case Value::Type::FLOAT: {
          double denom = rhs.getFloat();
          if (std::abs(denom) > kEpsilon) {
            return std::fmod(lhs.getInt(), denom);
          } else {
            return Value::kNullDivByZero;
          }
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    case Value::Type::FLOAT: {
      switch (rhs.type()) {
        case Value::Type::INT: {
          int64_t denom = rhs.getInt();
          if (denom != 0) {
            return std::fmod(lhs.getFloat(), denom);
          } else {
            return Value::kNullDivByZero;
          }
        }
        case Value::Type::FLOAT: {
          double denom = rhs.getFloat();
          if (std::abs(denom) > kEpsilon) {
            return std::fmod(lhs.getFloat(), denom);
          } else {
            return Value::kNullDivByZero;
          }
        }
        default: {
          return Value::kNullBadType;
        }
      }
    }
    default: {
      return Value::kNullBadType;
    }
  }
}

Value operator-(const Value& rhs) {
  if (rhs.isNull() || rhs.empty()) {
    return rhs;
  }

  switch (rhs.type()) {
    case Value::Type::INT: {
      int64_t rVal = rhs.getInt();
      if (rVal == INT64_MIN) {
        return Value::kNullOverflow;
      }
      auto val = -rVal;
      return val;
    }
    case Value::Type::FLOAT: {
      auto val = -rhs.getFloat();
      return val;
    }
    case Value::Type::DURATION: {
      auto val = -rhs.getDuration();
      return val;
    }
    default: {
      return Value::kNullBadType;
    }
  }
}

Value operator!(const Value& rhs) {
  if (rhs.isNull() || rhs.empty()) {
    return rhs;
  }

  if (!rhs.isImplicitBool()) {
    return Value::kNullBadType;
  }

  auto val = rhs.implicitBool();
  return !val;
}

bool operator<(const Value& lhs, const Value& rhs) {
  auto lType = lhs.type();
  auto rType = rhs.type();
  auto hasNullOrEmpty = (lType | rType) & Value::kEmptyNullType;
  auto notSameType = lType != rType;
  auto notBothNumeric = ((lType | rType) & Value::kNumericType) != Value::kNumericType;
  if (hasNullOrEmpty || (notSameType && notBothNumeric)) {
    return lType < rType;
  }

  switch (lType) {
    case Value::Type::BOOL: {
      return lhs.getBool() < rhs.getBool();
    }
    case Value::Type::INT: {
      switch (rType) {
        case Value::Type::INT: {
          return lhs.getInt() < rhs.getInt();
        }
        case Value::Type::FLOAT: {
          return lhs.getInt() < rhs.getFloat();
        }
        default: {
          return false;
        }
      }
    }
    case Value::Type::FLOAT: {
      switch (rType) {
        case Value::Type::INT: {
          return lhs.getFloat() < rhs.getInt();
        }
        case Value::Type::FLOAT: {
          return lhs.getFloat() < rhs.getFloat();
        }
        default: {
          return false;
        }
      }
    }
    case Value::Type::STRING: {
      return lhs.getStr() < rhs.getStr();
    }
    case Value::Type::VERTEX: {
      return lhs.getVertex() < rhs.getVertex();
    }
    case Value::Type::EDGE: {
      return lhs.getEdge() < rhs.getEdge();
    }
    case Value::Type::PATH: {
      return lhs.getPath() < rhs.getPath();
    }
    case Value::Type::TIME: {
      return lhs.getTime() < rhs.getTime();
    }
    case Value::Type::DATE: {
      return lhs.getDate() < rhs.getDate();
    }
    case Value::Type::DATETIME: {
      return lhs.getDateTime() < rhs.getDateTime();
    }
    case Value::Type::LIST: {
      return lhs.getList() < rhs.getList();
    }
    case Value::Type::MAP:
    case Value::Type::SET:
    case Value::Type::DATASET: {
      // TODO:
      return false;
    }
    case Value::Type::GEOGRAPHY: {
      return lhs.getGeography() < rhs.getGeography();
    }
    case Value::Type::DURATION: {
      DLOG(FATAL) << "Duration is not comparable.";
      return false;
    }
    case Value::Type::NULLVALUE:
    case Value::Type::__EMPTY__: {
      return false;
    }
  }
  DLOG(FATAL) << "Unknown type " << static_cast<int>(lType);
  return false;
}

bool Value::equals(const Value& rhs) const {
  auto lType = type();
  auto rType = rhs.type();
  auto hasNullOrEmpty = (lType | rType) & Value::kEmptyNullType;
  auto notSameType = lType != rType;
  auto notBothNumeric = ((lType | rType) & Value::kNumericType) != Value::kNumericType;
  if (hasNullOrEmpty || (notSameType && notBothNumeric)) {
    return type() == rhs.type();
  }

  switch (lType) {
    case Value::Type::BOOL: {
      return getBool() == rhs.getBool();
    }
    case Value::Type::INT: {
      switch (rType) {
        case Value::Type::INT: {
          return getInt() == rhs.getInt();
        }
        case Value::Type::FLOAT: {
          return std::abs(getInt() - rhs.getFloat()) < kEpsilon;
        }
        default: {
          return false;
        }
      }
    }
    case Value::Type::FLOAT: {
      switch (rType) {
        case Value::Type::INT: {
          return std::abs(getFloat() - rhs.getInt()) < kEpsilon;
        }
        case Value::Type::FLOAT: {
          return std::abs(getFloat() - rhs.getFloat()) < kEpsilon;
        }
        default: {
          return false;
        }
      }
    }
    case Value::Type::STRING: {
      return getStr() == rhs.getStr();
    }
    case Value::Type::DATE: {
      return getDate() == rhs.getDate();
    }
    case Value::Type::TIME: {
      // TODO(shylock) convert to UTC then compare
      return getTime() == rhs.getTime();
    }
    case Value::Type::DATETIME: {
      // TODO(shylock) convert to UTC then compare
      return getDateTime() == rhs.getDateTime();
    }
    case Value::Type::VERTEX: {
      return getVertex() == rhs.getVertex();
    }
    case Value::Type::EDGE: {
      return getEdge() == rhs.getEdge();
    }
    case Value::Type::PATH: {
      return getPath() == rhs.getPath();
    }
    case Value::Type::LIST: {
      return getList() == rhs.getList();
    }
    case Value::Type::MAP: {
      return getMap() == rhs.getMap();
    }
    case Value::Type::SET: {
      return getSet() == rhs.getSet();
    }
    case Value::Type::DATASET: {
      return getDataSet() == rhs.getDataSet();
    }
    case Value::Type::GEOGRAPHY: {
      return getGeography() == rhs.getGeography();
    }
    case Value::Type::DURATION: {
      return getDuration() == rhs.getDuration();
    }
    case Value::Type::NULLVALUE:
    case Value::Type::__EMPTY__: {
      return false;
    }
  }
  DLOG(FATAL) << "Unknown type " << static_cast<int>(type());
  return false;
}

std::size_t Value::hash() const {
  switch (type()) {
    case Type::__EMPTY__: {
      return 0;
    }
    case Type::NULLVALUE: {
      return ~0UL;
    }
    case Type::BOOL: {
      return std::hash<bool>()(getBool());
    }
    case Type::INT: {
      return std::hash<int64_t>()(getInt());
    }
    case Type::FLOAT: {
      return std::hash<double>()(getFloat());
    }
    case Type::STRING: {
      return std::hash<std::string>()(getStr());
    }
    case Type::DATE: {
      return std::hash<Date>()(getDate());
    }
    case Type::TIME: {
      return std::hash<Time>()(getTime());
    }
    case Type::DATETIME: {
      return std::hash<DateTime>()(getDateTime());
    }
    case Type::VERTEX: {
      return std::hash<Vertex>()(getVertex());
    }
    case Type::EDGE: {
      return std::hash<Edge>()(getEdge());
    }
    case Type::PATH: {
      return std::hash<Path>()(getPath());
    }
    case Type::LIST: {
      return std::hash<List>()(getList());
    }
    case Type::GEOGRAPHY: {
      return std::hash<Geography>()(getGeography());
    }
    case Type::MAP: {
      return std::hash<Map>()(getMap());
    }
    case Type::SET: {
      return std::hash<Set>()(getSet());
    }
    case Type::DURATION: {
      return std::hash<Duration>()(getDuration());
    }
    case Type::DATASET: {
      LOG(FATAL) << "Hash for DATASET has not been implemented";
    }
    default: {
      LOG(FATAL) << "Unknown type";
    }
  }
}

bool operator!=(const Value& lhs, const Value& rhs) {
  return !(lhs == rhs);
}

bool operator>(const Value& lhs, const Value& rhs) {
  return rhs < lhs;
}

bool operator<=(const Value& lhs, const Value& rhs) {
  return !(rhs < lhs);
}

bool operator>=(const Value& lhs, const Value& rhs) {
  return !(lhs < rhs);
}

Value operator&&(const Value& lhs, const Value& rhs) {
  if (lhs.isNull()) {
    return lhs.getNull();
  }

  if (rhs.isNull()) {
    return rhs.getNull();
  }

  if (lhs.empty() || rhs.empty()) {
    return Value::kEmpty;
  }

  if (lhs.isImplicitBool() && rhs.isImplicitBool()) {
    return lhs.implicitBool() && rhs.implicitBool();
  } else {
    return Value::kNullBadType;
  }
}

Value operator||(const Value& lhs, const Value& rhs) {
  if (lhs.isNull()) {
    return lhs.getNull();
  }

  if (rhs.isNull()) {
    return rhs.getNull();
  }

  if (lhs.empty() || rhs.empty()) {
    return Value::kEmpty;
  }

  if (lhs.isImplicitBool() && rhs.isImplicitBool()) {
    return lhs.implicitBool() || rhs.implicitBool();
  } else {
    return Value::kNullBadType;
  }
}

Value operator&(const Value& lhs, const Value& rhs) {
  if (lhs.isNull() || (lhs.empty() && !rhs.isNull())) {
    return lhs;
  }

  if (rhs.isNull() || rhs.empty()) {
    return rhs;
  }

  if (!lhs.isInt() || lhs.type() != rhs.type()) {
    return Value::kNullBadType;
  }

  switch (lhs.type()) {
    case Value::Type::INT: {
      return lhs.getInt() & rhs.getInt();
    }
    default: {
      return Value::kNullBadType;
    }
  }
}

Value operator|(const Value& lhs, const Value& rhs) {
  if (lhs.isNull() || (lhs.empty() && !rhs.isNull())) {
    return lhs;
  }

  if (rhs.isNull() || rhs.empty()) {
    return rhs;
  }

  if (!lhs.isInt() || lhs.type() != rhs.type()) {
    return Value::kNullBadType;
  }

  switch (lhs.type()) {
    case Value::Type::INT: {
      return lhs.getInt() | rhs.getInt();
    }
    default: {
      return Value::kNullBadType;
    }
  }
}

Value operator^(const Value& lhs, const Value& rhs) {
  if (lhs.isNull() || (lhs.empty() && !rhs.isNull())) {
    return lhs;
  }

  if (rhs.isNull() || rhs.empty()) {
    return rhs;
  }

  if (!lhs.isInt() || lhs.type() != rhs.type()) {
    return Value::kNullBadType;
  }

  switch (lhs.type()) {
    case Value::Type::INT: {
      return lhs.getInt() ^ rhs.getInt();
    }
    default: {
      return Value::kNullBadType;
    }
  }
}
}  // namespace nebula

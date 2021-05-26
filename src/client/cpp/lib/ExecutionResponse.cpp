/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "client/cpp/include/nebula/ExecutionResponse.h"
#include <assert.h>

namespace nebula {

PathEntry::PathEntry() : type_(kEmpty) {
}

PathEntry::PathEntry(const PathEntry &rhs) {
    switch (rhs.getType()) {
        case kVertex:
            setVertexValue(rhs.getVertexValue());
            break;
        case kEdge:
            setEdgeValue(rhs.getEdgeValue());
            break;
        default:
            break;
    }
}

PathEntry::~PathEntry() {
    clear();
}

void PathEntry::setVertexValue(Vertex vertex) {
    clear();
    type_ = kVertex;
    ::new (std::addressof(entry_.vertex)) Vertex(vertex);
}

void PathEntry::setEdgeValue(Edge edge) {
    clear();
    type_ = kEdge;
    ::new (std::addressof(entry_.edge)) Edge(edge);
}

Vertex const & PathEntry::getVertexValue() const {
    assert(type_ == kVertex);
    return entry_.vertex;
}

Edge const & PathEntry::getEdgeValue() const {
    assert(type_ == kEdge);
    return entry_.edge;
}


void PathEntry::clear() {
    if (type_ == kEmpty) {
        return;
    }
    switch (type_) {
        case kVertex:
            destruct(entry_.vertex);
            break;
        case kEdge:
            destruct(entry_.edge);
            break;
        default:
            break;
    }
    type_ = kEmpty;
}

Path::Path(const Path &rhs) {
    setEntryList(rhs.getEntryList());
}

std::vector<PathEntry> const & Path::getEntryList() const {
    return entryList_;
}

std::vector<PathEntry> Path::getEntryList() {
    return std::move(entryList_);
}

void Path::setEntryList(std::vector<PathEntry> entryList) {
    clear();
    entryList_ = std::move(entryList);
}

ColValue::ColValue() : type_(kEmptyType) {
}

ColValue::~ColValue() {
    clear();
}

ColValue::ColValue(const ColValue &rhs) {
    assign(rhs);
}

ColValue& ColValue::operator=(const ColValue &rhs) {
    if (&rhs != this) {
        assign(rhs);
    }
    return *this;
}

bool ColValue::operator==(const ColValue &rhs) const {
    if (type_ != rhs.getType()) {
        return false;
    }
    switch (type_) {
        case kBoolType:
            return value_.bool_val == rhs.getBoolValue();
        case kIntType:
            return value_.integer == rhs.getIntValue();
        case kIdType:
            return value_.id == rhs.getIdValue();
        case kDoubleType:
            return value_.integer == rhs.getDoubleValue();
        case kStringType:
            return value_.str == rhs.getStrValue();
        case kTimestampType:
            return value_.timestamp == rhs.getTimestampValue();
        default:
            break;
    }
    return false;
}

void ColValue::setBoolValue(bool val) {
    clear();
    type_ = kBoolType;
    ::new (std::addressof(value_.bool_val)) bool(val);  // NOLINT
}

void ColValue::setIntValue(int64_t val) {
    clear();
    type_ = kIntType;
    ::new (std::addressof(value_.integer)) int64_t(val);
}

void ColValue::setIdValue(int64_t val) {
    clear();
    type_ = kIdType;
    ::new (std::addressof(value_.id)) int64_t(val);
}

void ColValue::setFloatValue(float val) {
    clear();
    type_ = kFloatType;
    ::new (std::addressof(value_.single_precision)) float(val);  // NOLINT
}

void ColValue::setDoubleValue(double val) {
    clear();
    type_ = kDoubleType;
    ::new (std::addressof(value_.double_precision)) double(val);  // NOLINT
}

void ColValue::setStrValue(const std::string &val) {
    clear();
    type_ = kStringType;
    ::new (std::addressof(value_.str)) std::string(std::move(val));
}

void ColValue::setTimestampValue(int64_t val) {
    clear();
    type_ = kTimestampType;
    ::new (std::addressof(value_.timestamp)) int64_t(val);
}

void ColValue::setPathValue(Path val) {
    clear();
    type_ = kPathType;
    ::new (std::addressof(value_.path)) Path(val);
}

bool ColValue::getBoolValue() const {
    assert(type_ == kBoolType);
    return value_.bool_val;
}

int64_t ColValue::getIntValue() const {
    assert(type_ == kIntType);
    return value_.integer;
}

int64_t ColValue::getIdValue() const {
    assert(type_ == kIdType);
    return value_.id;
}

float ColValue::getFloatValue() const {
    assert(type_ == kFloatType);
    return value_.single_precision;
}

double ColValue::getDoubleValue() const {
    assert(type_ == kDoubleType);
    return value_.double_precision;
}

std::string const & ColValue::getStrValue() const {
    assert(type_ == kStringType);
    return value_.str;
}

int64_t ColValue::getTimestampValue() const {
    assert(type_ == kTimestampType);
    return value_.timestamp;
}

Year const & ColValue::getYearValue() const {
    assert(type_ == kYearType);
    return value_.year;
}

YearMonth const & ColValue::getYearMonthValue() const {
    assert(type_ == kMonthType);
    return value_.month;
}

Date const & ColValue::getDateValue() const {
    assert(type_ == kDateType);
    return value_.date;
}

DateTime const & ColValue::getDateTimeValue() const {
    assert(type_ == kDatetimeType);
    return value_.datetime;
}

Path const & ColValue::getPathValue() const {
    assert(type_ == kPathType);
    return value_.path;
}

void ColValue::assign(const ColValue &rhs) {
    switch (rhs.getType()) {
        case kBoolType:
            setBoolValue(rhs.getBoolValue());
            break;
        case kIntType:
            setIntValue(rhs.getIntValue());
            break;
        case kIdType:
            setIdValue(rhs.getIdValue());
            break;
        case kFloatType:
            setFloatValue(rhs.getFloatValue());
            break;
        case kDoubleType:
            setDoubleValue(rhs.getDoubleValue());
            break;
        case kStringType:
            setStrValue(rhs.getStrValue());
            break;
        case kTimestampType:
            setTimestampValue(rhs.getTimestampValue());
            break;
        case kPathType:
            setPathValue(rhs.getPathValue());
            break;
        default:
            break;
    }
}

void ColValue::clear() {
    if (type_ == kEmptyType) {
        return;
    }
    switch (type_) {
        case kBoolType:
            destruct(value_.bool_val);
            break;
        case kIntType:
            destruct(value_.integer);
            break;
        case kIdType:
            destruct(value_.id);
            break;
        case kFloatType:
            destruct(value_.single_precision);
            break;
        case kDoubleType:
            destruct(value_.double_precision);
            break;
        case kStringType:
            destruct(value_.str);
            break;
        case kTimestampType:
            destruct(value_.timestamp);
            break;
        case kPathType:
            destruct(value_.path);
            break;
        default:
            break;
    }
    type_ = kEmptyType;
}

void ExecutionResponse::setErrorCode(const ErrorCode code) {
    errorCode_ = code;
}

void ExecutionResponse::setLatencyInUs(const int32_t latency) {
    latencyInUs_ = latency;
}

void ExecutionResponse::setWholeLatencyInUss(const int64_t latency) {
    wholeLatencyInUs_ = latency;
}

void ExecutionResponse::setErrorMsg(std::string errorMsg) {
    errorMsg_ = std::move(errorMsg);
}

void ExecutionResponse::setSpaceName(std::string spaceName) {
    spaceName_ = std::move(spaceName);
}

void ExecutionResponse::setColumnNames(std::vector<std::string> columnNames) {
    columnNames_ = std::move(columnNames);
}

void ExecutionResponse::setRows(std::vector<RowValue> rows) {
    rows_ = std::move(rows);
}

ErrorCode ExecutionResponse::getErrorCode() {
    return errorCode_;
}

int32_t ExecutionResponse::getLatencyInUs() {
    return latencyInUs_;
}

int64_t ExecutionResponse::getWholeLatencyInUs() {
    return wholeLatencyInUs_;
}

std::string const & ExecutionResponse::getErrorMsg() const & {
    return errorMsg_;
}

std::string const & ExecutionResponse::getSpaceName() const & {
    return spaceName_;
}

std::vector<std::string> const & ExecutionResponse::getColumnNames() const & {
    return columnNames_;
}

std::vector<RowValue> const & ExecutionResponse::getRows() const & {
    return rows_;
}
}  // namespace nebula

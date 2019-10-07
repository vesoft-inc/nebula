/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "client/cpp/include/nebula/ExecuteResponse.h"
#include <assert.h>

namespace nebula {
namespace graph {

ColValue::ColValue() : type_(kEmtypType) {
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

bool const & ColValue::getBoolValue() const {
    assert(type_ == kBoolType);
    return value_.bool_val;
}

int64_t const & ColValue::getIntValue() const {
    assert(type_ == kIntType);
    return value_.integer;
}

int64_t const & ColValue::getIdValue() const {
    assert(type_ == kIdType);
    return value_.id;
}

float const & ColValue::getFloatValue() const {
    assert(type_ == kFloatType);
    return value_.single_precision;
}

double const & ColValue::getDoubleValue() const {
    assert(type_ == kDoubleType);
    return value_.double_precision;
}

std::string const & ColValue::getStrValue() const {
    assert(type_ == kStringType);
    return value_.str;
}

int64_t const & ColValue::getTimestampValue() const {
    assert(type_ == kTimestampType);
    return value_.timestamp;
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
        default:
            break;
    }
}

void ColValue::clear() {
    if (type_ == kEmtypType) {
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
        default:
            break;
    }
    type_ = kEmtypType;
}

void ExecuteResponse::setErrorCode(ErrorCode code) {
    errorCode_ = code;
}

void ExecuteResponse::setLatencyInUs(int32_t latency) {
    latencyInUs_ = latency;
}

void ExecuteResponse::setErrorMsg(std::string errorMsg) {
    hasErrorMsg_ = true;
    errorMsg_ = std::move(errorMsg);
}

void ExecuteResponse::setSpaceName(std::string spaceName) {
    hasSpaceName_ = true;
    spaceName_ = std::move(spaceName);
}

void ExecuteResponse::setColumnNames(std::vector<std::string> columnNames) {
    hasColumnNames_ = true;
    columnNames_ = std::move(columnNames);
}

void ExecuteResponse::setRows(std::vector<RowValue> rows) {
    hasRows_ = true;
    rows_ = std::move(rows);
}

int32_t ExecuteResponse::getErrorCode() {
    return errorCode_;
}

int32_t ExecuteResponse::getLatencyInUs() {
    return latencyInUs_;
}

std::string* ExecuteResponse::getErrorMsg() {
    return hasErrorMsg_ ? std::addressof(errorMsg_) : nullptr;
}

std::string* ExecuteResponse::getSpaceName() {
    return hasSpaceName_ ? std::addressof(spaceName_) : nullptr;
}

std::vector<std::string>* ExecuteResponse::getColumnNames() {
    return hasColumnNames_ ? std::addressof(columnNames_) : nullptr;
}

std::vector<RowValue>* ExecuteResponse::getRows() {
    return hasRows_ ? std::addressof(rows_) : nullptr;
}
}  // namespace graph
}  // namespace nebula

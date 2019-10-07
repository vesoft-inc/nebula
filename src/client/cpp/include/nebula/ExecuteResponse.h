/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENT_CPP_INCLUDE_NEBULA_EXECUTERESPONSE_H_
#define CLIENT_CPP_INCLUDE_NEBULA_EXECUTERESPONSE_H_

#include <string>
#include <iostream>
#include <vector>
#include <memory>

namespace nebula {
namespace graph {

enum ErrorCode {
    kSucceed             = 0,
    // RPC Failure
    kDisconnected        = -1,
    kFailToConnect       = -2,
    kRpcFailure          = -3,
    // Authentication error
    kBadUsernamePassowrd = -4,
    // Execution errors
    kSessionInvalid      = -5,
    kSessionTimeout      = -6,
    kSyntaxError         = -7,
    kExecutionError      = -8,
    // Nothing is executed When command is comment
    kStatementEmpty      = -9,
};

enum ValueType {
    kEmtypType          = 0,
    kBoolType           = 1,
    kIntType            = 2,
    kIdType             = 3,
    kFloatType          = 4,
    kDoubleType         = 5,
    kStringType         = 6,
    kTimestampType      = 7,
    kYearType           = 8,
    kMonth              = 9,
    kDate               = 10,
    kDatetime           = 11,
};

class ColValue {
public:
    ColValue();
    ColValue(const ColValue &rhs);

    virtual ~ColValue();

    ColValue& operator=(const ColValue &rhs);
    bool operator==(const ColValue &rhs) const;

    // set value
    void setBoolValue(bool val);
    void setIntValue(int64_t val);
    void setIdValue(int64_t val);
    void setFloatValue(float val);
    void setDoubleValue(double val);
    void setStrValue(const std::string &val);
    void setTimestampValue(int64_t val);

    // get value
    bool const & getBoolValue() const;
    int64_t const & getIntValue() const;
    int64_t const & getIdValue() const;
    float const & getFloatValue() const;
    double const & getDoubleValue() const;
    std::string const & getStrValue() const;
    int64_t const & getTimestampValue() const;

    bool isBool() const {
        return type_ == kBoolType;
    }
    bool isInt() const {
        return type_ == kIntType;
    }
    bool isId() const {
        return type_ == kIdType;
    }
    bool isDouble() const {
        return type_ == kDoubleType;
    }
    bool isString() const {
        return type_ == kTimestampType;
    }
    bool isTimestamp() const {
        return type_ == kTimestampType;
    }

    ValueType getType() const {
        return type_;
    }

private:
    template <class T>
    void destruct(T &val) {
        (&val)->~T();
    }

    void assign(const ColValue &value);
    void clear();

private:
    typedef int64_t IdType;
    typedef int64_t Timestamp;
    typedef int16_t Year;
    struct YearMonth {
        int16_t year;
        int8_t month;
    };

    struct Date {
        int16_t year;
        int8_t month;
        int8_t day;
    };

    struct DateTime {
        int16_t year;
        int8_t  month;
        int8_t  day;
        int8_t  hour;
        int8_t  minute;
        int8_t  second;
        int16_t millisec;
        int16_t microsec;
    };

    union Value {
        Value() {}
        ~Value() {}
        bool          bool_val;
        int64_t       integer;
        IdType        id;
        float         single_precision;
        double        double_precision;
        std::string   str;
        Timestamp     timestamp;
        Year          year;
        YearMonth     month;
        Date          date;
        DateTime      datetime;
    };

    ValueType       type_{kEmtypType};
    Value           value_;
};

class RowValue {
public:
    void setColumns(std::vector<ColValue> columns) {
        columns_ = std::move(columns);
    }

    std::vector<ColValue> const & getColumns() const {
        return columns_;
    }

    std::vector<ColValue> getColumns() {
        return std::move(columns_);
    }

private:
    std::vector<ColValue>      columns_;
};

class ExecuteResponse {
public:
    ExecuteResponse() {}
    virtual ~ExecuteResponse() {}

    // set value
    void setErrorCode(ErrorCode code);
    void setLatencyInUs(int32_t latency);
    void setErrorMsg(std::string errorMsg);
    void setSpaceName(std::string spaceName);
    void setColumnNames(std::vector<std::string> columnNames);
    void setRows(std::vector<RowValue> rows);

    // get value
    int32_t getErrorCode();
    int32_t getLatencyInUs();
    std::string* getErrorMsg();
    std::string* getSpaceName();
    std::vector<std::string>* getColumnNames();
    std::vector<RowValue>* getRows();

private:
    ErrorCode                       errorCode_;
    int32_t                         latencyInUs_{0};
    std::string                     spaceName_;
    std::string                     errorMsg_;
    std::vector<std::string>        columnNames_;
    std::vector<RowValue>           rows_;

    bool                            hasErrorMsg_{false};
    bool                            hasSpaceName_{false};
    bool                            hasColumnNames_{false};
    bool                            hasRows_{false};
};

}  // namespace graph
}  // namespace nebula
#endif  // CLIENT_CPP_INCLUDE_NEBULA_EXECUTERESPONSE_H_

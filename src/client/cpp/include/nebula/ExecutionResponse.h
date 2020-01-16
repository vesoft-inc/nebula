/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENT_CPP_INCLUDE_NEBULA_EXECUTERESPONSE_H_
#define CLIENT_CPP_INCLUDE_NEBULA_EXECUTERESPONSE_H_

#include <string>
#include <vector>
#include <memory>
#include <iostream>
#include <functional>

namespace nebula {
namespace graph {
class NebulaClientImpl;
}

typedef int64_t IdType;
typedef int64_t RankType;
typedef int64_t Timestamp;
typedef int16_t Year;
struct YearMonth {
    int16_t   year;
    int8_t    month;
};

struct Date {
    int16_t   year;
    int8_t    month;
    int8_t    day;
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

struct Vertex {
    Vertex() {}
    ~Vertex() {
        id = 0;
    }
    IdType   id{0};
};

struct Edge {
    Edge() {}
    ~Edge() {
        type = "";
        ranking = 0;
        src = 0;
        dst = 0;
    }
    std::string   type{""};
    RankType      ranking{0};
    IdType        src{0};
    IdType        dst{0};
};

enum EntryType {
    kEmpty  = 0,
    kVertex = 1,
    kEdge = 2,
};

union Entry {
    Entry() {}
    ~Entry() {}
    Vertex     vertex;
    Edge       edge;
};

class PathEntry final {
public:
    PathEntry();
    PathEntry(const PathEntry &rhs);
    ~PathEntry();

public:
    void setVertexValue(Vertex vertex);
    void setEdgeValue(Edge edge);

    Vertex const & getVertexValue() const;
    Edge const & getEdgeValue() const;

    EntryType getType() const {
        return type_;
    }

private:
    template <class T>
    void destruct(T &val) {
        (&val)->~T();
    }

    void clear();

private:
    EntryType   type_{kEmpty};
    Entry       entry_;
};

class Path final {
    friend class nebula::graph::NebulaClientImpl;
public:
    Path() = default;
    Path(const Path &rhs);
    ~Path() = default;

    std::vector<PathEntry> const & getEntryList() const;
    std::vector<PathEntry> getEntryList();

protected:
    void setEntryList(std::vector<PathEntry> entryList);

private:
    void clear() {
        entryList_.clear();
    }
private:
    std::vector<PathEntry> entryList_;
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
    Path          path;
};

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
    kErrorSyntax         = -7,
    kErrorExecution      = -8,
    // Nothing is executed When command is comment
    kEmptyStatement      = -9,
};

enum ValueType {
    kEmptyType          = 0,
    kBoolType           = 1,
    kIntType            = 2,
    kIdType             = 3,
    kFloatType          = 4,
    kDoubleType         = 5,
    kStringType         = 6,
    kTimestampType      = 7,
    kYearType           = 8,
    kMonthType          = 9,
    kDateType           = 10,
    kDatetimeType       = 11,
    kPathType           = 12,
};

class ColValue final {
    friend class nebula::graph::NebulaClientImpl;
public:
    ColValue();
    ColValue(const ColValue &rhs);

    ~ColValue();

    ColValue& operator=(const ColValue &rhs);
    bool operator==(const ColValue &rhs) const;

    // TODO: need provide move semantics

    // get value
    bool getBoolValue() const;
    int64_t getIntValue() const;
    IdType getIdValue() const;
    float getFloatValue() const;
    double getDoubleValue() const;
    std::string const & getStrValue() const;
    Timestamp getTimestampValue() const;
    Year const & getYearValue() const;
    YearMonth const & getYearMonthValue() const;
    Date const & getDateValue() const;
    DateTime const & getDateTimeValue() const;
    Path const & getPathValue() const;

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
        return type_ == kStringType;
    }

    bool isTimestamp() const {
        return type_ == kTimestampType;
    }

    bool isYear() const {
        return type_ == kYearType;
    }

    bool isYearMonth() const {
        return type_ == kMonthType;
    }

    bool isDate() const {
        return type_ == kDateType;
    }

    bool isDateTime() const {
        return type_ == kDatetimeType;
    }

    bool isPath() const {
        return type_ == kPathType;
    }

    ValueType getType() const {
        return type_;
    }

protected:
    // set value
    void setBoolValue(bool val);
    void setIntValue(int64_t val);
    void setIdValue(int64_t val);
    void setFloatValue(float val);
    void setDoubleValue(double val);
    void setStrValue(const std::string &val);
    void setTimestampValue(int64_t val);
    void setPathValue(Path val);

private:
    template <class T>
    void destruct(T &val) {
        (&val)->~T();
    }

    void assign(const ColValue &value);
    void clear();

private:
    ValueType       type_{kEmptyType};
    Value           value_;
};

class RowValue final {
public:
    void setColumns(std::vector<ColValue> columns) {
        columns_ = std::move(columns);
    }

    std::vector<ColValue> const & getColumns() const & {
        return columns_;
    }

    std::vector<ColValue> getColumns() && {
        return std::move(columns_);
    }

private:
    std::vector<ColValue>      columns_;
};

class ExecutionResponse final {
    friend class nebula::graph::NebulaClientImpl;
public:
    ExecutionResponse() {}
    ~ExecutionResponse() {}

public:
    // get error code
    ErrorCode getErrorCode();
    // the latency of graphd handle
    int32_t getLatencyInUs();
    // the latency from send msg to receive msg, without convert data time
    int64_t getWholeLatencyInUs();

    std::string const & getErrorMsg() const &;
    std::string const & getSpaceName() const &;
    std::vector<std::string> const & getColumnNames() const &;
    std::vector<RowValue> const & getRows() const &;

protected:
    // set value
    void setErrorCode(const ErrorCode code);
    void setLatencyInUs(const int32_t latency);
    void setWholeLatencyInUss(const int64_t latency);
    void setErrorMsg(std::string errorMsg);
    void setSpaceName(std::string spaceName);
    void setColumnNames(std::vector<std::string> columnNames);
    void setRows(std::vector<RowValue> rows);

private:
    ErrorCode                       errorCode_;
    int32_t                         latencyInUs_{0};
    int64_t                         wholeLatencyInUs_{0};
    std::string                     spaceName_;
    std::string                     errorMsg_;
    std::vector<std::string>        columnNames_;
    std::vector<RowValue>           rows_;
};

struct ConnectionInfo {
    ConnectionInfo() : connectionNum(10), timeout(1000) {}
    std::string addr;           // addr of graphd
    uint32_t    port;           // port of graphd
    uint32_t    connectionNum;  // connection number
    int32_t     timeout;        // timeout ms
};

using CallbackFun = std::function<void(ExecutionResponse*, ErrorCode)>;

}  // namespace nebula
#endif  // CLIENT_CPP_INCLUDE_NEBULA_EXECUTERESPONSE_H_

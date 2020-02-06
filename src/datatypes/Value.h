/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATATYPES_VALUE_H_
#define DATATYPES_VALUE_H_

#include "base/Base.h"
#include "thrift/ThriftTypes.h"
#include "datatypes/Date.h"
#include "datatypes/Path.h"

namespace nebula {

struct Map;
struct List;

enum class NullType {
    __NULL__ = 0,
    NaN      = 1,
    BAD_DATA = 2,
    BAD_TYPE = 3
};


struct Value {
    enum class Type {
        __EMPTY__ = 0,
        NULLVALUE = 1,
        BOOL = 2,
        INT = 3,
        FLOAT = 4,
        STRING = 5,
        DATE = 6,
        DATETIME = 7,
        PATH = 8,
        LIST = 9,
        MAP = 10,
    };

    // Constructors
    Value() : type_(Type::__EMPTY__) {}
    Value(Value&& rhs);
    Value(const Value& rhs);

    Value(NullType v);              // NOLINT
    Value(bool v);                  // NOLINT
    Value(int64_t v);               // NOLINT
    Value(double v);                // NOLINT
    Value(const std::string& v);    // NOLINT
    Value(std::string&& v);         // NOLINT
    Value(const Date& v);           // NOLINT
    Value(Date&& v);                // NOLINT
    Value(const DateTime& v);       // NOLINT
    Value(DateTime&& v);            // NOLINT
    Value(const Path& v);           // NOLINT
    Value(Path&& v);                // NOLINT
    Value(const List& v);           // NOLINT
    Value(List&& v);                // NOLINT
    Value(const Map& v);            // NOLINT
    Value(Map&& v);                 // NOLINT

    Value& operator=(Value&& rhs);
    Value& operator=(const Value& rhs);

    Value& operator=(NullType v);
    Value& operator=(bool v);
    Value& operator=(int64_t v);
    Value& operator=(double v);
    Value& operator=(const std::string& v);
    Value& operator=(std::string&& v);
    Value& operator=(const Date& v);
    Value& operator=(Date&& v);
    Value& operator=(const DateTime& v);
    Value& operator=(DateTime&& v);
    Value& operator=(const Path& v);
    Value& operator=(Path&& v);
    Value& operator=(const List& v);
    Value& operator=(List&& v);
    Value& operator=(const Map& v);
    Value& operator=(Map&& v);

    void set(NullType v);
    void set(bool v);
    void set(int64_t v);
    void set(double v);
    void set(const std::string& v);
    void set(std::string&& v);
    void set(const Date& v);
    void set(Date&& v);
    void set(const DateTime& v);
    void set(DateTime&& v);
    void set(const Path& v);
    void set(Path&& v);
    void set(const List& v);
    void set(List&& v);
    void set(const Map& v);
    void set(Map&& v);

    NullType getNullType() const;
    bool getBool() const;
    int64_t getInt() const;
    double getDouble() const;
    const std::string& getString() const;
    const Date& getDate() const;
    const DateTime& getDateTime() const;
    const Path& getPath() const;
    const List& getList() const;
    const Map& getMap() const;

    std::string moveString();
    Date moveDate();
    DateTime moveDateTime();
    Path movePath();
    List moveList();
    Map moveMap();

    Type type() const noexcept {
        return type_;
    }

    bool operator==(const Value& rhs) const;

    static const Value& null() noexcept {
        static const Value kNullValue(NullType::__NULL__);
        return kNullValue;
    }

private:
    Type type_;

    union Storage {
        NullType                nVal;
        bool                    bVal;
        int64_t                 iVal;
        double                  fVal;
        std::string             sVal;
        Date                    dVal;
        DateTime                tVal;
        Path                    pVal;
        std::unique_ptr<List>   lVal;
        std::unique_ptr<Map>    mVal;

        Storage() {}
        ~Storage() {}
    } value_;

    template <class T>
    void destruct(T& val) {
        (&val)->~T();
    }

    void clear();
};

void swap(Value& a, Value& b);

std::ostream& operator<<(std::ostream& os, const Value::Type& type);


struct Map {
    std::unordered_map<std::string, Value> kvs;

    Map() = default;
    Map(const Map&) = default;
    Map(Map&&) = default;

    Map& operator=(const Map& rhs) {
        kvs = rhs.kvs;
        return *this;
    }
    Map& operator=(Map&& rhs) {
        kvs = std::move(rhs.kvs);
        return *this;
    }

    bool operator==(const Map& rhs) const {
        return kvs == rhs.kvs;
    }
};


struct List {
    std::vector<Value> values;

    List() = default;
    List(const List&) = default;
    List(List&&) = default;

    List& operator=(const List& rhs) {
        values = rhs.values;
        return *this;
    }
    List& operator=(List&& rhs) {
        values = std::move(rhs.values);
        return *this;
    }

    bool operator==(const List& rhs) const {
        return values == rhs.values;
    }
};

}  // namespace nebula
#endif  // DATATYPES_VALUE_H_


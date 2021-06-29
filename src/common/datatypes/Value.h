/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_VALUE_H_
#define COMMON_DATATYPES_VALUE_H_

#include <memory>

#include "common/thrift/ThriftTypes.h"
#include "common/datatypes/Date.h"

namespace apache {
namespace thrift {

template<class T, class U>
class Cpp2Ops;

}  // namespace thrift
}  // namespace apache


namespace nebula {

struct Vertex;
struct Edge;
struct Path;
struct Map;
struct List;
struct Set;
struct DataSet;

enum class NullType {
    __NULL__ = 0,
    NaN      = 1,
    BAD_DATA = 2,
    BAD_TYPE = 3,
    ERR_OVERFLOW = 4,
    UNKNOWN_PROP = 5,
    DIV_BY_ZERO = 6,
    OUT_OF_RANGE = 7,
};


struct Value {
    static const Value kEmpty;
    static const Value kNullValue;
    static const Value kNullNaN;
    static const Value kNullBadData;
    static const Value kNullBadType;
    static const Value kNullOverflow;
    static const Value kNullUnknownProp;
    static const Value kNullDivByZero;
    static const Value kNullOutOfRange;

    static const uint64_t kEmptyNullType;
    static const uint64_t kNumericType;

    friend class apache::thrift::Cpp2Ops<Value, void>;

    enum class Type : uint64_t {
        __EMPTY__ = 1UL,
        BOOL      = 1UL << 1,
        INT       = 1UL << 2,
        FLOAT     = 1UL << 3,
        STRING    = 1UL << 4,
        DATE      = 1UL << 5,
        TIME      = 1UL << 6,
        DATETIME  = 1UL << 7,
        VERTEX    = 1UL << 8,
        EDGE      = 1UL << 9,
        PATH      = 1UL << 10,
        LIST      = 1UL << 11,
        MAP       = 1UL << 12,
        SET       = 1UL << 13,
        DATASET   = 1UL << 14,
        NULLVALUE = 1UL << 63,
    };

    // Constructors
    Value() : type_(Type::__EMPTY__) {}
    Value(Value&& rhs) noexcept;
    Value(const Value& rhs);

    // For the cpp bool-pointer conversion, if Value ctor accept a pointer without matched ctor
    // it will convert to bool type and the match the bool value ctor,
    // So we disable all pointer ctor except the char*
    template <typename T>
    Value(T *) = delete;                  // NOLINT
    Value(const std::nullptr_t) = delete; // NOLINT
    Value(const NullType& v);             // NOLINT
    Value(NullType&& v);                  // NOLINT
    Value(const bool& v);                 // NOLINT
    Value(bool&& v);                      // NOLINT
    Value(const int8_t& v);               // NOLINT
    Value(int8_t&& v);                    // NOLINT
    Value(const int16_t& v);              // NOLINT
    Value(int16_t&& v);                   // NOLINT
    Value(const int32_t& v);              // NOLINT
    Value(int32_t&& v);                   // NOLINT
    Value(const int64_t& v);              // NOLINT
    Value(int64_t&& v);                   // NOLINT
    Value(const double& v);               // NOLINT
    Value(double&& v);                    // NOLINT
    Value(const std::string& v);          // NOLINT
    Value(std::string&& v);               // NOLINT
    Value(const char* v);                 // NOLINT
    Value(const Date& v);                 // NOLINT
    Value(Date&& v);                      // NOLINT
    Value(const Time& v);                 // NOLINT
    Value(Time&& v);                      // NOLINT
    Value(const DateTime& v);             // NOLINT
    Value(DateTime&& v);                  // NOLINT
    Value(const Vertex& v);               // NOLINT
    Value(Vertex&& v);                    // NOLINT
    Value(const Edge& v);                 // NOLINT
    Value(Edge&& v);                      // NOLINT
    Value(const Path& v);                 // NOLINT
    Value(Path&& v);                      // NOLINT
    Value(const List& v);                 // NOLINT
    Value(List&& v);                      // NOLINT
    Value(const Map& v);                  // NOLINT
    Value(Map&& v);                       // NOLINT
    Value(const Set& v);                  // NOLINT
    Value(Set&& v);                       // NOLINT
    Value(const DataSet& v);              // NOLINT
    Value(DataSet&& v);                   // NOLINT
    ~Value() { clear(); }

    Type type() const noexcept {
        return type_;
    }

    const std::string& typeName() const;

    bool empty() const {
        return type_ == Type::__EMPTY__;
    }
    bool isNull() const {
        return type_ == Type::NULLVALUE;
    }
    bool isBadNull() const {
        if (!isNull()) {
            return false;
        }
        auto& null = value_.nVal;
        return null == NullType::NaN
            || null == NullType::BAD_DATA
            || null == NullType::BAD_TYPE
            || null == NullType::ERR_OVERFLOW
            || null == NullType::UNKNOWN_PROP
            || null == NullType::DIV_BY_ZERO
            || null == NullType::OUT_OF_RANGE;
    }
    bool isNumeric() const {
        return type_ == Type::INT || type_ == Type::FLOAT;
    }
    bool isBool() const {
        return type_ == Type::BOOL;
    }
    bool isInt() const {
        return type_ == Type::INT;
    }
    bool isFloat() const {
        return type_ == Type::FLOAT;
    }
    bool isStr() const {
        return type_ == Type::STRING;
    }
    bool isDate() const {
        return type_ == Type::DATE;
    }
    bool isTime() const {
        return type_ == Type::TIME;
    }
    bool isDateTime() const {
        return type_ == Type::DATETIME;
    }
    bool isVertex() const {
        return type_ == Type::VERTEX;
    }
    bool isEdge() const {
        return type_ == Type::EDGE;
    }
    bool isPath() const {
        return type_ == Type::PATH;
    }
    bool isList() const {
        return type_ == Type::LIST;
    }
    bool isMap() const {
        return type_ == Type::MAP;
    }
    bool isSet() const {
        return type_ == Type::SET;
    }
    bool isDataSet() const {
        return type_ == Type::DATASET;
    }

    void clear();

    void __clear() {
        clear();
    }

    Value& operator=(Value&& rhs) noexcept;
    Value& operator=(const Value& rhs);

    void setNull(const NullType& v);
    void setNull(NullType&& v);
    void setBool(const bool& v);
    void setBool(bool&& v);
    void setInt(const int8_t& v);
    void setInt(int8_t&& v);
    void setInt(const int16_t& v);
    void setInt(int16_t&& v);
    void setInt(const int32_t& v);
    void setInt(int32_t&& v);
    void setInt(const int64_t& v);
    void setInt(int64_t&& v);
    void setFloat(const double& v);
    void setFloat(double&& v);
    void setStr(const std::string& v);
    void setStr(std::string&& v);
    void setStr(const char* v);
    void setDate(const Date& v);
    void setDate(Date&& v);
    void setTime(const Time& v);
    void setTime(Time&& v);
    void setDateTime(const DateTime& v);
    void setDateTime(DateTime&& v);
    void setVertex(const Vertex& v);
    void setVertex(Vertex&& v);
    void setVertex(std::unique_ptr<Vertex>&& v);
    void setEdge(const Edge& v);
    void setEdge(Edge&& v);
    void setEdge(std::unique_ptr<Edge>&& v);
    void setPath(const Path& v);
    void setPath(Path&& v);
    void setPath(std::unique_ptr<Path>&& v);
    void setList(const List& v);
    void setList(List&& v);
    void setList(std::unique_ptr<List>&& v);
    void setMap(const Map& v);
    void setMap(Map&& v);
    void setMap(std::unique_ptr<Map>&& v);
    void setSet(const Set& v);
    void setSet(Set&& v);
    void setSet(std::unique_ptr<Set>&& v);
    void setDataSet(const DataSet& v);
    void setDataSet(DataSet&& v);
    void setDataSet(std::unique_ptr<DataSet>&& v);

    const NullType& getNull() const;
    const bool& getBool() const;
    const int64_t& getInt() const;
    const double& getFloat() const;
    const std::string& getStr() const;
    const Date& getDate() const;
    const Time& getTime() const;
    const DateTime& getDateTime() const;
    const Vertex& getVertex() const;
    const Vertex* getVertexPtr() const;
    const Edge& getEdge() const;
    const Edge* getEdgePtr() const;
    const Path& getPath() const;
    const Path* getPathPtr() const;
    const List& getList() const;
    const List* getListPtr() const;
    const Map& getMap() const;
    const Map* getMapPtr() const;
    const Set& getSet() const;
    const Set* getSetPtr() const;
    const DataSet& getDataSet() const;
    const DataSet* getDataSetPtr() const;

    NullType moveNull();
    bool moveBool();
    int64_t moveInt();
    double moveFloat();
    std::string moveStr();
    Date moveDate();
    Time moveTime();
    DateTime moveDateTime();
    Vertex moveVertex();
    Edge moveEdge();
    Path movePath();
    List moveList();
    Map moveMap();
    Set moveSet();
    DataSet moveDataSet();

    NullType& mutableNull();
    bool& mutableBool();
    int64_t& mutableInt();
    double& mutableFloat();
    std::string& mutableStr();
    Date& mutableDate();
    Time& mutableTime();
    DateTime& mutableDateTime();
    Vertex& mutableVertex();
    Edge& mutableEdge();
    Path& mutablePath();
    List& mutableList();
    Map& mutableMap();
    Set& mutableSet();
    DataSet& mutableDataSet();

    static const Value& null() noexcept {
        return kNullValue;
    }

    std::string toString() const;

    Value toBool() const;
    Value toFloat() const;
    Value toInt() const;

    Value lessThan(const Value& v) const;

    Value equal(const Value& v) const;

private:
    Type type_;

    union Storage {
        NullType                    nVal;
        bool                        bVal;
        int64_t                     iVal;
        double                      fVal;
        std::unique_ptr<std::string>sVal;
        Date                        dVal;
        Time                        tVal;
        DateTime                    dtVal;
        std::unique_ptr<Vertex>     vVal;
        std::unique_ptr<Edge>       eVal;
        std::unique_ptr<Path>       pVal;
        std::unique_ptr<List>       lVal;
        std::unique_ptr<Map>        mVal;
        std::unique_ptr<Set>        uVal;
        std::unique_ptr<DataSet>    gVal;

        Storage() {}
        ~Storage() {}
    } value_;

    template <class T>
    void destruct(T& val) {
        (&val)->~T();
    }

    // Null value
    void setN(const NullType& v);
    void setN(NullType&& v);
    // Bool value
    void setB(const bool& v);
    void setB(bool&& v);
    // Integer value
    void setI(const int64_t& v);
    void setI(int64_t&& v);
    // Double float value
    void setF(const double& v);
    void setF(double&& v);
    // String value
    void setS(const std::string& v);
    void setS(std::string&& v);
    void setS(const char* v);
    void setS(std::unique_ptr<std::string> v);
    // Date value
    void setD(const Date& v);
    void setD(Date&& v);
    // Date value
    void setT(const Time& v);
    void setT(Time&& v);
    // DateTime value
    void setDT(const DateTime& v);
    void setDT(DateTime&& v);
    // Vertex value
    void setV(const std::unique_ptr<Vertex>& v);
    void setV(std::unique_ptr<Vertex>&& v);
    void setV(const Vertex& v);
    void setV(Vertex&& v);
    // Edge value
    void setE(const std::unique_ptr<Edge>& v);
    void setE(std::unique_ptr<Edge>&& v);
    void setE(const Edge& v);
    void setE(Edge&& v);
    // Path value
    void setP(const std::unique_ptr<Path>& v);
    void setP(std::unique_ptr<Path>&& v);
    void setP(const Path& v);
    void setP(Path&& v);
    // List value
    void setL(const std::unique_ptr<List>& v);
    void setL(std::unique_ptr<List>&& v);
    void setL(const List& v);
    void setL(List&& v);
    // Map value
    void setM(const std::unique_ptr<Map>& v);
    void setM(std::unique_ptr<Map>&& v);
    void setM(const Map& v);
    void setM(Map&& v);
    // Set value
    void setU(const std::unique_ptr<Set>& v);
    void setU(std::unique_ptr<Set>&& v);
    void setU(const Set& v);
    void setU(Set&& v);
    // DataSet value
    void setG(const std::unique_ptr<DataSet>& v);
    void setG(std::unique_ptr<DataSet>&& v);
    void setG(const DataSet& v);
    void setG(DataSet&& v);
};

static_assert(sizeof(Value) == 16UL, "The size of Value should be 16UL");

void swap(Value& a, Value& b);

constexpr auto kEpsilon = 1e-8;

// Arithmetic operations
Value operator+(const Value& lhs, const Value& rhs);
Value operator-(const Value& lhs, const Value& rhs);
Value operator*(const Value& lhs, const Value& rhs);
Value operator/(const Value& lhs, const Value& rhs);
Value operator%(const Value& lhs, const Value& rhs);
// Unary operations
Value operator-(const Value& rhs);
Value operator!(const Value& rhs);
// Comparison operations
// 1. we compare the type directly in these cases:
//  if type do not match except both numeric
//  if lhs and rhs have at least one null or empty
// 2. null is the biggest, empty is the smallest
bool operator< (const Value& lhs, const Value& rhs);
bool operator==(const Value& lhs, const Value& rhs);
bool operator!=(const Value& lhs, const Value& rhs);
bool operator> (const Value& lhs, const Value& rhs);
bool operator<=(const Value& lhs, const Value& rhs);
bool operator>=(const Value& lhs, const Value& rhs);
// Logical operations
Value operator&&(const Value& lhs, const Value& rhs);
Value operator||(const Value& lhs, const Value& rhs);
// Bit operations
Value operator&(const Value& lhs, const Value& rhs);
Value operator|(const Value& lhs, const Value& rhs);
Value operator^(const Value& lhs, const Value& rhs);
// Visualize
std::ostream& operator<<(std::ostream& os, const Value::Type& type);
inline std::ostream& operator<<(std::ostream& os, const Value& value) {
    return os << value.toString();
}

inline uint64_t operator|(const Value::Type& lhs, const Value::Type& rhs) {
    return static_cast<uint64_t>(lhs) | static_cast<uint64_t>(rhs);
}
inline uint64_t operator|(const uint64_t lhs, const Value::Type& rhs) {
    return lhs | static_cast<uint64_t>(rhs);
}
inline uint64_t operator|(const Value::Type& lhs, const uint64_t rhs) {
    return static_cast<uint64_t>(lhs) | rhs;
}
inline uint64_t operator&(const Value::Type& lhs, const Value::Type& rhs) {
    return static_cast<uint64_t>(lhs) & static_cast<uint64_t>(rhs);
}
inline uint64_t operator&(const uint64_t lhs, const Value::Type& rhs) {
    return lhs & static_cast<uint64_t>(rhs);
}
inline uint64_t operator&(const Value::Type& lhs, const uint64_t rhs) {
    return static_cast<uint64_t>(lhs) & rhs;
}

}  // namespace nebula


namespace std {

// Inject a customized hash function
template<>
struct hash<nebula::Value> {
    std::size_t operator()(const nebula::Value& h) const noexcept;
};

template<>
struct hash<nebula::Value*> {
    std::size_t operator()(const nebula::Value* h) const noexcept {
        return h == nullptr ? 0 : hash<nebula::Value>()(*h);
    }
};

template<>
struct equal_to<nebula::Value*> {
    bool operator()(const nebula::Value* lhs, const nebula::Value* rhs) const noexcept {
        return lhs == rhs ? true : (lhs != nullptr) && (rhs != nullptr) && (*lhs == *rhs);
    }
};

}  // namespace std
#endif  // COMMON_DATATYPES_VALUE_H_

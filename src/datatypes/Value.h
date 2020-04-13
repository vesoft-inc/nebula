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

enum class NullType {
    __NULL__ = 0,
    NaN      = 1,
    BAD_DATA = 2,
    BAD_TYPE = 3,
    ERR_OVERFLOW = 4,
    UNKNOWN_PROP = 5,
};


struct Value {
    friend class apache::thrift::Cpp2Ops<Value, void>;

    enum class Type {
        __EMPTY__ = 0,
        NULLVALUE = 1,
        BOOL = 2,
        INT = 3,
        FLOAT = 4,
        STRING = 5,
        DATE = 6,
        DATETIME = 7,
        VERTEX = 8,
        EDGE = 9,
        PATH = 10,
        LIST = 11,
        MAP = 12,
        SET = 13,
    };

    // Constructors
    Value() : type_(Type::__EMPTY__) {}
    Value(Value&& rhs);
    Value(const Value& rhs);

    Value(const NullType& v);       // NOLINT
    Value(NullType&& v);            // NOLINT
    Value(const bool& v);           // NOLINT
    Value(bool&& v);                // NOLINT
    Value(const int8_t& v);         // NOLINT
    Value(int8_t&& v);              // NOLINT
    Value(const int16_t& v);        // NOLINT
    Value(int16_t&& v);             // NOLINT
    Value(const int32_t& v);        // NOLINT
    Value(int32_t&& v);             // NOLINT
    Value(const int64_t& v);        // NOLINT
    Value(int64_t&& v);             // NOLINT
    Value(const double& v);         // NOLINT
    Value(double&& v);              // NOLINT
    Value(const std::string& v);    // NOLINT
    Value(std::string&& v);         // NOLINT
    Value(const char* v);           // NOLINT
    Value(folly::StringPiece v);    // NOLINT
    Value(const Date& v);           // NOLINT
    Value(Date&& v);                // NOLINT
    Value(const DateTime& v);       // NOLINT
    Value(DateTime&& v);            // NOLINT
    Value(const Vertex& v);         // NOLINT
    Value(Vertex&& v);              // NOLINT
    Value(const Edge& v);           // NOLINT
    Value(Edge&& v);                // NOLINT
    Value(const Path& v);           // NOLINT
    Value(Path&& v);                // NOLINT
    Value(const List& v);           // NOLINT
    Value(List&& v);                // NOLINT
    Value(const Map& v);            // NOLINT
    Value(Map&& v);                 // NOLINT
    Value(const Set& v);            // NOLINT
    Value(Set&& v);                 // NOLINT

    Type type() const noexcept {
        return type_;
    }

    bool empty() const {
        return type_ == Type::__EMPTY__;
    }

    void clear();

    Value& operator=(Value&& rhs);
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
    void setStr(folly::StringPiece v);
    void setDate(const Date& v);
    void setDate(Date&& v);
    void setDateTime(const DateTime& v);
    void setDateTime(DateTime&& v);
    void setVertex(const Vertex& v);
    void setVertex(Vertex&& v);
    void setEdge(const Edge& v);
    void setEdge(Edge&& v);
    void setPath(const Path& v);
    void setPath(Path&& v);
    void setList(const List& v);
    void setList(List&& v);
    void setMap(const Map& v);
    void setMap(Map&& v);
    void setSet(const Set& v);
    void setSet(Set&& v);

    const NullType& getNull() const;
    const bool& getBool() const;
    const int64_t& getInt() const;
    const double& getFloat() const;
    const std::string& getStr() const;
    const Date& getDate() const;
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

    NullType moveNull();
    bool moveBool();
    int64_t moveInt();
    double moveFloat();
    std::string moveStr();
    Date moveDate();
    DateTime moveDateTime();
    Vertex moveVertex();
    Edge moveEdge();
    Path movePath();
    List moveList();
    Map moveMap();
    Set moveSet();

    NullType& mutableNull();
    bool& mutableBool();
    int64_t& mutableInt();
    double& mutableFloat();
    std::string& mutableStr();
    Date& mutableDate();
    DateTime& mutableDateTime();
    Vertex& mutableVertex();
    Edge& mutableEdge();
    Path& mutablePath();
    List& mutableList();
    Map& mutableMap();
    Set& mutableSet();

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
        std::unique_ptr<Vertex> vVal;
        std::unique_ptr<Edge>   eVal;
        std::unique_ptr<Path>   pVal;
        std::unique_ptr<List>   lVal;
        std::unique_ptr<Map>    mVal;
        std::unique_ptr<Set>    uVal;

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
    void setS(folly::StringPiece v);
    // Date value
    void setD(const Date& v);
    void setD(Date&& v);
    // DateTime value
    void setT(const DateTime& v);
    void setT(DateTime&& v);
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
};

void swap(Value& a, Value& b);

std::ostream& operator<<(std::ostream& os, const Value::Type& type);

}  // namespace nebula


namespace std {

// Inject a customized hash function
template<>
struct hash<nebula::Value> {
    std::size_t operator()(const nebula::Value& h) const noexcept;
};

}  // namespace std
#endif  // DATATYPES_VALUE_H_


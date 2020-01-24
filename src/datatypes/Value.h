/* Copyright (c) 2019 vesoft inc. All rights reserved.
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
    NT_Null = 0,
    NT_NaN = 1,
    NT_BadType = 2
};


struct Value {
    enum Type {
        __EMPTY__ = 0,
        nVal = 1,
        vVal = 2,
        bVal = 3,
        iVal = 4,
        fVal = 5,
        sVal = 6,
        tVal = 7,
        dVal = 8,
        dtVal = 9,
        pathVal = 10,
        listVal = 11,
        mapVal = 12,
    } type_;

    union Storage {
        NullType                nVal;
        VertexID                vVal;
        bool                    bVal;
        int64_t                 iVal;
        double                  fVal;
        std::string             sVal;
        Timestamp               tVal;
        Date                    dVal;
        DateTime                dtVal;
        Path                    pVal;
        std::unique_ptr<List>   listVal;
        std::unique_ptr<Map>    mapVal;

        Storage() {}
        ~Storage() {}
    } value_;

    // Constructors
    Value() : type_(Type::__EMPTY__) {}
    Value(Value&& rhs);
    Value(const Value& rhs);

    bool operator==(const Value& rhs) const;

    Value& operator=(Value&& rhs);
    Value& operator=(const Value& rhs);

private:
  template <class T>
  void destruct(T& val) {
    (&val)->~T();
  }

  void clear();
};

void swap(Value& a, Value& b);


struct Map {
    std::unordered_map<std::string, Value> kvs;

    bool operator==(const Map& rhs) const {
        return kvs == rhs.kvs;
    }
};


struct List {
    std::vector<Value> values;

    bool operator==(const List& rhs) const {
        return values == rhs.values;
    }
};

}  // namespace nebula
#endif  // DATATYPES_VALUE_H_


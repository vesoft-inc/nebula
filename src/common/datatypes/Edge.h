/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_EDGE_H_
#define COMMON_DATATYPES_EDGE_H_

#include <unordered_map>

#include "common/thrift/ThriftTypes.h"
#include "common/datatypes/Value.h"

namespace nebula {

struct Edge {
    VertexID src;
    VertexID dst;
    EdgeType type;
    std::string name;
    EdgeRanking ranking;
    std::unordered_map<std::string, Value> props;

    Edge() {}
    Edge(Edge&& v) noexcept
        : src(std::move(v.src))
        , dst(std::move(v.dst))
        , type(std::move(v.type))
        , name(std::move(v.name))
        , ranking(std::move(v.ranking))
        , props(std::move(v.props)) {}
    Edge(const Edge& v)
        : src(v.src)
        , dst(v.dst)
        , type(v.type)
        , name(v.name)
        , ranking(v.ranking)
        , props(v.props) {}
    Edge(VertexID s,
         VertexID d,
         EdgeType t,
         std::string n,
         EdgeRanking r,
         std::unordered_map<std::string, Value>&& p)
        : src(std::move(s))
        , dst(std::move(d))
        , type(std::move(t))
        , name(std::move(n))
        , ranking(std::move(r))
        , props(std::move(p)) {}

    void clear() {
        src.clear();
        dst.clear();
        type = 0;
        name.clear();
        ranking = 0;
        props.clear();
    }

    std::string toString() const;

    bool operator==(const Edge& rhs) const {
        return src == rhs.src &&
               dst == rhs.dst &&
               type == rhs.type &&
               ranking == rhs.ranking &&
               props == rhs.props;
    }

    void format() {
        if (type < 0) {
            reverse();
        }
    }

    void reverse() {
        type = -type;
        auto tmp = std::move(src);
        src = std::move(dst);
        dst = std::move(tmp);
    }

    bool operator<(const Edge& rhs) const {
        if (src != rhs.src) {
            return src < rhs.src;
        }
        if (dst != rhs.dst) {
            return dst < rhs.dst;
        }
        if (type != rhs.type) {
            return type < rhs.type;
        }
        if (ranking != rhs.ranking) {
            return ranking < rhs.ranking;
        }
        if (props.size() != rhs.props.size()) {
            return props.size() < rhs.props.size();
        }
        return false;
    }
};

inline std::ostream &operator<<(std::ostream& os, const Edge& v) {
    return os << v.toString();
}

}  // namespace nebula


namespace std {

// Inject a customized hash function
template<>
struct hash<nebula::Edge> {
    std::size_t operator()(const nebula::Edge& h) const noexcept;
};

}  // namespace std
#endif  // COMMON_DATATYPES_EDGE_H_

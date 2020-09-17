/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_EDGE_H_
#define COMMON_DATATYPES_EDGE_H_

#include "common/base/Base.h"
#include <folly/hash/Hash.h>
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

    std::string toString() const {
        std::stringstream os;
        os << "(" << src << ")"
            << "-" << "[" << name << "(" << type << ")]" << "->"
            << "(" << dst << ")"
            << "@" << ranking;
        if (!props.empty()) {
            std::vector<std::string> value(props.size());
            std::transform(
                props.begin(), props.end(), value.begin(), [](const auto& iter) -> std::string {
                    std::stringstream out;
                    out << iter.first << ":" << iter.second;
                    return out.str();
                });
            os << " " << folly::join(",", value);
        }
        return os.str();
    }

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
};

inline std::ostream &operator<<(std::ostream& os, const Edge& v) {
    return os << v.toString();
}

}  // namespace nebula


namespace std {

// Inject a customized hash function
template<>
struct hash<nebula::Edge> {
    std::size_t operator()(const nebula::Edge& h) const noexcept {
        size_t hv = folly::hash::fnv64(h.src);
        hv = folly::hash::fnv64(h.dst, hv);
        hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.type),
                                    sizeof(h.type),
                                    hv);
        return folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.ranking),
                                      sizeof(h.ranking),
                                      hv);
    }
};

}  // namespace std
#endif  // COMMON_DATATYPES_EDGE_H_

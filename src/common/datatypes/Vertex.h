/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_VERTEX_H_
#define COMMON_DATATYPES_VERTEX_H_

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"
#include "common/datatypes/Value.h"

namespace nebula {

struct Tag {
    std::string name;
    std::unordered_map<std::string, Value> props;

    Tag() = default;
    Tag(Tag&& tag) noexcept
        : name(std::move(tag.name))
        , props(std::move(tag.props)) {}
    Tag(const Tag& tag)
        : name(tag.name)
        , props(tag.props) {}
    Tag(std::string tagName, std::unordered_map<std::string, Value> tagProps)
        : name(std::move(tagName))
        , props(std::move(tagProps)) {}

    void clear() {
        name.clear();
        props.clear();
    }

    std::string toString() const {
        std::vector<std::string> value(props.size());
        std::transform(
            props.begin(), props.end(), value.begin(), [](const auto& iter) -> std::string {
                std::stringstream out;
                out << iter.first << ":" << iter.second;
                return out.str();
            });

        std::stringstream os;
        os << "Tag: " << name << ", " << folly::join(",", value);
        return os.str();
    }

    Tag& operator=(Tag&& rhs) noexcept {
        if (&rhs != this) {
            name = std::move(rhs.name);
            props = std::move(rhs.props);
        }
        return *this;
    }

    Tag& operator=(const Tag& rhs) {
        if (&rhs != this) {
            name = rhs.name;
            props = rhs.props;
        }
        return *this;
    }

    bool operator==(const Tag& rhs) const {
        return name == rhs.name && props == rhs.props;
    }
};


struct Vertex {
    VertexID vid;
    std::vector<Tag> tags;

    Vertex() = default;
    Vertex(const Vertex& v) : vid(v.vid), tags(v.tags) {}
    Vertex(Vertex&& v) noexcept
        : vid(std::move(v.vid))
        , tags(std::move(v.tags)) {}
    Vertex(VertexID id, std::vector<Tag> t)
        : vid(std::move(id))
        , tags(std::move(t)) {}

    void clear() {
        vid.clear();
        tags.clear();
    }

    std::string toString() const {
        std::stringstream os;
        os << "(" << vid << ")";
        if (!tags.empty()) {
            os << " ";
            for (const auto& tag : tags) {
                os << tag.toString();
            }
        }
        return os.str();
    }

    Vertex& operator=(Vertex&& rhs) noexcept {
        if (&rhs != this) {
            vid = std::move(rhs.vid);
            tags = std::move(rhs.tags);
        }
        return *this;
    }

    Vertex& operator=(const Vertex& rhs) {
        if (&rhs != this) {
            vid = rhs.vid;
            tags = rhs.tags;
        }
        return *this;
    }

    bool operator==(const Vertex& rhs) const {
        return vid == rhs.vid && tags == rhs.tags;
    }

    bool operator<(const Vertex& rhs) const {
        if (vid != rhs.vid) {
            return vid < rhs.vid;
        }
        if (tags.size() != rhs.tags.size()) {
            return tags.size() < rhs.tags.size();
        }
        return false;
    }
};


inline void swap(Vertex& a, Vertex& b) {
    auto temp = std::move(a);
    a = std::move(b);
    b = std::move(temp);
}

inline std::ostream &operator<<(std::ostream& os, const Vertex& v) {
    return os << v.toString();
}

}  // namespace nebula


namespace std {

// Inject a customized hash function
template<>
struct hash<nebula::Tag> {
    std::size_t operator()(const nebula::Tag& h) const noexcept {
        return folly::hash::fnv64(h.name);
    }
};


template<>
struct hash<nebula::Vertex> {
    std::size_t operator()(const nebula::Vertex& h) const noexcept {
        size_t hv = folly::hash::fnv64(h.vid);
        for (auto& t : h.tags) {
            hv += (hv << 1) + (hv << 4) + (hv << 5) + (hv << 7) + (hv << 8) + (hv << 40);
            hv ^= hash<nebula::Tag>()(t);
        }

        return hv;
    }
};

}  // namespace std
#endif  // COMMON_DATATYPES_VERTEX_H_

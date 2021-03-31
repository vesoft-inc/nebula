/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_PATH_H_
#define COMMON_DATATYPES_PATH_H_

#include "common/thrift/ThriftTypes.h"
#include "common/datatypes/Value.h"
#include "common/datatypes/Vertex.h"

namespace nebula {

struct Step {
    Vertex dst;
    EdgeType type;
    std::string name;
    EdgeRanking ranking;
    std::unordered_map<std::string, Value> props;

    Step() = default;
    Step(const Step& s) : dst(s.dst)
                        , type(s.type)
                        , name(s.name)
                        , ranking(s.ranking)
                        , props(s.props) {}
    Step(Step&& s) noexcept
        : dst(std::move(s.dst))
        , type(std::move(s.type))
        , name(std::move(s.name))
        , ranking(std::move(s.ranking))
        , props(std::move(s.props)) {}
    Step(Vertex d,
         EdgeType t,
         std::string n,
         EdgeRanking r,
         std::unordered_map<std::string, Value> p) noexcept
        : dst(std::move(d)), type(t), name(std::move(n)), ranking(r), props(std::move(p)) {}

    void clear() {
        dst.clear();
        type = 0;
        name.clear();
        ranking = 0;
        props.clear();
    }

    void __clear() {
        clear();
    }

    std::string toString() const {
        std::stringstream os;
        os << "-[" << name << "(" << type << ")]->"
           << "(" << dst << ")"
           << "@" << ranking << " ";
        for (const auto& prop : props) {
            os << prop.first << ":" << prop.second << ",";
        }
        auto path = os.str();
        path.pop_back();
        return path;
    }

    Step& operator=(Step&& rhs) noexcept {
        if (&rhs != this) {
            dst = std::move(rhs.dst);
            type = std::move(rhs.type);
            name = std::move(rhs.name);
            ranking = std::move(rhs.ranking);
            props = std::move(rhs.props);
        }
        return *this;
    }

    Step& operator=(const Step& rhs) noexcept {
        if (&rhs != this) {
            dst = rhs.dst;
            type = rhs.type;
            name = rhs.name;
            ranking = rhs.ranking;
            props = rhs.props;
        }
        return *this;
    }

    bool operator==(const Step& rhs) const {
        return dst == rhs.dst &&
               type == rhs.type &&
               name == rhs.name &&
               ranking == rhs.ranking &&
               props == rhs.props;
    }

    bool operator<(const Step& rhs) const {
        if (dst != rhs.dst) {
            return dst < rhs.dst;
        }
        if (type != rhs.dst) {
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


struct Path {
    Vertex src;
    std::vector<Step> steps;

    Path() = default;
    Path(const Path& p) = default;
    Path(Path&& p) noexcept
        : src(std::move(p.src)), steps(std::move(p.steps)) {}
    Path(Vertex v, std::vector<Step> s)
        : src(std::move(v))
        , steps(std::move(s)) {}

    void clear() {
        src.clear();
        steps.clear();
    }

    void __clear() {
        clear();
    }

    std::string toString() const {
        std::stringstream os;
        os << "(" << src << ")";
        os << " ";
        for (const auto &s : steps) {
            os << s.toString();
            os << " ";
        }
        return os.str();
    }

    Path& operator=(Path&& rhs) noexcept {
        if (&rhs != this) {
            src = std::move(rhs.src);
            steps = std::move(rhs.steps);
        }
        return *this;
    }

    Path& operator=(const Path& rhs) noexcept {
        if (&rhs != this) {
            src = rhs.src;
            steps = rhs.steps;
        }
        return *this;
    }

    bool operator==(const Path& rhs) const {
        return src == rhs.src &&
               steps == rhs.steps;
    }

    void addStep(Step step) {
        steps.emplace_back(std::move(step));
    }

    void reverse();

    // Append a path to another one.
    // 5->4>3 appended by 3->2->1 => 5->4->3->2->1
    bool append(Path path);

    bool operator<(const Path& rhs) const {
        if (src != rhs.src) {
            return src < rhs.src;
        }
        if (steps != rhs.steps) {
            return steps < rhs.steps;
        }
        return false;
    }

    bool hasDuplicateEdges() const;
    bool hasDuplicateVertices() const;
};

inline void swap(Step& a, Step& b) {
    auto tmp = std::move(a);
    a = std::move(b);
    b = std::move(tmp);
}

inline std::ostream &operator<<(std::ostream& os, const Path& p) {
    return os << p.toString();
}

}  // namespace nebula


namespace std {

template<>
struct hash<nebula::Step> {
    std::size_t operator()(const nebula::Step& h) const noexcept;
};


template<>
struct hash<nebula::Path> {
    std::size_t operator()(const nebula::Path& h) const noexcept;
};

}  // namespace std
#endif  // COMMON_DATATYPES_PATH_H_

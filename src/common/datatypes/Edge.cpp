/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/String.h>
#include <folly/hash/Hash.h>

#include <common/datatypes/Edge.h>

namespace nebula {

std::string Edge::toString() const {
    std::stringstream os;
    os << "(" << src << ")"
       << "-"
       << "[" << name << "(" << type << ")]"
       << "->"
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

}   // namespace nebula

namespace std {

// Inject a customized hash function
std::size_t hash<nebula::Edge>::operator()(const nebula::Edge& h) const noexcept {
    const auto& src = h.type > 0 ? h.src.toString() : h.dst.toString();
    const auto& dst = h.type > 0 ? h.dst.toString() : h.src.toString();
    auto type = h.type > 0 ? h.type : -h.type;
    size_t hv = folly::hash::fnv64(src);
    hv = folly::hash::fnv64(dst, hv);
    hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&type), sizeof(type), hv);
    return folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.ranking), sizeof(h.ranking), hv);
}

}   // namespace std

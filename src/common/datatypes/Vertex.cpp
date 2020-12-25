/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/hash/Hash.h>
#include <folly/String.h>

#include "common/datatypes/Vertex.h"

namespace nebula {

std::string Tag::toString() const {
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

}  // namespace nebula


namespace std {

// Inject a customized hash function
std::size_t hash<nebula::Tag>::operator()(const nebula::Tag& h) const noexcept {
    return folly::hash::fnv64(h.name);
}

std::size_t hash<nebula::Vertex>::operator()(const nebula::Vertex& h) const noexcept {
    size_t hv = folly::hash::fnv64(h.vid.toString());
    for (auto& t : h.tags) {
        hv += (hv << 1) + (hv << 4) + (hv << 5) + (hv << 7) + (hv << 8) + (hv << 40);
        hv ^= hash<nebula::Tag>()(t);
    }

    return hv;
}

}  // namespace std

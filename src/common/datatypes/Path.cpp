/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/datatypes/Path.h"

#include <algorithm>
#include <tuple>
#include <unordered_set>

#include <folly/String.h>
#include <folly/hash/Hash.h>

namespace nebula {
void Path::reverse() {
    if (steps.empty()) {
        return;
    }
    std::reverse(steps.begin(), steps.end());
    swap(src, steps.front().dst);
    for (size_t i = 0; i < steps.size() - 1; ++i) {
        swap(steps[i].dst, steps[i + 1].dst);
        steps[i].type = -steps[i].type;
    }
    steps.back().type = -steps.back().type;
}

bool Path::append(Path path) {
    if (src != path.src && steps.back().dst != path.src) {
        return false;
    }
    steps.reserve(steps.size() + path.steps.size());
    steps.insert(steps.end(),
                 std::make_move_iterator(path.steps.begin()),
                 std::make_move_iterator(path.steps.end()));
    return true;
}

bool Path::hasDuplicateVertices() const {
    if (steps.empty()) {
        return false;
    }
    std::unordered_set<Value> uniqueVid;
    uniqueVid.reserve(steps.size() + 1);
    uniqueVid.emplace(src.vid);
    for (const auto& step : steps) {
        auto ret = uniqueVid.emplace(step.dst.vid);
        if (!ret.second) {
            return true;
        }
    }
    return false;
}

bool Path::hasDuplicateEdges() const {
    if (steps.size() < 2) {
        return false;
    }
    using Key = std::tuple<Value, Value, EdgeType, EdgeRanking>;
    std::unordered_set<Key> uniqueSet;
    uniqueSet.reserve(steps.size());
    auto srcVid = src.vid;
    for (const auto& step : steps) {
        const auto& dstVid = step.dst.vid;
        bool ret = true;
        if (step.type > 0) {
            ret = uniqueSet.emplace(srcVid, dstVid, step.type, step.ranking).second;
        } else {
            ret = uniqueSet.emplace(dstVid, srcVid, -step.type, step.ranking).second;
        }
        if (!ret) {
            return true;
        }
        srcVid = dstVid;
    }
    return false;
}

}  // namespace nebula

namespace std {

std::size_t hash<nebula::Step>::operator()(const nebula::Step& h) const noexcept {
    size_t hv = hash<nebula::Vertex>()(h.dst);
    hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.type),
                                sizeof(h.type),
                                hv);
    return folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.ranking),
                                    sizeof(h.ranking),
                                    hv);
}

std::size_t hash<nebula::Path>::operator()(const nebula::Path& h) const noexcept {
    size_t hv = hash<nebula::Vertex>()(h.src);
    for (auto& s : h.steps) {
        hv += (hv << 1) + (hv << 4) + (hv << 5) + (hv << 7) + (hv << 8) + (hv << 40);
        hv ^= hash<nebula::Step>()(s);
    }

    return hv;
}

}  // namespace std

/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Query.h"

#include <folly/String.h>

using folly::stringPrintf;

namespace nebula {
namespace graph {
std::string GetNeighbors::explain() const {
    // TODO:
    return "GetNeighbors";
}

std::string GetVertices::explain() const {
    // TODO:
    return "GetVertices";
}

std::string GetEdges::explain() const {
    // TODO:
    return "GetEdges";
}

std::string IndexScan::explain() const {
    // TODO:
    return "IndexScan";
}

std::string Filter::explain() const {
    // TODO:
    return "Filter";
}

std::string Union::explain() const {
    // TODO:
    return "Union";
}

std::string Intersect::explain() const {
    // TODO:
    return "Intersect";
}

std::string Minus::explain() const {
    // TODO:
    return "Minus";
}

std::string Project::explain() const {
    // TODO:
    return "Project";
}

std::string Sort::explain() const {
    // TODO:
    return "Sort";
}

std::string Limit::explain() const {
    std::string buf;
    buf.reserve(256);
    buf += "Limit: ";
    buf += folly::stringPrintf("offset %ld, count %ld", offset_, count_);
    return buf;
}

std::string Aggregate::explain() const {
    // TODO:
    return "Aggregate";
}
std::string SwitchSpace::explain() const {
    return "SwitchSpace";
}

std::string Dedup::explain() const {
    return "Dedup";
}

std::string DataCollect::explain() const {
    return "DataCollect";
}
}  // namespace graph
}  // namespace nebula

/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "Query.h"

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
    buf.reserve(32);
    buf += "Limit: ";
    buf += folly::StringPrintf("offset %ld, count %ld", offset_, count_);
    return buf;
}

std::string Aggregate::explain() const {
    // TODO:
    return "Aggregate";
}
}  // namespace graph
}  // namespace nebula

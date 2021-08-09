/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "context/Symbols.h"

#include <sstream>

#include "planner/plan/PlanNode.h"
#include "util/Utils.h"

namespace nebula {
namespace graph {

std::string Variable::toString() const {
    std::stringstream ss;
    ss << "name: " << name << ", type: " << type << ", colNames: <" << folly::join(",", colNames)
       << ">, readBy: <" << util::join(readBy, [](auto pn) { return pn->toString(); })
       << ">, writtenBy: <" << util::join(writtenBy, [](auto pn) { return pn->toString(); }) << ">";
    return ss.str();
}

std::string SymbolTable::toString() const {
    std::stringstream ss;
    ss << "SymTable: [";
    for (const auto& p : vars_) {
        ss << "\n" << p.first << ": ";
        if (p.second) {
            ss << p.second->toString();
        }
    }
    ss << "\n]";
    return ss.str();
}

}   // namespace graph
}   // namespace nebula

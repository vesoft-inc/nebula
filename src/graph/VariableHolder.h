/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_VARIABLEHOLDER_H_
#define GRAPH_VARIABLEHOLDER_H_

#include "base/Base.h"
#include "graph/InterimResult.h"

namespace nebula {
namespace session {
class Session;
}

namespace graph {

class InterimResult;
class VariableHolder final {
public:
    explicit VariableHolder(session::Session* session)
        : session_(session) { }

    void add(const std::string &var, std::unique_ptr<InterimResult> result, bool global = false);

    const InterimResult* get(const std::string &var, bool *existing = nullptr) const;

private:
    std::unordered_map<std::string, std::unique_ptr<InterimResult>> holder_;
    mutable std::unordered_set<std::shared_ptr<const InterimResult>> gHolder_;
    session::Session* session_;
};

class GlobalVariableHolder final {
public:
    void add(const std::string &var, std::unique_ptr<InterimResult> result);

    std::shared_ptr<const InterimResult> get(const std::string &var, bool *existing = nullptr) const;

private:
    mutable folly::RWSpinLock lock_;
    std::unordered_map<std::string, std::shared_ptr<const InterimResult>> holder_;
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_VARIABLEHOLDER_H_

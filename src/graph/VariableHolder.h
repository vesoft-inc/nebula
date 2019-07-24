/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_VARIABLEHOLDER_H_
#define GRAPH_VARIABLEHOLDER_H_

#include "base/Base.h"

namespace nebula {
namespace graph {

class InterimResult;
class VariableHolder final {
public:
    VariableHolder();
    ~VariableHolder();
    VariableHolder(const VariableHolder&) = delete;
    VariableHolder& operator=(const VariableHolder&) = delete;
    VariableHolder(VariableHolder &&) noexcept;
    VariableHolder& operator=(VariableHolder &&) noexcept;

    void add(const std::string &var, std::unique_ptr<InterimResult> result);

    const InterimResult* get(const std::string &var, bool *existing = nullptr) const;

private:
    std::unordered_map<std::string, std::unique_ptr<InterimResult>> holder_;
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_VARIABLEHOLDER_H_

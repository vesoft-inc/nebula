/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_VARIABLEHOLDER_H_
#define GRAPH_VARIABLEHOLDER_H_

#include "base/Base.h"

namespace nebula {
namespace graph {

class IntermResult;
class VariableHolder final {
public:
    VariableHolder();
    ~VariableHolder();
    VariableHolder(const VariableHolder&) = delete;
    VariableHolder& operator=(const VariableHolder&) = delete;
    VariableHolder(VariableHolder &&) noexcept;
    VariableHolder& operator=(VariableHolder &&) noexcept;

    void add(const std::string &var, std::unique_ptr<IntermResult> result);

    const IntermResult* get(const std::string &var) const;

private:
    std::unordered_map<std::string, std::unique_ptr<IntermResult>> holder_;
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_VARIABLEHOLDER_H_

/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_EXECUTIONCONTEXT_H_
#define CONTEXT_EXECUTIONCONTEXT_H_

#include "common/datatypes/Value.h"
#include "context/Result.h"

namespace nebula {
namespace graph {

class QueryInstance;

/***************************************************************************
 *
 * The context for each query request
 *
 * The context is NOT thread-safe. The execution plan has to guarantee
 * all accesses to context are safe by pre-allocate all place for variables in semantic analysis
 * and the single variable is accessed sequentially in runtime, so don't add/delete variable
 * in runtime it's safe to access
 * The life span of the context is same as the request. That means a new
 * context object will be created as soon as the query engine receives the
 * query request. The context object will be visible to the parser, the
 * planner, the optimizer, and the executor.
 *
 **************************************************************************/
class ExecutionContext {
public:
    // 0 is the latest, -1 is the previous one, and so on
    // 1 is the oldest, 2 is the second elder, and so on
    static constexpr int64_t kLatestVersion = 0;
    static constexpr int64_t kOldestVersion = 1;
    static constexpr int64_t kPreviousOneVersion = -1;

    ExecutionContext() = default;

    virtual ~ExecutionContext() = default;

    void initVar(const std::string& name) {
        valueMap_[name];
    }

    // Get the latest version of the value
    const Value& getValue(const std::string& name) const;

    const Result& getResult(const std::string& name) const;

    const Result& getVersionedResult(const std::string& name, int64_t version) const;

    size_t numVersions(const std::string& name) const;

    // Return all existing history of the value. The front is the latest value
    // and the back is the oldest value
    const std::vector<Result>& getHistory(const std::string& name) const;

    void setValue(const std::string& name, Value&& val);

    void setResult(const std::string& name, Result&& result);

    void dropResult(const std::string& name);

    // Only keep the last several versoins of the Value
    void truncHistory(const std::string& name, size_t numVersionsToKeep);

    bool exist(const std::string& name) const {
        return valueMap_.find(name) != valueMap_.end();
    }

private:
    friend class QueryInstance;
    Value moveValue(const std::string& name);

    // name -> Value with multiple versions
    std::unordered_map<std::string, std::vector<Result>>     valueMap_;
};

}  // namespace graph
}  // namespace nebula
#endif  // CONTEXT_EXECUTIONCONTEXT_H_

/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_QUERYCONTEXT_H_
#define CONTEXT_QUERYCONTEXT_H_

#include "base/Base.h"
#include "datatypes/Value.h"

namespace nebula {

/***************************************************************************
 *
 * The context for each query request
 *
 * The context is NOT thread-safe. The execution plan has to guarantee
 * all accesses to context are safe
 *
 * The life span of the context is same as the request. That means a new
 * context object will be created as soon as the query engine receives the
 * query request. The context object will be visible to the parser, the
 * planner, the optimizer, and the executor.
 *
 **************************************************************************/
class QueryContext {
public:
    QueryContext() = default;
    virtual ~QueryContext() = default;

    // Get the latest version of the value
    const Value& getValue(const std::string& name) const;

    size_t numVersions(const std::string& name) const;

    // Return all existing history of the value. The front is the latest value
    // and the back is the oldest value
    const std::list<Value>& getHistory(const std::string& name) const;

    void setValue(const std::string& name, Value&& val);

    void deleteValue(const std::string& name);

    // Only keep the last several versoins of the Value
    void truncHistory(const std::string& name, size_t numVersionsToKeep);

private:
    // name -> Value with multiple versions
    std::unordered_map<std::string, std::list<Value>> valueMap_;
};

}  // namespace nebula
#endif  // CONTEXT_QUERYCONTEXT_H_



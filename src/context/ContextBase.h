/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_CONTEXTBASE_H_
#define CONTEXT_CONTEXTBASE_H_

#include "base/Base.h"
#include "folly/RWSpinLock.h"
#include "datatypes/Value.h"
#include "datatypes/DataSet.h"

namespace nebula {

/***************************************************************************
 *
 * The base class for all Context objects
 *
 * The context is NOT thread-safe.
 *
 **************************************************************************/
class ContextBase {
public:
    virtual ~ContextBase() = default;

    // Get the latest version of the value
    virtual const Value& getValue(const std::string& name) const = 0;

    // Return the number of versions for the given variable
    virtual size_t numVersions(const std::string& name) const = 0;

    // Return all existing history of the value. The front is the latest value
    // and the back is the oldest value
    virtual const std::list<Value>& getHistory(const std::string& name) const = 0;
};

}  // namespace nebula
#endif  // CONTEXT_CONTEXTBASE_H_

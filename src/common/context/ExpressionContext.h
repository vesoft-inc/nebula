/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_CONTEXT_EXPRESSIONCONTEXT_H_
#define COMMON_CONTEXT_EXPRESSIONCONTEXT_H_

#include "common/base/Base.h"
#include <folly/RWSpinLock.h>
#include "common/datatypes/Value.h"
#include "common/datatypes/DataSet.h"

namespace nebula {

/***************************************************************************
 *
 * The base class for all ExpressionContext implementations
 *
 * The context is NOT thread-safe
 *
 **************************************************************************/
class ExpressionContext {
public:
    virtual ~ExpressionContext() = default;

    // Get the latest version value for the given variable name, such as $a, $b
    virtual const Value& getVar(const std::string& var) const = 0;

    // Get the given version value for the given variable name, such as $a, $b
    virtual const Value& getVersionedVar(const std::string& var,
                                         int64_t version) const = 0;

    // Get the specified property from a variable, such as $a.prop_name
    virtual const Value& getVarProp(const std::string& var,
                                    const std::string& prop) const = 0;

    // Get the specified property from the edge, such as edge_type.prop_name
    virtual Value getEdgeProp(const std::string& edgeType,
                              const std::string& prop) const = 0;

    // Get the specified property from the source vertex, such as $^.tag_name.prop_name
    virtual Value getSrcProp(const std::string& tag,
                             const std::string& prop) const = 0;

    // Get the specified property from the destination vertex, such as $$.tag_name.prop_name
    virtual const Value& getDstProp(const std::string& tag,
                                    const std::string& prop) const = 0;

    // Get the specified property from the input, such as $-.prop_name
    virtual const Value& getInputProp(const std::string& prop) const = 0;

    virtual void setVar(const std::string& var, Value val) = 0;
};

}  // namespace nebula
#endif  // COMMON_CONTEXT_EXPRESSIONCONTEXT_H_

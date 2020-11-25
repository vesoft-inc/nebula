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

    // Get the specified property from the tag, such as tag.prop_name
    virtual Value getTagProp(const std::string& tag,
                             const std::string& prop) const = 0;

    // Get the specified property from the source vertex, such as $^.tag_name.prop_name
    virtual Value getSrcProp(const std::string& tag,
                             const std::string& prop) const = 0;

    // Get the specified property from the destination vertex, such as $$.tag_name.prop_name
    virtual const Value& getDstProp(const std::string& tag,
                                    const std::string& prop) const = 0;

    // Get the specified property from the input, such as $-.prop_name
    virtual const Value& getInputProp(const std::string& prop) const = 0;

    // Get Vertex
    virtual Value getVertex() const = 0;

    // Get Edge
    virtual Value getEdge() const = 0;

    // Get Value by Column index
    virtual Value getColumn(int32_t index) const = 0;

    // Get regex
    const std::regex& getRegex(const std::string& pattern) {
        auto iter = regex_.find(pattern);
        if (iter == regex_.end()) {
            iter = regex_.emplace(pattern, std::regex(pattern)).first;
        }
        return iter->second;
    }

    virtual void setVar(const std::string& var, Value val) = 0;

private:
    std::unordered_map<std::string, std::regex> regex_;
};

}  // namespace nebula
#endif  // COMMON_CONTEXT_EXPRESSIONCONTEXT_H_

/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_FILTERNODE_H_
#define STORAGE_EXEC_FILTERNODE_H_

#include "base/Base.h"
#include "expression/Expression.h"
#include "context/ExpressionContext.h"

namespace nebula {
namespace storage {

class FilterNode {
public:
    FilterNode() = default;

    void fillTagProp(TagID tagId, const std::string& prop, const nebula::Value& value) {
        tagFilters_.emplace(std::make_pair(tagId, prop), value);
    }

    bool eval(const Expression* exp) const {
        UNUSED(exp);
        return true;
    }

private:
    // key: <tagId, propName> -> propValue
    std::unordered_map<std::pair<TagID, std::string>, nebula::Value> tagFilters_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_

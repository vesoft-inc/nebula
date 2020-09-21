/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/IndexUtil.h"

namespace nebula {
namespace graph {

Status IndexUtil::validateColumns(const std::vector<std::string>& fields) {
    std::unordered_set<std::string> fieldSet(fields.begin(), fields.end());
    if (fieldSet.size() != fields.size()) {
        return Status::Error("Found duplicate column field");
    }

    if (fields.empty()) {
        return Status::Error("Column is empty");
    }

    return Status::OK();
}

}  // namespace graph
}  // namespace nebula


/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_STRINGVALUE_H_
#define COMMON_BASE_STRINGVALUE_H_

#include "common/base/Base.h"

namespace nebula {

/**
 * This class is a wrapper of the std::string. It provides operations
 * between std::string and other value types
 */
class StringValue final {
public:
    explicit StringValue(std::string str) : str_(std::move(str)) {}

    const std::string& str() const& {
        return str_;
    }

    std::string&& str() && {
        return std::move(str_);
    }

private:
    std::string str_;
};

}  // namespace nebula
#endif  // COMMON_BASE_STRINGVALUE_H_

/* Copyright (c) 2018 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef COMMON_UTIL_CONVERTTIMETYPE_H_
#define COMMON_UTIL_CONVERTTIMETYPE_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include "common/filter/Expressions.h"


namespace nebula {
/**
* This class converts string to a time type
* */
class ConvertTimeType final {
public:
    ~ConvertTimeType() = default;

    static StatusOr<int64_t> toTimestamp(const VariantType &value);
};

}  // namespace nebula
#endif  // COMMON_UTIL_CONVERTTIMETYPE_H_


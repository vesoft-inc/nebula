/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef COMMON_FUNCTION_TIMEFUNCTION_H
#define COMMON_FUNCTION_TIMEFUNCTION_H

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/Date.h"
#include "common/interface/gen-cpp2/common_types.h"


namespace nebula {

class TimeFunction final {
public:
    TimeFunction() = default;
    ~TimeFunction() = default;

public:
    static StatusOr<Timestamp> toTimestamp(const Value &val);

    static StatusOr<Date> toDate(const Value &val);

    static StatusOr<DateTime> toDateTime(const Value &val);
};
}   // namespace nebula
#endif   // COMMON_FUNCTION_TIMEFUNCTION_H


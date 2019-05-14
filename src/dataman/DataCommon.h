/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATAMAN_DATACOMMON_H_
#define DATAMAN_DATACOMMON_H_

#include "base/Base.h"

namespace nebula {

enum class ResultType {
    SUCCEEDED = 0,
    E_NAME_NOT_FOUND = -1,
    E_INDEX_OUT_OF_RANGE = -2,
    E_INCOMPATIBLE_TYPE = -3,
    E_VALUE_OUT_OF_RANGE = -4,
    E_DATA_INVALID = -5,
};


using FieldValue = boost::variant<bool, int64_t, float, double, std::string>;
#define VALUE_TYPE_BOOL 0
#define VALUE_TYPE_INT 1
#define VALUE_TYPE_FLOAT 2
#define VALUE_TYPE_DOUBLE 3
#define VALUE_TYPE_STRING 4

template<typename IntType>
typename std::enable_if<
    std::is_integral<
        typename std::remove_cv<
            typename std::remove_reference<IntType>::type
        >::type
    >::value,
    bool
>::type
intToBool(IntType iVal) {
    return iVal != 0;
}

inline bool strToBool(folly::StringPiece str) {
    return str == "Y" || str == "y" || str == "T" || str == "t" ||
           str == "yes" || str == "Yes" || str == "YES" ||
           str == "true" || str == "True" || str == "TRUE";
}

}  // namespace nebula
#endif  // DATAMAN_DATACOMMON_H_


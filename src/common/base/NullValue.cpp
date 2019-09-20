/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/NullValue.h"


namespace std {

std::ostream& operator<<(std::ostream& os, const nebula::NullValue&) {
    os << "Null";
    return os;
}

}

namespace nebula {

}  // namespace nebula


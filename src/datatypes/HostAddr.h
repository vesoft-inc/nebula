/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATATYPES_HOSTADDR_H_
#define DATATYPES_HOSTADDR_H_

#include "base/Base.h"
#include "thrift/ThriftTypes.h"

namespace nebula {

// Host address type and utility functions
using HostAddr = std::pair<IPv4, Port>;

std::ostream& operator<<(std::ostream &, const HostAddr&);

}  // namespace nebula
#endif  // DATATYPES_HOSTADDR_H_

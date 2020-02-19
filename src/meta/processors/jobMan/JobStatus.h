/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_KVJOBSTATUS_H_
#define META_KVJOBSTATUS_H_

#include <vector>
#include <string>
#include "gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

class JobStatus {
    using Status = cpp2::JobStatus;
public:
    static std::string toString(Status st);
    static bool laterThan(Status lhs, Status rhs);
private:
    static int phaseNumber(Status st);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_KVJOBSTATUS_H_

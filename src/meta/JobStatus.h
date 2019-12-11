/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_KVJOBSTATUS_H_
#define META_KVJOBSTATUS_H_

#include <vector>
#include <string>

namespace nebula {
namespace meta {

struct JobStatus {
    static const char* queue;
    static const char* running;
    static const char* finished;
    static const char* failed;
    static const char* stopped;

    static int phaseNumber(const std::string& phase);
    static bool laterThan(const std::string& lhs, const std::string& rhs);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_KVJOBSTATUS_H_

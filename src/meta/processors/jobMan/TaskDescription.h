/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_TASKDESCRIPTION_H_
#define META_TASKDESCRIPTION_H_

#include <vector>
#include <string>

namespace nebula {
namespace meta {

struct TaskDescription {
    static const char* delim;
    TaskDescription() {}
    explicit TaskDescription(const std::string& sTask);
    std::string toString();

    std::string status;
    std::string dest;
    std::string startTime;
    std::string stopTime;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_TASKDESCRIPTION_H_

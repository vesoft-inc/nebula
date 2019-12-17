/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_KVJOBDESCRIPTION_H_
#define META_KVJOBDESCRIPTION_H_

#include <vector>
#include <string>
// #include <gtest/gtest_prod.h>

namespace nebula {
namespace meta {

struct JobDescription {
    static const char* delim;
    JobDescription() {}
    explicit JobDescription(const std::string& sJobDesc);
    std::string toString();

    std::string type;
    std::string para;
    std::string status;
    std::string startTime;
    std::string stopTime;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_KVJOBDESCRIPTION_H_

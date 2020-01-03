/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_JOB_UTIL_H_
#define META_JOB_UTIL_H_

#include <ctime>
#include <string>
#include <folly/Optional.h>

namespace nebula {
namespace meta {

class JobUtil{
public:
    static const std::string& jobPrefix();
    static const std::string& currJobKey();
    static const std::string& archivePrefix();
    static std::string strTimeT(std::time_t t);
    static std::string strTimeT(const folly::Optional<std::time_t>& t);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_JOB_UTIL_H_


/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_JOBUTIL_H_
#define META_JOBUTIL_H_

#include <ctime>
#include <string>
#include <vector>
#include <stdexcept>
#include <folly/Optional.h>
#include <folly/Range.h>
#include <folly/String.h>

namespace nebula {
namespace meta {

class JobUtil {
public:
    static const std::string& jobPrefix();
    static const std::string& currJobKey();
    static const std::string& archivePrefix();

    template<typename T>
    static T parseFixedVal(folly::StringPiece rawVal, size_t offset) {
        if (rawVal.size() < offset + sizeof(T)) {
            throw std::runtime_error(folly::stringPrintf("%s: offset=%zu, rawVal.size()=%zu",
                                                         __func__, offset, rawVal.size()));
        }
        return *reinterpret_cast<const T*>(rawVal.data() + offset);
    }

    static std::string
    parseString(folly::StringPiece rawVal, size_t offset);

    static std::vector<std::string>
    parseStrVector(folly::StringPiece rawVal, size_t* offset);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_JOBUTIL_H_


/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METACOMMON_H_
#define META_METACOMMON_H_

#include "base/Base.h"

namespace nebula {
namespace meta {

class MetaCommon final {
public:
    MetaCommon() = delete;

    static bool checkSegment(const std::string& segment) {
        static const std::regex pattern("^[0-9a-zA-Z]+$");
        if (!segment.empty() && std::regex_match(segment, pattern)) {
            return true;
        }
        return false;
    }
};

}  // namespace meta
}  // namespace nebula

#endif  // META_METACOMMON_H_

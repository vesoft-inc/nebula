/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_FILTER_GEO_GEOFILTER_H_
#define COMMON_FILTER_GEO_GEOFILTER_H_

#include "base/Base.h"
#include "base/StatusOr.h"

namespace nebula {
namespace geo {
class GeoFilter final {
public:
    enum class GeoQueryType : uint8_t {
        NEAR,
        WITHIN,
        CONTAIN,
    };

    /**
     * Function near is now used as a normal function that generates the
     * geo code coressponding to the given [lat, lng].
     */
    static StatusOr<std::string> near(const std::vector<VariantType> &args);
};
}  // namespace geo
}  // namespace nebula
#endif

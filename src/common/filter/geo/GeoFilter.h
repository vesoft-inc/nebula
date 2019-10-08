/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_FILTER_GEO_GEOFILTER_H_
#define COMMON_FILTER_GEO_GEOFILTER_H_

#include "base/Base.h"
#include "base/Status.h"

namespace nebula {
namespace geo {
class GeoFilter final {
public:
    enum class GeoQueryType : uint8_t {
        NEAR,
        WITHIN,
        CONTAIN,
    };

    static Status near(const std::vector<VariantType> &args);
};
}  // namespace geo
}  // namespace nebula
#endif

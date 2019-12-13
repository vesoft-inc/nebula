/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "StatsManager.h"

namespace nebula {
namespace stats {

class MetricsSerializer final {
public:
    std::string serialize(StatsManager& sm) const {
        std::ostringstream ss;
        serialize(ss, sm);
        return ss.str();
    }
    void serialize(std::ostream& out, StatsManager& sm) const;
};

}  // namespace stats
}  // namespace nebula

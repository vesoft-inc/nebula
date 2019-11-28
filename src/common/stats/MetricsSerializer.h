/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "StatsManager.h"

namespace nebula {
namespace stats {

class MetricsSerializer {
public:
    virtual ~MetricsSerializer() = default;
    std::string serialize(StatsManager& sm) const {
        std::ostringstream ss;
        serialize(ss, sm);
        return ss.str();
    }
    virtual void serialize(std::ostream& out, StatsManager& sm) const = 0;
};

class PrometheusSerializer final : public MetricsSerializer {
public:
    virtual ~PrometheusSerializer() = default;
    void serialize(std::ostream& out, StatsManager& sm) const override {
        prometheusSerialize(out, sm);
    }
    void prometheusSerialize(std::ostream& out, StatsManager& sm) const;
    struct Label {
        std::string name;
        std::string value;
    };

private:
    void annotate(std::ostream& out, const std::string& metricName,
        const std::string& metricType) const;
    void writeValue(std::ostream& out, double value) const;
    void writeValue(std::ostream& out, const int64_t value) const;
    void writeValue(std::ostream& out, const std::string& value) const;
    template <typename T = std::string>
    void writeHead(std::ostream& out, const std::string& family,
        const std::vector<Label>& labels, const std::string& suffix = "",
        const std::string& extraLabelName = "",
        const T extraLabelValue = T()) const;
    void writeTail(std::ostream& out, const std::int64_t timestampMs = 0) const;
};

}  // namespace stats
}  // namespace nebula

/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "MetricsSerializer.h"

namespace nebula {
namespace stats {

void PrometheusSerializer::annotate(
        std::ostream& out, const std::string& metricName, const std::string& metricType) const {
    out << "# HELP " << metricName << " Record all " << metricType << " about nebula" << "\n";
    out << "# TYPE " << metricName << " " << metricType << "\n";
}

// Write a double as a string, with proper formatting for infinity and NaN
void PrometheusSerializer::writeValue(std::ostream& out, double value) const {
    if (std::isnan(value)) {
        out << "Nan";
    } else if (std::isinf(value)) {
        out << (value < 0 ? "-Inf" : "+Inf");
    } else {
        auto saved_flags = out.setf(std::ios::fixed, std::ios::floatfield);
        out << value;
        out.setf(saved_flags, std::ios::floatfield);
    }
}

void PrometheusSerializer::writeValue(std::ostream& out, const int64_t value) const {
    if (std::isnan(value)) {
        out << "Nan";
    } else if (std::isinf(value)) {
        out << (value < 0 ? "-Inf" : "+Inf");
    } else {
        out << value;
    }
}

void PrometheusSerializer::writeValue(std::ostream& out, const std::string& value) const {
    for (auto c : value) {
        switch (c) {
        case '\n':
            out << '\\' << 'n';
            break;
        case '\\':
            out << '\\' << c;
            break;
        case '"':
            out << '\\' << c;
            break;
        default:
            out << c;
            break;
        }
    }
}

template<typename T>
void PrometheusSerializer::writeHead(std::ostream& out, const std::string& family,
        const std::vector<Label>& labels, const std::string& suffix,
        const std::string& extraLabelName,
        const T extraLabelValue) const {
    out << family << suffix;
    if (!labels.empty() || !extraLabelName.empty()) {
        out << "{";
        const char* prefix = "";

        for (auto& lp : labels) {
            out << prefix << lp.name << "=\"";
            writeValue(out, lp.value);
            out << "\"";
            prefix = ",";
        }
        if (!extraLabelName.empty()) {
            out << prefix << extraLabelName << "=\"";
            writeValue(out, extraLabelValue);
            out << "\"";
        }
        out << "}";
    }
    out << " ";
}

// Write a line trailer: timestamp
void PrometheusSerializer::writeTail(std::ostream& out, const std::int64_t timestampMs) const {
    if (timestampMs != 0) {
        out << " " << timestampMs;
    }
    out << "\n";
}


void PrometheusSerializer::prometheusSerialize(std::ostream& out, StatsManager& sm) const {
    std::locale saved_locale = out.getloc();
    out.imbue(std::locale::classic());
    for (auto& index : sm.nameMap_) {
        auto parsedName = sm.idParsedName_[index.second].get();
        std::string name = parsedName != nullptr ? parsedName->name : index.first;
        if (StatsManager::isStatIndex(index.second)) {
            annotate(out, name, "gauge");

            writeHead(out, name, {});
            writeValue(out, StatsManager::readStats(index.second,
                StatsManager::TimeRange::ONE_MINUTE, StatsManager::StatsMethod::AVG).value());
            writeTail(out);
        } else if (StatsManager::isHistoIndex(index.second)) {
            annotate(out, name, "histogram");

            writeHead(out, name, {}, "_count");
            if (parsedName != nullptr) {
                out << StatsManager::readStats(index.second,
                    parsedName->range, StatsManager::StatsMethod::COUNT).value();
            } else {
                out << StatsManager::readStats(index.second,
                    StatsManager::TimeRange::ONE_HOUR, StatsManager::StatsMethod::COUNT).value();
            }
            writeTail(out);

            writeHead(out, name, {}, "_sum");
            if (parsedName != nullptr) {
                writeValue(out, StatsManager::readStats(index.second,
                    parsedName->range, StatsManager::StatsMethod::SUM).value());
            } else {
                writeValue(out, StatsManager::readStats(index.second,
                    StatsManager::TimeRange::ONE_HOUR, StatsManager::StatsMethod::SUM).value());
            }
            writeTail(out);

            auto& p = sm.histograms_[StatsManager::physicalHistoIndex(index.second)];
            std::lock_guard<std::mutex> lk(*p.first);
            auto& hist = p.second;
            auto last = hist->getMax();
            auto cumulativeCount = 0;
            auto diff = (hist->getMax() - hist->getMin()) /
                static_cast<double>(hist->getNumBuckets());
            double bound = hist->getMin() + diff;
            for (std::size_t i = 0; i < hist->getNumBuckets(); ++i) {
                writeHead(out, name, {}, "_bucket", "le", bound);
                if (parsedName != nullptr) {
                    cumulativeCount +=hist->getBucket(i)
                        .count(static_cast<std::size_t>(parsedName->range));
                } else {
                    cumulativeCount += hist->getBucket(i)
                        .count(static_cast<std::size_t>(StatsManager::TimeRange::ONE_HOUR));
                }
                out << cumulativeCount;
                writeTail(out);
                bound += diff;
            }

            if (!std::isinf(last)) {
                writeHead(out, name, {}, "_bucket", "le", "+Inf");
                out << cumulativeCount;
                writeTail(out);
            }
        } else {
            DCHECK(false) << "Impossible neither stat nor histogram!";
        }
    }

    out.imbue(saved_locale);
}

}  // namespace stats
}  // namespace nebula

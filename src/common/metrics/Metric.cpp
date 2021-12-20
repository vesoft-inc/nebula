/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/metrics/Metric.h"

namespace nebula {
namespace metric {

void parseStats(const folly::StringPiece stats,
                std::vector<StatsMethod>& methods,
                std::vector<std::pair<std::string, double>>& percentiles) {
  std::vector<std::string> parts;
  folly::split(",", stats, parts, true);

  for (auto& part : parts) {
    // Now check the statistic method
    std::string trimmedPart = folly::trimWhitespace(part).toString();
    folly::toLowerAscii(trimmedPart);
    if (trimmedPart == "sum") {
      methods.push_back(StatsMethod::SUM);
    } else if (trimmedPart == "count") {
      methods.push_back(StatsMethod::COUNT);
    } else if (trimmedPart == "avg") {
      methods.push_back(StatsMethod::AVG);
    } else if (trimmedPart == "rate") {
      methods.push_back(StatsMethod::RATE);
    } else if (trimmedPart[0] == 'p') {
      // Percentile
      double pct;
      if (strToPct(trimmedPart, pct)) {
        percentiles.emplace_back(trimmedPart, pct);
      } else {
        LOG(ERROR) << "\"" << trimmedPart << "\" is not a valid percentile form";
      }
    } else {
      LOG(ERROR) << "Unsupported statistic method \"" << trimmedPart << "\"";
    }
  }
}

bool strToPct(folly::StringPiece part, double& pct) {
  static const int32_t divisors[] = {1, 1, 10, 100, 1000, 10000};
  try {
    size_t len = part.size() - 1;
    if (len > 0 && len <= 6) {
      auto digits = folly::StringPiece(&(part[1]), len);
      pct = folly::to<double>(digits) / divisors[len - 1];
      return true;
    } else {
      LOG(ERROR) << "Precision " << part.toString() << " is too long";
      return false;
    }
  } catch (const std::exception& ex) {
    LOG(ERROR) << "Failed to convert the digits to a double: " << ex.what();
  }

  return false;
}

}  // namespace metric
}  // namespace nebula

// namespace std {

// // Inject a customized hash function
// std::size_t hash<nebula::metric::LabelValues>::operator()(
//     const nebula::metric::LabelValues& h) const noexcept {
//   std::string s = folly::join("", h);
//   return std::hash<std::string>()(s);
// }

// }  // namespace std

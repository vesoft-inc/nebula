/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#pragma once
#include <sstream>
namespace nebula {
namespace stats {
class MetricsReader {
 public:
  template <typename ValueType>
  MetricsReader& append(const std::string& name, ValueType value) {
    stream_ << name << value;
    return *this;
  }

 private:
  std::ostringstream stream_;
};
}  // namespace stats
}  // namespace nebula

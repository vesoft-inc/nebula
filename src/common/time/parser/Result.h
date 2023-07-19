/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "common/datatypes/Date.h"

namespace nebula {
namespace time {

struct TimeResult {
  Time t;
  bool withTimeZone{false};
};

static inline bool operator==(const TimeResult& lhs, const TimeResult& rhs) {
  return lhs.t == rhs.t && lhs.withTimeZone == rhs.withTimeZone;
}

static inline std::ostream& operator<<(std::ostream& os, const TimeResult& result) {
  os << result.t;
  if (result.withTimeZone) {
    os << "Z";
  }
  return os;
}

struct Result {
  TimeResult time() const {
    return TimeResult{dt.time(), withTimeZone};
  }

  DateTime dt;
  bool withTimeZone{false};
};

static inline bool operator==(const Result& lhs, const Result& rhs) {
  return lhs.dt == rhs.dt && lhs.withTimeZone == rhs.withTimeZone;
}

static inline std::ostream& operator<<(std::ostream& os, const Result& result) {
  os << result.dt;
  if (result.withTimeZone) {
    os << "Z";
  }
  return os;
}

}  // namespace time
}  // namespace nebula

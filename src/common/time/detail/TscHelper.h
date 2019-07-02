/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"

#ifndef COMMON_TIME_DETAIL_TSCHELPER_H_
#define COMMON_TIME_DETAIL_TSCHELPER_H_

namespace nebula {
namespace time {

volatile uint64_t readTsc();

extern const std::chrono::steady_clock::time_point kUptime;
extern const uint64_t kFirstTick;
extern const struct timespec kStartTime;

extern volatile std::atomic<double> ticksPerSecFactor;
extern volatile std::atomic<double> ticksPerMSecFactor;
extern volatile std::atomic<double> ticksPerUSecFactor;

}  // namespace time
}  // namespace nebula
#endif  // COMMON_TIME_DETAIL_TSCHELPER_H_


/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gflags/gflags.h>

#include "common/time/TimezoneInfo.h"

DEFINE_string(timezone_file,
              "share/resources/date_time_zonespec.csv",
              "The file path to the timezone file.");

// If it's invalid timezone the service initialize will failed.
// Empty for system default configuration
DEFINE_string(timezone_name,
              "UTC+00:00:00",
              "The timezone used in current system, "
              "only used in nebula datetime compute won't "
              "affect process time (such as log time etc.).");

namespace nebula {
namespace time {

/*static*/ ::boost::local_time::tz_database Timezone::tzdb;

/*static*/ Timezone Timezone::globalTimezone;

/*static*/ Status Timezone::initializeGlobalTimezone() {
    // use system timezone configuration if not set.
    if (FLAGS_timezone_name.empty()) {
        auto *tz = ::getenv("TZ");
        if (tz != nullptr) {
            FLAGS_timezone_name.append(tz);
        }
    }
    if (!FLAGS_timezone_name.empty()) {
        if (FLAGS_timezone_name.front() == ':') {
            NG_RETURN_IF_ERROR(Timezone::init());
            return globalTimezone.loadFromDb(
                std::string(FLAGS_timezone_name.begin() + 1, FLAGS_timezone_name.end()));
        } else {
            return globalTimezone.parsePosixTimezone(FLAGS_timezone_name);
        }
    } else {
        return Status::Error("Don't allowed empty timezone.");
    }
}

}   // namespace time
}   // namespace nebula

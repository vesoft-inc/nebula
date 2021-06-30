/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_TIME_TIMEZONEINFO_H_
#define COMMON_TIME_TIMEZONEINFO_H_

#include <boost/date_time/local_time/local_time.hpp>
#include <gflags/gflags_declare.h>

#include <exception>

#include "common/base/Base.h"
#include "common/base/Status.h"

DECLARE_string(timezone_file);

namespace nebula {
namespace time {

class Timezone {
public:
    Timezone() = default;

    static MUST_USE_RESULT Status init() {
        try {
            tzdb.load_from_file(FLAGS_timezone_file);
        } catch (const std::exception &e) {
            return Status::Error("Invalid timezone file `%s', exception: `%s'.",
                                 FLAGS_timezone_file.c_str(),
                                 e.what());
        }
        return Status::OK();
    }

    MUST_USE_RESULT Status loadFromDb(const std::string &region) {
        zoneInfo_ = tzdb.time_zone_from_region(region);
        if (zoneInfo_ == nullptr) {
            return Status::Error("Not supported timezone `%s'.", region.c_str());
        }
        return Status::OK();
    }

    // see the posix timezone literal format in https://man7.org/linux/man-pages/man3/tzset.3.html
    MUST_USE_RESULT Status parsePosixTimezone(const std::string &posixTimezone) {
        try {
            zoneInfo_.reset(new ::boost::local_time::posix_time_zone(posixTimezone));
        } catch (const std::exception &e) {
            return Status::Error("Malformed timezone format: `%s', exception: `%s'.",
                                 posixTimezone.c_str(),
                                 e.what());
        }
        return Status::OK();
    }

    std::string stdZoneName() const {
        return DCHECK_NOTNULL(zoneInfo_)->std_zone_name();
    }

    // offset in seconds
    int32_t utcOffsetSecs() const {
        return DCHECK_NOTNULL(zoneInfo_)->base_utc_offset().total_seconds();
    }

    // TODO(shylock) Get Timzone info(I.E. GMT offset) directly from IANA tzdb
    // to support non-global timezone configuration
    // See the timezone format from https://man7.org/linux/man-pages/man3/tzset.3.html
    static Status initializeGlobalTimezone();

    static const auto& getGlobalTimezone() {
        return globalTimezone;
    }

private:
    static ::boost::local_time::tz_database tzdb;

    static Timezone globalTimezone;

    ::boost::shared_ptr<::boost::date_time::time_zone_base<::boost::posix_time::ptime, char>>
        zoneInfo_{nullptr};
};

}   // namespace time
}   // namespace nebula

#endif   // COMMON_TIME_TIMEZONEINFO_H_

/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/log/LogMonitor.h"

namespace nebula {

// default min_warn is 256M, disk freebytes < 256M will change LOG_LEVEL to WARNING
DEFINE_uint64(log_min_reserved_bytes_to_warn,
              256 * (1UL << 20),
              "if freebytes in logdir less than this, will change log level to WARN");

// default min_error is 64M, disk freebytes < 64M will change LOG_LEVEL to ERROR
DEFINE_uint64(log_min_reserved_bytes_to_error,
              64 * (1UL << 20),
              "if freebytes in logdir less than this, will change log level to ERROR");

// default min_fatal is 4M, disk freebytes < 4M will change LOG_LEVEL to FATAL
DEFINE_uint64(log_min_reserved_bytes_to_fatal,
              4 * (1UL << 20),
              "if freebytes in logdir less than this, will change log level to FATAL");

// default check log_disk interval is 10s
DEFINE_int32(log_disk_check_interval_secs, 10, "interval to check free space of log path");

void LogMonitor::getLogDiskFreeByte() {
  boost::system::error_code ec;
  auto info = boost::filesystem::space(FLAGS_log_dir, ec);
  if (!ec) {
    freeByte_ = info.available;
  } else {
    LOG(WARNING) << "Get filesystem info of logdir: " << FLAGS_log_dir << " failed";
  }
}

void LogMonitor::checkAndChangeLogLevel() {
  getLogDiskFreeByte();

  if (FLAGS_log_min_reserved_bytes_to_fatal > FLAGS_log_min_reserved_bytes_to_error ||
      FLAGS_log_min_reserved_bytes_to_fatal > FLAGS_log_min_reserved_bytes_to_warn ||
      FLAGS_log_min_reserved_bytes_to_error > FLAGS_log_min_reserved_bytes_to_warn) {
    LOG(ERROR) << "Get Invalid config in LogMonitor, the LogMonitor config should be "
               << "FLAGS_log_min_reserved_bytes_to_warn >"
               << "FLAGS_log_min_reserved_bytes_to_error > FLAGS_log_min_reserved_bytes_to_fatal;";
    return;
  }

  if (freeByte_ < FLAGS_log_min_reserved_bytes_to_fatal) {
    LOG(ERROR) << "log disk freebyte is less than " << FLAGS_log_min_reserved_bytes_to_fatal
               << ", change log level to FATAL";
    FLAGS_minloglevel = google::GLOG_FATAL;
  } else if (freeByte_ < FLAGS_log_min_reserved_bytes_to_error) {
    LOG(ERROR) << "log disk freebyte is less than " << FLAGS_log_min_reserved_bytes_to_error
               << ", change log level to ERROR";
    FLAGS_minloglevel = google::GLOG_ERROR;
  } else if (freeByte_ < FLAGS_log_min_reserved_bytes_to_warn) {
    LOG(ERROR) << "log disk freebyte is less than " << FLAGS_log_min_reserved_bytes_to_warn
               << ", change log level to WARNING";
    FLAGS_minloglevel = google::GLOG_WARNING;
  } else {
    // if freeByte_ is bigger than every min_log_flag, reset the FLAGS_minloglevel to old value
    if (FLAGS_minloglevel != oldMinLogLevel_) {
      FLAGS_minloglevel = oldMinLogLevel_;
    }
  }
}

}  // namespace nebula

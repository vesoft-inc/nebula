/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef COMMON_LOG_LOGMONITOR_H
#define COMMON_LOG_LOGMONITOR_H

#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>

#include "common/thread/GenericWorker.h"

namespace nebula {

DECLARE_uint64(log_min_reserved_bytes_to_warn);
DECLARE_uint64(log_min_reserved_bytes_to_error);
DECLARE_uint64(log_min_reserved_bytes_to_fatal);
DECLARE_int32(log_disk_check_interval_secs);

class LogMonitor {
 public:
  LogMonitor() : oldMinLogLevel_(FLAGS_minloglevel), freeByte_(1UL << 60) {
    worker_ = std::make_shared<thread::GenericWorker>();
    CHECK(worker_->start());
    worker_->addRepeatTask(
        FLAGS_log_disk_check_interval_secs * 1000, &LogMonitor::checkAndChangeLogLevel, this);
  }

  ~LogMonitor() {
    worker_->stop();
    worker_->wait();
  }

  void getLogDiskFreeByte();

  void checkAndChangeLogLevel();

 private:
  int32_t oldMinLogLevel_;
  std::shared_ptr<thread::GenericWorker> worker_;
  std::atomic_uint64_t freeByte_;
};

}  // namespace nebula
#endif

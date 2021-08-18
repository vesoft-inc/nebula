/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <breakpad/client/linux/handler/exception_handler.h>

#include "common/base/Status.h"
#include "common/base/StatusOr.h"
#include "common/fs/FileUtils.h"

DECLARE_string(log_dir);

using nebula::Status;
using nebula::StatusOr;
using nebula::fs::FileUtils;

StatusOr<std::unique_ptr<google_breakpad::ExceptionHandler>> setupBreakpad() {
  if (!FileUtils::exist(FLAGS_log_dir)) {
    return Status::Error("Log directory does not exist:`%s'", FLAGS_log_dir.c_str());
  }
  google_breakpad::MinidumpDescriptor descriptor(FLAGS_log_dir);
  return std::make_unique<google_breakpad::ExceptionHandler>(
      descriptor, nullptr, nullptr, nullptr, true, -1);
}

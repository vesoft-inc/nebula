/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "daemons/SetupLogging.h"

#include <glog/logging.h>

#include <filesystem>
#include <string>

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/fs/FileUtils.h"
DECLARE_string(log_dir);

DEFINE_bool(redirect_stdout, true, "Whether to redirect stdout and stderr to separate files");
DEFINE_string(stdout_log_file, "stdout.log", "Destination filename of stdout");
DEFINE_string(stderr_log_file, "stderr.log", "Destination filename of stderr");

using nebula::Status;
using nebula::fs::FileUtils;

Status setupLogging(const std::string &exe) {
  // If the log directory does not exist, try to create
  auto executable = std::filesystem::path(exe).filename().string();
  if (!FileUtils::exist(FLAGS_log_dir) && !FileUtils::makeDir(FLAGS_log_dir)) {
    return Status::Error("Failed to create log directory `%s'", FLAGS_log_dir.c_str());
  }
  if (!FLAGS_timestamp_in_logfile_name) {
    google::SetLogDestination(google::GLOG_INFO,
                              (FLAGS_log_dir + '/' + executable + ".INFO.impl").c_str());
    google::SetLogDestination(google::GLOG_WARNING,
                              (FLAGS_log_dir + '/' + executable + ".WARNING.impl").c_str());
    google::SetLogDestination(google::GLOG_ERROR,
                              (FLAGS_log_dir + '/' + executable + ".ERROR.impl").c_str());
    google::SetLogDestination(google::GLOG_FATAL,
                              (FLAGS_log_dir + '/' + executable + ".FATAL.impl").c_str());
  }

  if (!FLAGS_redirect_stdout) {
    return Status::OK();
  }

  auto dup = [](const std::string &filename, FILE *stream) -> Status {
    auto path = FLAGS_log_dir + "/" + filename;
    auto fd = ::open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (fd == -1) {
      return Status::Error("Failed to create or open `%s': %s", path.c_str(), ::strerror(errno));
    }
    if (::dup2(fd, ::fileno(stream)) == -1) {
      return Status::Error(
          "Failed to ::dup2 from `%s' to stdout: %s", path.c_str(), ::strerror(errno));
    }
    ::close(fd);
    return Status::OK();
  };

  NG_RETURN_IF_ERROR(dup(FLAGS_stdout_log_file, stdout));
  NG_RETURN_IF_ERROR(dup(FLAGS_stderr_log_file, stderr));
  return Status::OK();
}

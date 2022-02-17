/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/hdfs/HdfsCommandHelper.h"

#include <folly/String.h>  // for stringPrintf

#include <cstdlib>  // for getenv

#include "common/base/Logging.h"          // for LOG, LogMessage, _LOG_INFO
#include "common/base/Status.h"           // for Status
#include "common/process/ProcessUtils.h"  // for ProcessUtils

namespace nebula {
namespace hdfs {

StatusOr<std::string> HdfsCommandHelper::ls(const std::string& hdfsHost,
                                            int32_t hdfsPort,
                                            const std::string& hdfsPath) {
  auto command = folly::stringPrintf(
      "hdfs dfs -ls hdfs://%s:%d%s", hdfsHost.c_str(), hdfsPort, hdfsPath.c_str());
  LOG(INFO) << "Running HDFS Command: " << command;
  auto result = ProcessUtils::runCommand(command.c_str());
  if (result.ok()) {
    return result.value();
  } else {
    return Status::Error(folly::stringPrintf("Failed to run %s", command.c_str()));
  }
}

StatusOr<std::string> HdfsCommandHelper::copyToLocal(const std::string& hdfsHost,
                                                     int32_t hdfsPort,
                                                     const std::string& hdfsPath,
                                                     const std::string& localPath) {
  auto command = folly::stringPrintf("hdfs dfs -copyToLocal hdfs://%s:%d%s %s",
                                     hdfsHost.c_str(),
                                     hdfsPort,
                                     hdfsPath.c_str(),
                                     localPath.c_str());
  LOG(INFO) << "Running HDFS Command: " << command;
  auto result = ProcessUtils::runCommand(command.c_str());
  if (result.ok()) {
    return result.value();
  } else {
    return Status::Error(folly::stringPrintf("Failed to run %s", command.c_str()));
  }
}

bool HdfsCommandHelper::checkHadoopPath() {
  return std::getenv("HADOOP_HOME") != nullptr;
}

}  // namespace hdfs
}  // namespace nebula

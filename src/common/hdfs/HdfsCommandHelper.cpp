/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/hdfs/HdfsCommandHelper.h"

#include <folly/Subprocess.h>
#include <folly/system/Shell.h>

#include "common/process/ProcessUtils.h"

namespace nebula {
namespace hdfs {

Status HdfsCommandHelper::ls(const std::string& hdfsHost,
                             int32_t hdfsPort,
                             const std::string& hdfsPath) {
  auto command = folly::stringPrintf(
      "hdfs dfs -ls hdfs://%s:%d%s", hdfsHost.c_str(), hdfsPort, hdfsPath.c_str());
  LOG(INFO) << "Running HDFS Command: " << command;
  try {
    folly::Subprocess proc(std::vector<std::string>({"/bin/bash", "-c", command}));
    auto result = proc.wait();
    if (!result.exited()) {
      LOG(INFO) << "Failed to ls: " << result.str();
      return Status::Error("Failed to ls hdfs");
    } else if (result.exitStatus() != 0) {
      LOG(INFO) << "Failed to ls: " << result.str();
      return Status::Error("Failed to ls hdfs, errno: %d", result.exitStatus());
    } else {
      return Status::OK();
    }
  } catch (const folly::SubprocessSpawnError& ex) {
    return Status::Error("Exception when ls hdfs: %s", ex.what());
  }
}

Status HdfsCommandHelper::copyToLocal(const std::string& hdfsHost,
                                      int32_t hdfsPort,
                                      const std::string& hdfsPath,
                                      const std::string& localPath) {
  auto command = folly::stringPrintf("hdfs dfs -copyToLocal hdfs://%s:%d%s %s",
                                     hdfsHost.c_str(),
                                     hdfsPort,
                                     hdfsPath.c_str(),
                                     localPath.c_str());
  LOG(INFO) << "Running HDFS Command: " << command;
  try {
    folly::Subprocess proc(std::vector<std::string>({"/bin/bash", "-c", command}));
    auto result = proc.wait();
    if (!result.exited()) {
      LOG(INFO) << "Failed to download: " << result.str();
      return Status::Error("Failed to download from hdfs");
    } else if (result.exitStatus() != 0) {
      LOG(INFO) << "Failed to download: " << result.str();
      return Status::Error("Failed to download from hdfs, errno: %d", result.exitStatus());
    } else {
      return Status::OK();
    }
  } catch (const folly::SubprocessSpawnError& ex) {
    LOG(ERROR) << "Download exception: " << ex.what();
    return Status::Error("Exception when download: %s", ex.what());
  }
}

bool HdfsCommandHelper::checkHadoopPath() {
  return std::getenv("HADOOP_HOME") != nullptr;
}

}  // namespace hdfs
}  // namespace nebula

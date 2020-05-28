/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/hdfs/HdfsCommandHelper.h"
#include "common/process/ProcessUtils.h"

namespace nebula {
namespace hdfs {

StatusOr<std::string> HdfsCommandHelper::ls(const std::string& hdfsHost,
                                            int32_t hdfsPort,
                                            const std::string& hdfsPath) {
    auto command = folly::stringPrintf("hdfs dfs -ls hdfs://%s:%d%s",
                                       hdfsHost.c_str(), hdfsPort, hdfsPath.c_str());
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
                                       hdfsHost.c_str(), hdfsPort, hdfsPath.c_str(),
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

}   // namespace hdfs
}   // namespace nebula

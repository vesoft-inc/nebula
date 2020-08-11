/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "hdfs/HdfsCommandHelper.h"
#include "process/ProcessUtils.h"

namespace nebula {
namespace hdfs {

StatusOr<std::string> HdfsCommandHelper::ls(const std::string& hdfsHost,
                                            int32_t hdfsPort,
                                            const std::string& hdfsPath) {
    auto command = folly::stringPrintf("hadoop fs -ls hdfs://%s:%d%s",
                                       hdfsHost.c_str(), hdfsPort, hdfsPath.c_str());
    LOG(INFO) << "Start Running HDFS Command: [" << command << "]";
    auto resultStatus = ProcessUtils::runCommand(command.c_str());
    if (!resultStatus.ok()) {
        LOG(INFO) << "HDFS Command Failed." << resultStatus.status().toString();
        return resultStatus.status();
    }

    auto result = std::move(resultStatus).value();

    if (folly::StringPiece(result).startsWith("ERR")) {
        LOG(INFO) << "HDFS Command Failed. " << result;
        return Status::Error(result);
    } else {
        LOG(INFO) << "HDFS Command Finished. [" << command << "], output: [" << result << "]";
        return result;
    }
}

StatusOr<std::string> HdfsCommandHelper::copyToLocal(const std::string& hdfsHost,
                                                     int32_t hdfsPort,
                                                     const std::string& hdfsPath,
                                                     const std::string& localPath) {
    auto command = folly::stringPrintf("hadoop fs -copyToLocal hdfs://%s:%d%s %s",
                                       hdfsHost.c_str(), hdfsPort, hdfsPath.c_str(),
                                       localPath.c_str());
    LOG(INFO) << "Start Running HDFS Command: [" << command << "]";
    auto resultStatus = ProcessUtils::runCommand(command.c_str());
    if (!resultStatus.ok()) {
        LOG(INFO) << "HDFS Command Failed. " << resultStatus.status().toString();
        return resultStatus.status();
    }

    auto result = std::move(resultStatus).value();

    if (folly::StringPiece(result).startsWith("ERR")) {
        LOG(INFO) << "HDFS Command Failed. " << result;
        return Status::Error(result);
    } else if (!result.empty()) {
        LOG(INFO) << "HDFS Command Failed. command: ["
                  << command << "], output: [" << result << "]";
        return Status::Error(folly::stringPrintf(
            "HDFS Command Failed. command: [%s], output: [%s]",
            command.c_str(), result.c_str()));
    } else {
        LOG(INFO) << "HDFS Command Finished. [" << command << "], output: [" << result << "]";
        return result;
    }
}

StatusOr<bool> HdfsCommandHelper::exist(const std::string& hdfsHost,
                                        int32_t hdfsPort,
                                        const std::string& hdfsPath) {
    auto path = folly::stringPrintf("hdfs://%s:%d%s", hdfsHost.c_str(), hdfsPort, hdfsPath.c_str());
    auto command = folly::stringPrintf("hadoop fs -test -e %s", path.c_str());
    LOG(INFO) << "Start Running HDFS Command: [" << command << "]";
    auto resultStatus = ProcessUtils::runCommand(command.c_str());
    if (!resultStatus.ok()) {
        LOG(INFO) << "HDFS Command Failed. " << resultStatus.status().toString();
        return resultStatus.status();
    }

    auto result = std::move(resultStatus).value();

    if (folly::StringPiece(result).startsWith("ERR: [1]")) {
        LOG(INFO) << "HDFS Command Failed. " << result;
        return false;
    } else if (!result.empty()) {
        LOG(INFO) << "HDFS Command Failed. command: ["
                  << command << "], output: [" << result << "]";
        return Status::Error(folly::stringPrintf(
            "HDFS Command Failed. command: [%s], output: [%s]",
            command.c_str(), result.c_str()));
    } else {
        LOG(INFO) << "HDFS Command Finished. [" << command << "], output: [" << result << "]";
        return true;
    }
}

bool HdfsCommandHelper::checkHadoopPath() {
    return std::getenv("HADOOP_HOME") != nullptr;
}

}   // namespace hdfs
}   // namespace nebula

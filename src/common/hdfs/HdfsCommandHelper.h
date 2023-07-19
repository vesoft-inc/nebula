/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_HDFSCOMMANDHELPER_H
#define COMMON_HDFSCOMMANDHELPER_H

#include "common/hdfs/HdfsHelper.h"

namespace nebula {
namespace hdfs {

class HdfsCommandHelper : public HdfsHelper {
 public:
  Status ls(const std::string& hdfsHost, int32_t hdfsPort, const std::string& hdfsPath) override;

  Status copyToLocal(const std::string& hdfsHost,
                     int32_t hdfsPort,
                     const std::string& hdfsPath,
                     const std::string& localPath) override;

  bool checkHadoopPath() override;
};

}  // namespace hdfs
}  // namespace nebula

#endif  // COMMON_HDFSCOMMANDHELPER_H

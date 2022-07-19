/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_HDFSHELPER_H
#define COMMON_HDFSHELPER_H

#include "common/base/Base.h"
#include "common/base/StatusOr.h"

namespace nebula {
namespace hdfs {

class HdfsHelper {
 public:
  virtual ~HdfsHelper() = default;

  virtual Status ls(const std::string& hdfsHost, int32_t hdfsPort, const std::string& hdfsPath) = 0;

  virtual Status copyToLocal(const std::string& hdfsHost,
                             int32_t hdfsPort,
                             const std::string& hdfsPath,
                             const std::string& localPath) = 0;

  virtual bool checkHadoopPath() = 0;
};

}  // namespace hdfs
}  // namespace nebula

#endif  // COMMON_HDFSHELPER_H

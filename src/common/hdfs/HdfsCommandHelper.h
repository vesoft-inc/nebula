/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_HDFSCOMMANDHELPER_H
#define COMMON_HDFSCOMMANDHELPER_H

#include "hdfs/HdfsHelper.h"

namespace nebula {
namespace hdfs {

class HdfsCommandHelper : public HdfsHelper {
public:
    StatusOr<std::string> ls(const std::string& hdfsHost,
                             int32_t hdfsPort,
                             const std::string& hdfsPath) override;

    StatusOr<std::string> copyToLocal(const std::string& hdfsHost,
                                      int32_t hdfsPort,
                                      const std::string& hdfsPath,
                                      const std::string& localPath) override;
};

}   // namespace hdfs
}   // namespace nebula

#endif  // COMMON_HDFSCOMMANDHELPER_H

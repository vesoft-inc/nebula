/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TEST_MOCKHDFSHELPER_H_
#define STORAGE_TEST_MOCKHDFSHELPER_H_

#include "common/hdfs/HdfsHelper.h"

namespace nebula {
namespace storage {

class MockHdfsOKHelper : public nebula::hdfs::HdfsHelper {
public:
    StatusOr<std::string> ls(const std::string& hdfsHost,
                             int32_t hdfsPort,
                             const std::string& hdfsPath) override {
        UNUSED(hdfsHost); UNUSED(hdfsPort); UNUSED(hdfsPath);
        sleep(1);
        return Status::OK();
    }

    StatusOr<std::string> copyToLocal(const std::string& hdfsHost,
                                      int32_t hdfsPort,
                                      const std::string& hdfsPath,
                                      const std::string& localPath) override {
        UNUSED(hdfsHost); UNUSED(hdfsPort); UNUSED(hdfsPath); UNUSED(localPath);
        sleep(1);
        return "";
    }

    bool checkHadoopPath() override {
        return true;
    }
};

class MockHdfsExistHelper : public nebula::hdfs::HdfsHelper {
public:
    StatusOr<std::string> ls(const std::string& hdfsHost,
                             int32_t hdfsPort,
                             const std::string& hdfsPath) override {
        UNUSED(hdfsHost); UNUSED(hdfsPort); UNUSED(hdfsPath);
        sleep(1);
        return Status::OK();
    }

    StatusOr<std::string> copyToLocal(const std::string& hdfsHost,
                                      int32_t hdfsPort,
                                      const std::string& hdfsPath,
                                      const std::string& localPath) override {
        UNUSED(hdfsHost); UNUSED(hdfsPort); UNUSED(hdfsPath); UNUSED(localPath);
        sleep(1);
        return "copyToLocal: `/data/': File exists";
    }

    bool checkHadoopPath() override {
        return true;
    }
};

}   // namespace storage
}   // namespace nebula

#endif  // STORAGE_TEST_MOCKHDFSHELPER_H_

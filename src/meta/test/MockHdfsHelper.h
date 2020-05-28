/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_TEST_MOCKHDFSHELPER_H_
#define META_TEST_MOCKHDFSHELPER_H_

#include "common/hdfs/HdfsHelper.h"

namespace nebula {
namespace meta {

class MockHdfsOKHelper : public nebula::hdfs::HdfsHelper {
public:
    StatusOr<std::string> ls(const std::string& hdfsHost,
                             int32_t hdfsPort,
                             const std::string& hdfsPath) override {
        UNUSED(hdfsHost); UNUSED(hdfsPort); UNUSED(hdfsPath);
        sleep(1);
        return "total 2\n0000.sst\n000.sst";
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

class MockHdfsNotExistHelper : public nebula::hdfs::HdfsHelper {
public:
    StatusOr<std::string> ls(const std::string& hdfsHost,
                             int32_t hdfsPort,
                             const std::string& hdfsPath) override {
        UNUSED(hdfsHost); UNUSED(hdfsPort);
        sleep(1);
        return Status::Error(folly::stringPrintf("HDFS Path %s Not Exist", hdfsPath.c_str()));
    }

    StatusOr<std::string> copyToLocal(const std::string& hdfsHost,
                                      int32_t hdfsPort,
                                      const std::string& hdfsPath,
                                      const std::string& localPath) override {
        UNUSED(hdfsHost); UNUSED(hdfsPort); UNUSED(localPath);
        sleep(1);
        return Status::Error(folly::stringPrintf("HDFS Path %s Not Exist", hdfsPath.c_str()));
    }

    bool checkHadoopPath() override {
        return true;
    }
};

}   // namespace meta
}   // namespace nebula

#endif  // META_TEST_MOCKHDFSHELPER_H_

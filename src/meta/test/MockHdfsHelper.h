/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_TEST_MOCKHDFSHELPER_H_
#define META_TEST_MOCKHDFSHELPER_H_

#include "common/hdfs/HdfsHelper.h"

namespace nebula {
namespace meta {

class MockHdfsOKHelper : public nebula::hdfs::HdfsHelper {
 public:
  Status ls(const std::string&, int32_t, const std::string&) override {
    sleep(1);
    return Status::OK();
  }

  Status copyToLocal(const std::string&, int32_t, const std::string&, const std::string&) override {
    sleep(1);
    return Status::OK();
  }

  bool checkHadoopPath() override {
    return true;
  }
};

class MockHdfsNotExistHelper : public nebula::hdfs::HdfsHelper {
 public:
  Status ls(const std::string&, int32_t, const std::string& hdfsPath) override {
    sleep(1);
    return Status::Error(folly::stringPrintf("HDFS Path %s Not Exist", hdfsPath.c_str()));
  }

  Status copyToLocal(const std::string&,
                     int32_t,
                     const std::string& hdfsPath,
                     const std::string&) override {
    sleep(1);
    return Status::Error(folly::stringPrintf("HDFS Path %s Not Exist", hdfsPath.c_str()));
  }

  bool checkHadoopPath() override {
    return true;
  }
};

}  // namespace meta
}  // namespace nebula

#endif  // META_TEST_MOCKHDFSHELPER_H_

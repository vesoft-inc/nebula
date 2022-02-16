/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>  // for init
#include <glog/logging.h>     // for INFO
#include <gtest/gtest.h>      // for TestPartResult

#include <string>  // for string, basic_string, allocator
#include <vector>  // for vector

#include "common/base/Logging.h"       // for SetStderrLogging
#include "common/datatypes/DataSet.h"  // for Row, DataSet
#include "common/datatypes/Value.h"    // for Value

TEST(DataSetTest, Basic) {
  nebula::DataSet data({"col1", "col2", "col3"});
  data.emplace_back(nebula::Row({1, 2, 3}));
  data.emplace_back(nebula::Row({4, 5, 6}));
  data.emplace_back(nebula::Row({7, 8, 9}));

  EXPECT_EQ(std::vector<std::string>({"col1", "col2", "col3"}), data.keys());

  EXPECT_EQ(std::vector<nebula::Value>({4, 5, 6}), data.rowValues(1));

  EXPECT_EQ(std::vector<nebula::Value>({2, 5, 8}), data.colValues("col2"));

  nebula::DataSet data2({"col1", "col2", "col3"});
  for (const auto& it : data) {
    data2.emplace_back(it);
  }
  EXPECT_EQ(data, data2);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}

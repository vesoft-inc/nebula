/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/datatypes/DataSet.h"

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

  nebula::DataSet data3({"col4", "col5"});
  data3.emplace_back(nebula::Row({10, 20}));
  data3.emplace_back(nebula::Row({40, 50}));
  data3.emplace_back(nebula::Row({70, 80}));
  data.merge(std::move(data3));

  nebula::DataSet data4({"col1", "col2", "col3", "col4", "col5"});
  data4.emplace_back(nebula::Row({1, 2, 3, 10, 20}));
  data4.emplace_back(nebula::Row({4, 5, 6, 40, 50}));
  data4.emplace_back(nebula::Row({7, 8, 9, 70, 80}));
  EXPECT_EQ(data, data4);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}

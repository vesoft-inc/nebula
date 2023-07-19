/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/base/Cord.h"

namespace nebula {

TEST(CordTest, multipleBlocks) {
  Cord cord(128);

  std::string buf;
  for (int i = 0; i < 100; i++) {
    buf.append("Hello World!");
  }

  cord.write(buf.data(), buf.size());

  EXPECT_EQ(buf.size(), cord.size());
  EXPECT_EQ(buf.size(), cord.str().size());
  EXPECT_EQ(buf, cord.str());
}

}  // namespace nebula

#include "common/base/test/CordTestT.cpp"

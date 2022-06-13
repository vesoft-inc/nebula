// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include <gtest/gtest.h>

#include <type_traits>

#include "common/base/Arena.h"

namespace nebula {

TEST(ArenaTest, Basic) {
  Arena a;

  for (int i = 1; i < 4096; i += 8) {
    void *ptr = a.allocateAligned(i);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % std::alignment_of<std::max_align_t>::value, 0);
  }
}

TEST(ArenaTest, Construct) {
  Arena a;
  {
    void *ptr = a.allocateAligned(sizeof(std::string));
    auto *obj = new (ptr) std::string("Hello World!");
    EXPECT_EQ(*obj, "Hello World!");
    obj->~basic_string();
  }
  {
    void *ptr = a.allocateAligned(sizeof(int));
    auto *obj = new (ptr) int(3);  // NOLINT
    EXPECT_EQ(*obj, 3);
  }
  {
    for (std::size_t i = 0; i < 1024; ++i) {
      void *ptr = a.allocateAligned(sizeof(int));
      auto *obj = new (ptr) int(i);  // NOLINT
      EXPECT_EQ(*obj, i);
    }
  }
}

}  // namespace nebula

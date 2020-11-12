/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/ObjectPool.h"

#include <gtest/gtest.h>

namespace nebula {

static int instances = 0;

class MyClass {
public:
    MyClass() {
        instances++;
    }
    ~MyClass() {
        instances--;
    }
};

TEST(ObjectPoolTest, TestPooling) {
    ASSERT_EQ(instances, 0);

    ObjectPool pool;
    ASSERT_NE(pool.add(new MyClass), nullptr);
    ASSERT_NE(pool.add(new MyClass), nullptr);
    ASSERT_EQ(instances, 2);

    pool.clear();
    ASSERT_EQ(instances, 0);
}

}   // namespace nebula

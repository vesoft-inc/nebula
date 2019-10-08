/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "base/Base.h"
#include "base/ScopedInvoker.h"

// helper function
static void inc(int* a) {
    ++*a;
}

static void simple() {

}

// helper method
class sample {
public:
    sample(int i) : i_(i) {}

    void inc() {
        ++i_;
    }

    void reset() {
        i_ = 0;
    }

private:
    int i_;

    FRIEND_TEST(ScopedInvokerTest, Simple);
};

TEST(ScopedInvokerTest, Simple) {
    // wrap function
    int a = 0;
    {
        nebula::ScopedInvoker<int*> c(inc, &a);
    }
    ASSERT_EQ(a, 1);

    {
        nebula::ScopedInvoker<> c(simple);
    }

    // wrap method
    sample s(0);
    {
        nebula::ScopedInvoker<sample*> c(&sample::inc, &s);
    }
    ASSERT_EQ(s.i_, 1);
}

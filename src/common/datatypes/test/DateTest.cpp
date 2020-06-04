/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/datatypes/Date.h"

namespace nebula {

TEST(Date, DaysConversion) {
    Date a;
    EXPECT_EQ(11968512, a.toInt());
    Date b(11968512);
    EXPECT_EQ(a, b);

    a.reset(0, 12, 31);
    b.fromInt(a.toInt());
    EXPECT_EQ(a, b);

    a.reset(-1, 2, 28);
    b.fromInt(a.toInt());
    EXPECT_EQ(a, b);

    a.reset(2020, 4, 12);
    b.fromInt(a.toInt());
    EXPECT_EQ(a, b);

    a.reset(2020, 12, 31);
    b.fromInt(a.toInt());
    EXPECT_EQ(a, b);

    a.reset(2020, 1, 1);
    b.fromInt(a.toInt());
    EXPECT_EQ(a, b);

    a.reset(-1024, 11, 30);
    b.fromInt(a.toInt());
    EXPECT_EQ(a, b);

    a.reset(-2020, 12, 31);
    b.fromInt(a.toInt());
    EXPECT_EQ(a, b);

    a.reset(-2020, 1, 1);
    b.fromInt(a.toInt());
    EXPECT_EQ(a, b);
}


TEST(Date, Arithmetics) {
    Date a(2020, 1, 1);
    Date b = a + 365;
    EXPECT_EQ(Date(2020, 12, 31), b);

    a.reset(2020, 1, 1);
    b = a - 1;
    EXPECT_EQ(Date(2019, 12, 31), b);
    b = a - 365;
    EXPECT_EQ(Date(2019, 1, 1), b);
    b = a - 365 - 365;
    EXPECT_EQ(Date(2018, 1, 1), b);
    b = a - 365 - 365 - 365;
    EXPECT_EQ(Date(2017, 1, 1), b);
    b = a - 365 - 365 - 365 - 366;
    EXPECT_EQ(Date(2016, 1, 1), b);
    b = a + 366;
    EXPECT_EQ(Date(2021, 1, 1), b);
    b = a + 366 + 365;
    EXPECT_EQ(Date(2022, 1, 1), b);
    b = a + 366 + 365 + 365;
    EXPECT_EQ(Date(2023, 1, 1), b);
    b = a + 366 + 365 + 365 + 365;
    EXPECT_EQ(Date(2024, 1, 1), b);

    a.reset(2020, 12, 31);
    b = a + 1;
    EXPECT_EQ(Date(2021, 1, 1), b);
    b = a - 366;
    EXPECT_EQ(Date(2019, 12, 31), b);
    b = a - 366 - 365;
    EXPECT_EQ(Date(2018, 12, 31), b);
    b = a - 366 - 365 - 365;
    EXPECT_EQ(Date(2017, 12, 31), b);
    b = a - 366 - 365 - 365 - 365;
    EXPECT_EQ(Date(2016, 12, 31), b);
    b = a + 365;
    EXPECT_EQ(Date(2021, 12, 31), b);
    b = a + 365 + 365;
    EXPECT_EQ(Date(2022, 12, 31), b);
    b = a + 365 + 365 + 365;
    EXPECT_EQ(Date(2023, 12, 31), b);
    b = a + 365 + 365 + 365 + 366;
    EXPECT_EQ(Date(2024, 12, 31), b);

    a.reset(-1024, 1, 1);
    b = a - 1;
    EXPECT_EQ(Date(-1025, 12, 31), b);
    b = a - 365;
    EXPECT_EQ(Date(-1025, 1, 1), b);
    b = a - 365 - 365;
    EXPECT_EQ(Date(-1026, 1, 1), b);
    b = a - 365 - 365 - 365;
    EXPECT_EQ(Date(-1027, 1, 1), b);
    b = a - 365 - 365 - 365 - 366;
    EXPECT_EQ(Date(-1028, 1, 1), b);
    b = a + 366;
    EXPECT_EQ(Date(-1023, 1, 1), b);
    b = a + 366 + 365;
    EXPECT_EQ(Date(-1022, 1, 1), b);
    b = a + 366 + 365 + 365;
    EXPECT_EQ(Date(-1021, 1, 1), b);
    b = a + 366 + 365 + 365 + 365;
    EXPECT_EQ(Date(-1020, 1, 1), b);

    a.reset(-1024, 12, 31);
    b = a + 1;
    EXPECT_EQ(Date(-1023, 1, 1), b);
    b = a - 366;
    EXPECT_EQ(Date(-1025, 12, 31), b);
    b = a - 366 - 365;
    EXPECT_EQ(Date(-1026, 12, 31), b);
    b = a - 366 - 365 - 365;
    EXPECT_EQ(Date(-1027, 12, 31), b);
    b = a - 366 - 365 - 365 - 365;
    EXPECT_EQ(Date(-1028, 12, 31), b);
    b = a + 365;
    EXPECT_EQ(Date(-1023, 12, 31), b);
    b = a + 365 + 365;
    EXPECT_EQ(Date(-1022, 12, 31), b);
    b = a + 365 + 365 + 365;
    EXPECT_EQ(Date(-1021, 12, 31), b);
    b = a + 365 + 365 + 365 + 366;
    EXPECT_EQ(Date(-1020, 12, 31), b);
}

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

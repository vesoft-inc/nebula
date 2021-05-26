/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/FlattenList.h"
#include "gtest/gtest.h"

namespace nebula {

TEST(FlatternListTest, INT) {
    FlattenListWriter w;
    w << 1L;
    w << 2L;
    w << 3L;
    std::string encoded = w.finish();

    FlattenListReader r(encoded);
    auto iter = r.begin();
    auto end = r.end();
    {
        EXPECT_TRUE(iter->ok());
        auto v = iter->value();
        EXPECT_EQ(v.which(), VAR_INT64);
        EXPECT_EQ(boost::get<int64_t>(v), 1L);
    }
    {
        ++iter;
        EXPECT_TRUE(iter->ok());
        auto v = iter->value();
        EXPECT_EQ(v.which(), VAR_INT64);
        EXPECT_EQ(boost::get<int64_t>(v), 2L);
    }
    {
        ++iter;
        EXPECT_TRUE(iter->ok());
        auto v = iter->value();
        EXPECT_EQ(v.which(), VAR_INT64);
        EXPECT_EQ(boost::get<int64_t>(v), 3L);
    }
    {
        ++iter;
        EXPECT_EQ(iter, end);
    }
}

TEST(FlatternListTest, BOOL) {
    FlattenListWriter w;
    w << true;
    w << true;
    w << false;
    std::string encoded = w.finish();

    FlattenListReader r(encoded);
    auto iter = r.begin();
    auto end = r.end();
    {
        EXPECT_TRUE(iter->ok());
        auto v = iter->value();
        EXPECT_EQ(v.which(), VAR_BOOL);
        EXPECT_EQ(boost::get<bool>(v), true);
    }
    {
        ++iter;
        EXPECT_TRUE(iter->ok());
        auto v = iter->value();
        EXPECT_EQ(v.which(), VAR_BOOL);
        EXPECT_EQ(boost::get<bool>(v), true);
    }
    {
        ++iter;
        EXPECT_TRUE(iter->ok());
        auto v = iter->value();
        EXPECT_EQ(v.which(), VAR_BOOL);
        EXPECT_EQ(boost::get<bool>(v), false);
    }
    {
        ++iter;
        EXPECT_EQ(iter, end);
    }
}

TEST(FlatternListTest, DOUBLE) {
    FlattenListWriter w;
    w << 1.0;
    w << 2.0;
    w << 3.0;
    std::string encoded = w.finish();

    FlattenListReader r(encoded);
    auto iter = r.begin();
    auto end = r.end();
    {
        EXPECT_TRUE(iter->ok());
        auto v = iter->value();
        EXPECT_EQ(v.which(), VAR_DOUBLE);
        EXPECT_EQ(boost::get<double>(v), 1.0);
    }
    {
        ++iter;
        EXPECT_TRUE(iter->ok());
        auto v = iter->value();
        EXPECT_EQ(v.which(), VAR_DOUBLE);
        EXPECT_EQ(boost::get<double>(v), 2.0);
    }
    {
        ++iter;
        EXPECT_TRUE(iter->ok());
        auto v = iter->value();
        EXPECT_EQ(v.which(), VAR_DOUBLE);
        EXPECT_EQ(boost::get<double>(v), 3.0);
    }
    {
        ++iter;
        EXPECT_EQ(iter, end);
    }
}

TEST(FlatternListTest, STR) {
    FlattenListWriter w;
    w << "1.0";
    w << "2.00";
    w << "3.000";
    std::string encoded = w.finish();

    FlattenListReader r(encoded);
    auto iter = r.begin();
    auto end = r.end();
    {
        EXPECT_TRUE(iter->ok());
        auto v = iter->value();
        EXPECT_EQ(v.which(), VAR_STR);
        EXPECT_EQ(boost::get<std::string>(v), "1.0");
    }
    {
        ++iter;
        EXPECT_TRUE(iter->ok());
        auto v = iter->value();
        EXPECT_EQ(v.which(), VAR_STR);
        EXPECT_EQ(boost::get<std::string>(v), "2.00");
    }
    {
        ++iter;
        EXPECT_TRUE(iter->ok());
        auto v = iter->value();
        EXPECT_EQ(v.which(), VAR_STR);
        EXPECT_EQ(boost::get<std::string>(v), "3.000");
    }
    {
        ++iter;
        EXPECT_EQ(iter, end);
    }
}

TEST(FlatternListTest, ERR) {
    std::string encoded = "err message............";

    FlattenListReader r(encoded);
    auto iter = r.begin();
    EXPECT_TRUE(!iter->ok());
}

}   // namespace nebula

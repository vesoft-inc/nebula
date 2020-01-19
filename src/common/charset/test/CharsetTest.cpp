/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "charset/Charset.h"

namespace nebula {

TEST(CharsetInfo, isSupportCharset) {
    {
        auto status = CharsetInfo::isSupportCharset(std::string("utf8"));
        ASSERT_TRUE(status.ok()) << status;
    }
    {
        auto status = CharsetInfo::isSupportCharset(std::string("gbk"));
        ASSERT_FALSE(status.ok());
    }
}


TEST(CharsetInfo, isSupportCollate) {
    {
        auto status = CharsetInfo::isSupportCollate(std::string("utf8_bin"));
        ASSERT_TRUE(status.ok()) << status;
    }
    {
        auto status = CharsetInfo::isSupportCollate(std::string("utf8"));
        ASSERT_FALSE(status.ok());
    }
    {
        auto status = CharsetInfo::isSupportCollate(std::string("gbk_bin"));
        ASSERT_FALSE(status.ok());
    }
}


TEST(CharsetInfo, charsetAndCollateMatch) {
    {
        auto status = CharsetInfo::charsetAndCollateMatch(std::string("utf8"),
                                                          std::string("utf8_bin"));
        ASSERT_TRUE(status.ok()) << status;
    }
    {
        auto status = CharsetInfo::charsetAndCollateMatch(std::string("utf8"),
                                                          std::string("utf8_general_ci"));
        ASSERT_FALSE(status.ok());
    }
    {
        auto status = CharsetInfo::charsetAndCollateMatch(std::string("gbk"),
                                                          std::string("utf8_bin"));
        ASSERT_FALSE(status.ok());
    }
}


TEST(CharsetInfo, getDefaultCollationbyCharset) {
    {
        auto result = CharsetInfo::getDefaultCollationbyCharset(std::string("utf8"));
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(std::string("utf8_bin"), result.value());
    }
    {
        auto result = CharsetInfo::getDefaultCollationbyCharset(std::string("utf8mb4"));
        ASSERT_FALSE(result.ok());
    }
    {
        auto result = CharsetInfo::getDefaultCollationbyCharset(std::string("gbk"));
        ASSERT_FALSE(result.ok());
    }
}


TEST(CharsetInfo, getCharsetbyCollation) {
    {
        auto result = CharsetInfo::getCharsetbyCollation(std::string("utf8_bin"));
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(std::string("utf8"), result.value());
    }
    {
        auto result = CharsetInfo::getCharsetbyCollation(std::string("utf8mb4_bin"));
        ASSERT_FALSE(result.ok());
    }
    {
        auto result = CharsetInfo::getCharsetbyCollation(std::string("gbk_bin"));
        ASSERT_FALSE(result.ok());
    }
}

}   // namespace nebula

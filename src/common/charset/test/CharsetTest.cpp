/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/charset/Charset.h"

namespace nebula {

TEST(CharsetInfo, isSupportCharset) {
    auto* charsetInfo = CharsetInfo::instance();
    {
        auto status = charsetInfo->isSupportCharset("utf8");
        ASSERT_TRUE(status.ok()) << status;
    }
    {
        auto status = charsetInfo->isSupportCharset("gbk");
        ASSERT_FALSE(status.ok());
    }
}


TEST(CharsetInfo, isSupportCollate) {
    auto* charsetInfo = CharsetInfo::instance();
    {
        auto status = charsetInfo->isSupportCollate("utf8_bin");
        ASSERT_TRUE(status.ok()) << status;
    }
    {
        auto status = charsetInfo->isSupportCollate("utf8");
        ASSERT_FALSE(status.ok());
    }
    {
        auto status = charsetInfo->isSupportCollate("gbk_bin");
        ASSERT_FALSE(status.ok());
    }
}


TEST(CharsetInfo, charsetAndCollateMatch) {
    auto* charsetInfo = CharsetInfo::instance();
    {
        auto status = charsetInfo->charsetAndCollateMatch("utf8", "utf8_bin");
        ASSERT_TRUE(status.ok()) << status;
    }
    {
        auto status = charsetInfo->charsetAndCollateMatch("utf8", "utf8_general_ci");
        ASSERT_FALSE(status.ok());
    }
    {
        auto status = charsetInfo->charsetAndCollateMatch("gbk", "utf8_bin");
        ASSERT_FALSE(status.ok());
    }
}


TEST(CharsetInfo, getDefaultCollationbyCharset) {
    auto* charsetInfo = CharsetInfo::instance();
    {
        auto result = charsetInfo->getDefaultCollationbyCharset("utf8");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ("utf8_bin", result.value());
    }
    {
        auto result = charsetInfo->getDefaultCollationbyCharset("utf8mb4");
        ASSERT_FALSE(result.ok());
    }
    {
        auto result = charsetInfo->getDefaultCollationbyCharset("gbk");
        ASSERT_FALSE(result.ok());
    }
}


TEST(CharsetInfo, getCharsetbyCollation) {
    auto* charsetInfo = CharsetInfo::instance();
    {
        auto result = charsetInfo->getCharsetbyCollation("utf8_bin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ("utf8", result.value());
    }
    {
        auto result = charsetInfo->getCharsetbyCollation("utf8mb4_bin");
        ASSERT_FALSE(result.ok());
    }
    {
        auto result = charsetInfo->getCharsetbyCollation("gbk_bin");
        ASSERT_FALSE(result.ok());
    }
}


TEST(CharsetInfo, getCharsetDesc) {
    auto* charsetInfo = CharsetInfo::instance();
    auto result = charsetInfo->getCharsetDesc();
    EXPECT_EQ(1, result.size());
}
}   // namespace nebula

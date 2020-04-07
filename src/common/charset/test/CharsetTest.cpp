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
    auto* charsetInfo = CharsetInfo::instance();
    {
        auto status = charsetInfo->isSupportCharset("utf8");
        ASSERT_TRUE(status.ok()) << status;
    }
    {
        auto status = charsetInfo->isSupportCharset("utf8mb4");
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
        auto status = charsetInfo->isSupportCollate("utf8_general_ci");
        ASSERT_TRUE(status.ok()) << status;
    }
    {
        auto status = charsetInfo->isSupportCollate("utf8mb4_bin");
        ASSERT_TRUE(status.ok()) << status;
    }
    {
        auto status = charsetInfo->isSupportCollate("utf8mb4_general_ci");
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
        ASSERT_TRUE(status.ok());
    }
    {
        auto status = charsetInfo->charsetAndCollateMatch("utf8mb4", "utf8mb4_bin");
        ASSERT_TRUE(status.ok()) << status;
    }
    {
        auto status = charsetInfo->charsetAndCollateMatch("utf8mb4", "utf8mb4_general_ci");
        ASSERT_TRUE(status.ok());
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
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ("utf8mb4_bin", result.value());
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
        auto result = charsetInfo->getCharsetbyCollation("utf8_general_ci");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ("utf8", result.value());
    }
    {
        auto result = charsetInfo->getCharsetbyCollation("utf8mb4_bin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ("utf8mb4", result.value());
    }
    {
        auto result = charsetInfo->getCharsetbyCollation("utf8mb4_general_ci");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ("utf8mb4", result.value());
    }
    {
        auto result = charsetInfo->getCharsetbyCollation("gbk_bin");
        ASSERT_FALSE(result.ok());
    }
}


TEST(CharsetInfo, getCharsetDesc) {
    auto* charsetInfo = CharsetInfo::instance();
    auto result = charsetInfo->getCharsetDesc();
    EXPECT_EQ(2, result.size());
}


TEST(CharsetInfo, getUtf8Charlength) {
    auto* charsetInfo = CharsetInfo::instance();
    {
        auto result = charsetInfo->getUtf8Charlength("China");
        EXPECT_EQ(5, result);
    }
    {
        auto result = charsetInfo->getUtf8Charlength("123_ac");
        EXPECT_EQ(6, result);
    }
    {
        auto result = charsetInfo->getUtf8Charlength("中国北京");
        EXPECT_EQ(4, result);
    }
}


TEST(CharsetInfo, nebulaStrCmp) {
    auto* charsetInfo = CharsetInfo::instance();
    {
        auto result = charsetInfo->nebulaStrCmp("utf8_bin", "123456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(-1, result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmp("utf8_bin", "23456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(1, result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmp("utf8_bin", "beijing", "tianjin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(-1, result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmp("utf8_bin", "beijing", "beijinghaidian");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(-1, result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmp("utf8_bin", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(-1, result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmp("utf8_bin", "北京", "天津");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(-1, result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmp("utf8_general_ci", "123456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(-1, result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmp("utf8_general_ci", "23456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(1, result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmp("utf8_general_ci", "beijing", "tianjin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(-1, result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmp("utf8_general_ci", "beijing", "beijinghaidian");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(-1, result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmp("utf8_general_ci", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(0, result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmp("utf8_general_ci", "北京", "天津");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(-1, result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmp("utf8mb4_bin", "😃", "中");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(1, result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmp("utf8mb4_bin", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(-1, result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmp("utf8mb4_general_ci", "😜", "中");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(1, result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmp("utf8mb4_general_ci", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(0, result.value());
    }
}


TEST(CharsetInfo, nebulaStrCmpLT) {
    auto* charsetInfo = CharsetInfo::instance();
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8_bin", "123456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8_bin", "23456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8_bin", "beijing", "tianjin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8_bin", "beijing", "beijinghaidian");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8_bin", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8_bin", "北京", "天津");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8_general_ci", "123456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8_general_ci", "23456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8_general_ci", "beijing", "tianjin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8_general_ci", "beijing", "beijinghaidian");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8_general_ci", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8_general_ci", "北京", "天津");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8mb4_bin", "😜", "中");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8mb4_bin", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8mb4_general_ci", "😜", "中");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLT("utf8mb4_general_ci", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
}

TEST(CharsetInfo, nebulaStrCmpLE) {
    auto* charsetInfo = CharsetInfo::instance();
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8_bin", "123456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8_bin", "23456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8_bin", "beijing", "tianjin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8_bin", "beijing", "beijinghaidian");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8_bin", "China", "chine");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8_bin", "北京", "天津");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8_bin", "北京", "北京");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8_general_ci", "123456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8_general_ci", "23456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8_general_ci", "beijing", "tianjin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8_general_ci", "beijing", "beijinghaidian");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8_general_ci", "China", "chine");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8_general_ci", "北京", "天津");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8_general_ci", "北京", "北京");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8mb4_bin", "😜", "中");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8mb4_bin", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8mb4_general_ci", "😜", "中");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpLE("utf8mb4_general_ci", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
}


TEST(CharsetInfo, nebulaStrCmpGT) {
    auto* charsetInfo = CharsetInfo::instance();
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8_bin", "123456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8_bin", "beijing", "tianjin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8_bin", "beijing", "beijinghaidian");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8_bin", "China", "chine");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8_bin", "北京", "天津");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8_bin", "北京", "北京");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8_general_ci", "123456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8_general_ci", "beijing", "tianjin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8_general_ci", "beijing", "beijinghaidian");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8_general_ci", "China", "chine");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8_general_ci", "北京", "天津");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8_general_ci", "北京", "北京");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8mb4_bin", "😜", "中");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8mb4_bin", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8mb4_general_ci", "😜", "中");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGT("utf8mb4_general_ci", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
}


TEST(CharsetInfo, nebulaStrCmpGE) {
    auto* charsetInfo = CharsetInfo::instance();
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8_bin", "123456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8_bin", "beijing", "tianjin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8_bin", "beijing", "beijinghaidian");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8_bin", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8_bin", "北京", "天津");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8_bin", "北京", "北京");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8_general_ci", "123456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8_general_ci", "beijing", "tianjin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8_general_ci", "beijing", "beijinghaidian");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8_general_ci", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8_general_ci", "北京", "天津");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8_general_ci", "北京", "北京");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8mb4_bin", "😜", "中");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8mb4_bin", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8mb4_general_ci", "😜", "中");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpGE("utf8mb4_general_ci", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
}


TEST(CharsetInfo, nebulaStrCmpEQ) {
    auto* charsetInfo = CharsetInfo::instance();
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8_bin", "123456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8_bin", "beijing", "tianjin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8_bin", "beijing", "beijinghaidian");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8_bin", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8_bin", "北京", "天津");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8_bin", "北京", "北京");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8_general_ci", "123456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8_general_ci", "beijing", "tianjin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8_general_ci", "beijing", "beijinghaidian");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8_general_ci", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8_general_ci", "北京", "天津");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8_general_ci", "北京", "北京");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8mb4_bin", "😜", "中");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8mb4_bin", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8mb4_general_ci", "😜", "中");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpEQ("utf8mb4_general_ci", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
}


TEST(CharsetInfo, nebulaStrCmpNE) {
    auto* charsetInfo = CharsetInfo::instance();
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8_bin", "123456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8_bin", "beijing", "tianjin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8_bin", "beijing", "beijinghaidian");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8_bin", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8_bin", "北京", "天津");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8_bin", "北京", "北京");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8_general_ci", "123456", "21345");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8_general_ci", "beijing", "tianjin");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8_general_ci", "beijing", "beijinghaidian");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8_general_ci", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8_general_ci", "北京", "天津");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8_general_ci", "北京", "北京");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8mb4_bin", "😜", "中");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8mb4_bin", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8mb4_general_ci", "😜", "中");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result.value());
    }
    {
        auto result = charsetInfo->nebulaStrCmpNE("utf8mb4_general_ci", "China", "china");
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result.value());
    }
}

}   // namespace nebula

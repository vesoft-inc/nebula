/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "charset/UniUtf8.h"

namespace nebula {

TEST(UniUtf8Test, utf8BinCmp) {
    {
        std::string str1 = "beijing";
        std::string str2 = "Tianjin";
        auto len1 = str1.length();
        auto len2 = str2.length();

        const unsigned char* s1 = reinterpret_cast<const unsigned char*>(str1.c_str());
        const unsigned char* s2 = reinterpret_cast<const unsigned char*>(str2.c_str());

        const unsigned char* s1end = s1 + len1;
        const unsigned char* s2end = s2 + len2;

        auto ret  = UniUtf8::utf8BinCmp(s1, s1end, s2, s2end);
        ASSERT_GT(ret, 0);
    }
}


TEST(UniUtf8Test, utf8GeneralCICmp) {
    {
        std::string str1 = "beijing";
        std::string str2 = "Tianjin";

        auto ret  = UniUtf8::utf8GeneralCICmp(str1, str2);
        ASSERT_LT(ret, 0);
    }
}


TEST(UniUtf8Test, utf8ToUni) {
    {
        std::string p = "中";
        uint64_t suni = 0;
        auto slen = p.size();
        const unsigned char* s = reinterpret_cast<const unsigned char*>(p.c_str());
        const unsigned char* send = s + slen;

        auto ret = UniUtf8::utf8ToUni(&suni, s, send);
        ASSERT_EQ(ret, 3);
    }
    {
        std::string p = "a";
        uint64_t suni = 0;
        auto slen = p.size();
        const unsigned char* s = reinterpret_cast<const unsigned char*>(p.c_str());
        const unsigned char* send = s + slen;

        auto ret = UniUtf8::utf8ToUni(&suni, s, send);
        ASSERT_EQ(ret, 1);
    }
}

TEST(UniUtf8Test, toSortUnicode) {
    {
        std::string p = "中";
        uint64_t suni = 0;
        auto slen = p.size();
        const unsigned char* s = reinterpret_cast<const unsigned char*>(p.c_str());
        const unsigned char* send = s + slen;

        auto ret = UniUtf8::utf8ToUni(&suni, s, send);
        ASSERT_EQ(ret, 3);
        ASSERT_EQ(suni, 0x4E2D);

        UniUtf8::toSortUnicode(&suni);
        ASSERT_EQ(suni, 0x4E2D);
    }
}

}   // namespace nebula

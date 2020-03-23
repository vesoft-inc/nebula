/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "encryption/Base64.h"

namespace nebula {
namespace encryption {

TEST(Base64Test, encodeDecodeTest) {
    {
        std::string str1 = "Helloword";
        auto encodeRet = Base64::encode(str1);
        std::string expect = "SGVsbG93b3Jk";
        EXPECT_EQ(expect, encodeRet);

        auto decodeRet = Base64::decode(encodeRet);
        EXPECT_EQ(str1, decodeRet);
    }
    {
        std::string str1 = "Hellowor";
        auto encodeRet = Base64::encode(str1);
        std::string expect = "SGVsbG93b3I=";
        EXPECT_EQ(expect, encodeRet);

        auto decodeRet = Base64::decode(encodeRet);
        EXPECT_EQ(str1, decodeRet);
    }
    {
        std::string str1 = "Hellowo";
        auto encodeRet = Base64::encode(str1);
        std::string expect = "SGVsbG93bw==";
        EXPECT_EQ(expect, encodeRet);

        auto decodeRet = Base64::decode(encodeRet);
        EXPECT_EQ(str1, decodeRet);
    }
    {
        std::string str1 = "Hellow)";
        auto encodeRet = Base64::encode(str1);
        std::string expect = "SGVsbG93KQ==";
        EXPECT_EQ(expect, encodeRet);

        auto decodeRet = Base64::decode(encodeRet);
        EXPECT_EQ(str1, decodeRet);
    }
}
}   // namespace encryption
}   // namespace nebula

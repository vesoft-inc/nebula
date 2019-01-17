/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "meta/MetaUtils.h"


namespace nebula {
namespace meta {

TEST(MetaUtilsTest, PathValidTest) {
    ASSERT_TRUE(MetaUtils::pathValid("/"));
    ASSERT_TRUE(MetaUtils::pathValid("/abc"));
    ASSERT_TRUE(MetaUtils::pathValid("/a09_/325AZD"));
    ASSERT_TRUE(MetaUtils::pathValid("/abc/abc/"));
    ASSERT_FALSE(MetaUtils::pathValid("abc/aaa"));
    ASSERT_FALSE(MetaUtils::pathValid("/a^xt"));

    std::string longPath(300, 'a');
    ASSERT_FALSE(MetaUtils::pathValid(folly::stringPrintf("/%s", longPath.c_str())));
}

TEST(MetaUtilsTest, NormalizeTest) {
    ASSERT_EQ("/", MetaUtils::normalize("/"));
    ASSERT_EQ("/abc", MetaUtils::normalize("/abc"));
    ASSERT_EQ("/abc", MetaUtils::normalize("/abc/"));
    ASSERT_EQ("/abc", MetaUtils::normalize("//abc"));
    ASSERT_EQ("/abc", MetaUtils::normalize("//abc/"));
    ASSERT_EQ("/abc", MetaUtils::normalize("////abc///"));
    ASSERT_EQ("/", MetaUtils::normalize("//////"));
}

TEST(MetaUtilsTest, MetaKeyTest) {
    {
        auto key = MetaUtils::metaKey(1, "/abc");
        ASSERT_EQ(1, *reinterpret_cast<const uint8_t*>(key.c_str()));
        ASSERT_EQ("/abc", key.substr(sizeof(uint8_t)));
    }
    {
        auto key = MetaUtils::metaKey(0, "/");
        ASSERT_EQ(0, *reinterpret_cast<const uint8_t*>(key.c_str()));
        ASSERT_EQ("/", key.substr(sizeof(uint8_t)));
    }
    {
        auto key = MetaUtils::metaKey(2, "/abc/cde");
        ASSERT_EQ(2, *reinterpret_cast<const uint8_t*>(key.c_str()));
        ASSERT_EQ("/abc/cde", key.substr(sizeof(uint8_t)));
    }
}

TEST(MetaUtilsTest, LayerTest) {
    ASSERT_EQ(0, MetaUtils::layer("/"));
    ASSERT_EQ(1, MetaUtils::layer("/abc"));
    ASSERT_EQ(2, MetaUtils::layer("/abc/abc"));
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}



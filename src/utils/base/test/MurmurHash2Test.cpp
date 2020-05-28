/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/base/MurmurHash2.h"

namespace nebula {

TEST(MurmurHash2, Basic) {
    MurmurHash2 hash;
    // string
    {
#define LITERAL "Another one bites the dust"
        const char *cstr = LITERAL;
        std::string str = cstr;
        auto hv1 = hash(LITERAL);
        auto hv2 = hash(cstr);
        auto hv3 = hash(str);
        ASSERT_EQ(hv1, hv2);
        ASSERT_EQ(hv2, hv3);
        ASSERT_EQ(hv3, std::hash<std::string>()(str));
    }
    // integer
    {
        bool rand8 = folly::Random::rand64();
        unsigned char rand8_2 = folly::Random::rand64();
        int16_t rand16 = folly::Random::rand64();
        int32_t rand32 = folly::Random::rand64();
        int64_t rand64 = folly::Random::rand64();
        ASSERT_EQ(static_cast<size_t>(rand8), hash(rand8));
        ASSERT_EQ(static_cast<size_t>(rand8_2), hash(rand8_2));
        ASSERT_EQ(static_cast<size_t>(rand16), hash(rand16));
        ASSERT_EQ(static_cast<size_t>(rand32), hash(rand32));
        ASSERT_EQ(static_cast<size_t>(rand64), hash(rand64));
    }
    // pointer
    {
        {
            auto *ptr = new MurmurHash2();
            ASSERT_EQ(reinterpret_cast<size_t>(ptr), hash(ptr));
            delete ptr;
        }
        {
            auto *ptr = new std::string();
            ASSERT_EQ(reinterpret_cast<size_t>(ptr), hash(ptr));
            delete ptr;
        }
        {
            auto *ptr = new int();
            ASSERT_EQ(reinterpret_cast<size_t>(ptr), hash(ptr));
            delete ptr;
        }
    }
    // shared_ptr
    {
        {
            auto ptr = std::make_shared<MurmurHash2>();
            ASSERT_EQ(reinterpret_cast<size_t>(ptr.get()), hash(ptr));
        }
        {
            auto ptr = std::make_shared<std::string>();
            ASSERT_EQ(reinterpret_cast<size_t>(ptr.get()), hash(ptr));
        }
        {
            auto ptr = std::make_shared<int>();
            ASSERT_EQ(reinterpret_cast<size_t>(ptr.get()), hash(ptr));
        }
    }
    // unique_ptr
    {
        {
            auto ptr = std::make_unique<MurmurHash2>();
            ASSERT_EQ(reinterpret_cast<size_t>(ptr.get()), hash(ptr));
        }
        {
            auto ptr = std::make_unique<std::string>();
            ASSERT_EQ(reinterpret_cast<size_t>(ptr.get()), hash(ptr));
        }
        {
            auto ptr = std::make_unique<int>();
            ASSERT_EQ(reinterpret_cast<size_t>(ptr.get()), hash(ptr));
        }
    }
    // std::thread::id
    {
        auto id = std::this_thread::get_id();
        ASSERT_EQ(std::hash<std::thread::id>()(id), hash(id));
    }
}

}   // namespace nebula

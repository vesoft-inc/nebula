/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/base/Base.h"

// The testing template for Cord/ICord together

namespace nebula {

TEST(CordTest, empty) {
    Cord cord;
    std::string a;
    std::string b;
    EXPECT_TRUE(cord.empty());
    EXPECT_EQ(0, cord.size());
    EXPECT_EQ(a, cord.str());
    cord.appendTo(b);
    EXPECT_EQ(a, b);
    EXPECT_TRUE(
        cord.applyTo([](const char* s, int32_t len) -> bool { return s == nullptr && len == 0; }));
    cord.clear();
}

TEST(CordTest, write) {
    Cord cord;

    cord.write("cord", 4).write("-", 1).write("test", 4);

    EXPECT_EQ(9, cord.size());
    EXPECT_EQ(9, cord.str().size());
    EXPECT_EQ("cord-test", cord.str());

    int64_t iVal = 1234567890L;
    cord.write(reinterpret_cast<char*>(&iVal), sizeof(int64_t));

    EXPECT_EQ(9 + sizeof(int64_t), cord.size());
    EXPECT_EQ(9 + sizeof(int64_t), cord.str().size());
    iVal = 0;
    memcpy(reinterpret_cast<char*>(&iVal), cord.str().data() + 9, sizeof(int64_t));
    EXPECT_EQ(1234567890L, iVal);
}

TEST(CordTest, byteStream) {
    Cord cord1;

    cord1 << static_cast<int8_t>('A') << static_cast<uint8_t>('b') << 'C' << true;

    EXPECT_EQ(4, cord1.size());
    EXPECT_EQ(4, cord1.str().size());
    EXPECT_EQ("AbC", cord1.str().substr(0, 3));

    bool bVal = false;
    memcpy(reinterpret_cast<char*>(&bVal), cord1.str().data() + 3, sizeof(bool));
    EXPECT_EQ(true, bVal);

    uint8_t bytes[] = {0x00,
                       0x11,
                       0x22,
                       0x33,
                       0x44,
                       0x55,
                       0x66,
                       0x77,
                       0x88,
                       0x99,
                       0xAA,
                       0xBB,
                       0xCC,
                       0xDD,
                       0xEE,
                       0xFF};
    Cord cord2;

    cord2.write(reinterpret_cast<const char*>(&bytes[0]), sizeof(bytes));
    std::string str = cord2.str();

    EXPECT_EQ(sizeof(bytes), str.size());
    const char* p = str.data();
    for (auto i = 0UL; i < str.size(); i++) {
        EXPECT_EQ(bytes[i], uint8_t(p[i]));
    }
}

TEST(CordTest, integerStream) {
    Cord cord;

    cord << static_cast<int16_t>(16) << static_cast<uint16_t>(0x8080) << static_cast<int32_t>(32)
         << static_cast<uint32_t>(0xFF00FF00) << static_cast<int64_t>(64)
         << static_cast<uint64_t>(0xFF11FF22FF33FF44UL);

    EXPECT_EQ(sizeof(int16_t) + sizeof(uint16_t) + sizeof(int32_t) + sizeof(uint32_t) +
                  sizeof(int64_t) + sizeof(uint64_t),
              cord.size());
    EXPECT_EQ(sizeof(int16_t) + sizeof(uint16_t) + sizeof(int32_t) + sizeof(uint32_t) +
                  sizeof(int64_t) + sizeof(uint64_t),
              cord.str().size());

    int16_t sVal;
    uint16_t usVal;
    int32_t iVal;
    uint32_t uiVal;
    int64_t lVal;
    uint64_t ulVal;

    std::string str = cord.str();
    memcpy(reinterpret_cast<char*>(&sVal), str.data(), sizeof(int16_t));
    memcpy(reinterpret_cast<char*>(&usVal), str.data() + sizeof(int16_t), sizeof(uint16_t));
    memcpy(reinterpret_cast<char*>(&iVal),
           str.data() + sizeof(int16_t) + sizeof(uint16_t),
           sizeof(int32_t));
    memcpy(reinterpret_cast<char*>(&uiVal),
           str.data() + sizeof(int16_t) + sizeof(uint16_t) + sizeof(int32_t),
           sizeof(uint32_t));
    memcpy(reinterpret_cast<char*>(&lVal),
           str.data() + sizeof(int16_t) + sizeof(uint16_t) + sizeof(int32_t) + sizeof(uint32_t),
           sizeof(int64_t));
    memcpy(reinterpret_cast<char*>(&ulVal),
           str.data() + sizeof(int16_t) + sizeof(uint16_t) + sizeof(int32_t) + sizeof(uint32_t) +
               sizeof(int64_t),
           sizeof(uint64_t));

    EXPECT_EQ(16, sVal);
    EXPECT_EQ(0x8080, usVal);
    EXPECT_EQ(32, iVal);
    EXPECT_EQ(0xFF00FF00, uiVal);
    EXPECT_EQ(64, lVal);
    EXPECT_EQ(0xFF11FF22FF33FF44UL, ulVal);
}

TEST(CordTest, floatStream) {
    Cord cord;

    cord << static_cast<float>(1.234) << static_cast<double>(9.876);

    EXPECT_EQ(sizeof(float) + sizeof(double), cord.size());
    EXPECT_EQ(sizeof(float) + sizeof(double), cord.str().size());

    float fVal;
    double dVal;

    std::string str = cord.str();
    memcpy(reinterpret_cast<char*>(&fVal), str.data(), sizeof(float));
    memcpy(reinterpret_cast<char*>(&dVal), str.data() + sizeof(float), sizeof(double));

    EXPECT_FLOAT_EQ(1.234, fVal);
    EXPECT_DOUBLE_EQ(9.876, dVal);
}

TEST(CordTest, stringStream) {
    std::string str1("Hello");
    char str2[] = "Beautiful";
    std::string str3("World");

    Cord cord;

    cord << str1 << str2;
    cord.write(str3.data(), str3.size());

    EXPECT_EQ(str1.size() + strlen(str2) + str3.size(), cord.size());
    EXPECT_EQ(str1.size() + strlen(str2) + str3.size(), cord.str().size());
    EXPECT_EQ(str1 + str2 + str3, cord.str());
}

TEST(CordTest, cordStream) {
    Cord c1;
    Cord c2;

    std::string str1("Hello world!");
    std::string str2("Welcome to the future!");

    c2 << str2;
    c1 << str1 << c2;

    EXPECT_EQ(str1.size() + str2.size(), c1.size());
    EXPECT_EQ(str1 + str2, c1.str());
}

}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

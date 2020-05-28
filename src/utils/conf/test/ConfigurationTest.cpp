/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/conf/Configuration.h"
#include "common/fs/TempFile.h"

using nebula::fs::TempFile;

namespace nebula {
namespace conf {

TEST(Configuration, Basic) {
    std::string json = "{"
                            "\"int\": 123, "
                            "\"double\": 3.14, "
                            "\"bool\": true, "
                            "\"string\": \"string\""
                       "}";
    Configuration conf;
    auto status = conf.parseFromString(json);
    ASSERT_TRUE(status.ok()) << status.toString();

    {
        int64_t val;
        status = conf.fetchAsInt("int", val);
        ASSERT_TRUE(status.ok()) << status.toString();
        ASSERT_EQ(123, val);
    }
    {
        double val;
        status = conf.fetchAsDouble("double", val);
        ASSERT_TRUE(status.ok()) << status.toString();
        ASSERT_DOUBLE_EQ(3.14, val);
    }
    {
        bool val;
        status = conf.fetchAsBool("bool", val);
        ASSERT_TRUE(status.ok()) << status.toString();
        ASSERT_TRUE(val);
    }
    {
        std::string val;
        status = conf.fetchAsString("string", val);
        ASSERT_TRUE(status.ok()) << status.toString();
        ASSERT_EQ("string", val);
    }

    {
        double val;
        status = conf.fetchAsDouble("int", val);
        ASSERT_FALSE(status.ok()) << status.toString();
    }
    {
        int64_t val;
        status = conf.fetchAsInt("string", val);
        ASSERT_FALSE(status.ok()) << status.toString();
    }
    {
        bool val;
        status = conf.fetchAsBool("nonexist", val);
        ASSERT_FALSE(status.ok()) << status.toString();
    }
}


TEST(Configuration, IllegalFormat) {
    Configuration conf;
    {
        std::string empty;
        auto status = conf.parseFromString(empty);
        ASSERT_FALSE(status.ok());
    }
    {
        std::string ill = "\"key\": \"value\"";
        auto status = conf.parseFromString(ill);
        ASSERT_FALSE(status.ok());
    }
    {
        std::string ill = "{\"key\" \"value\"}";
        auto status = conf.parseFromString(ill);
        ASSERT_FALSE(status.ok());
    }
    {
        std::string ill = "{\"key\": \"value\"";
        auto status = conf.parseFromString(ill);
        ASSERT_FALSE(status.ok());
    }
}


TEST(Configuration, SubConf) {
    std::string json = "{"
                            "\"notsub\": 123,"
                            "\"subconf\": {"
                                "\"key\": \"value\""
                            "}"
                       "}";
    Configuration conf;
    auto status = conf.parseFromString(json);
    ASSERT_TRUE(status.ok()) << status.toString();

    Configuration subconf;
    status = conf.fetchAsSubConf("subconf", subconf);
    ASSERT_TRUE(status.ok()) << status.toString();

    std::string val;
    status = subconf.fetchAsString("key", val);
    ASSERT_TRUE(status.ok()) << status.toString();
    ASSERT_EQ("value", val);

    status = conf.fetchAsSubConf("notsub", subconf);
    ASSERT_FALSE(status.ok()) << status.toString();
}


TEST(Configuration, ParseFromFile) {
    TempFile tmpfile("/tmp/configuration_test.json.XXXXXX");
    FILE *file = ::fopen(tmpfile.path(), "w");
    fprintf(file, "// This a C++ style comment line\n");
    fprintf(file, "# This a shell style comment line\n");
    fprintf(file, "{ \n");
    fprintf(file, "\t\"int\": 123, // this is a integer\n");
    fprintf(file, "\t\"double\": 3.14 # this is a double\n");
    fprintf(file, "} \n");
    ::fflush(file);
    ::fclose(file);

    Configuration conf;
    auto status = conf.parseFromFile(tmpfile.path());
    ASSERT_TRUE(status.ok()) << status.toString();
    {
        int64_t val;
        status = conf.fetchAsInt("int", val);
        ASSERT_TRUE(status.ok()) << status.toString();
        ASSERT_EQ(123, val);
    }
    {
        double val;
        status = conf.fetchAsDouble("double", val);
        ASSERT_TRUE(status.ok()) << status.toString();
        ASSERT_DOUBLE_EQ(3.14, val);
    }
}


TEST(Configuration, GetArray) {
    std::string json =
        "{"
        "   \"int_array\" : [1, \"2\", 3, \"4\", 5],"
        "   \"string_array\" : [\"Hello\", \"World\"],"
        "   \"bool_array\" : [0, 1],"
        "   \"double_array\" : [1.2, \"2.3\", 3.4]"
        "}";

    Configuration conf;
    auto status = conf.parseFromString(json);
    ASSERT_TRUE(status.ok()) << status.toString();

    std::vector<int64_t> intArray;
    ASSERT_TRUE(conf.fetchAsIntArray("int_array", intArray).ok());
    ASSERT_EQ(5L, intArray.size());
    for (int64_t i = 1; i<= 5; i++) {
        EXPECT_EQ(i, intArray[i - 1]);
    }

    std::vector<std::string> strArray;
    ASSERT_TRUE(conf.fetchAsStringArray("string_array", strArray).ok());
    ASSERT_EQ(2L, strArray.size());
    EXPECT_EQ("Hello", strArray[0]);
    EXPECT_EQ("World", strArray[1]);

    std::vector<bool> boolArray;
    ASSERT_TRUE(conf.fetchAsBoolArray("bool_array", boolArray).ok());
    ASSERT_EQ(2L, boolArray.size());
    EXPECT_FALSE(boolArray[0]);
    EXPECT_TRUE(boolArray[1]);

    std::vector<double> dblArray;
    ASSERT_TRUE(conf.fetchAsDoubleArray("double_array", dblArray).ok());
    ASSERT_EQ(3L, dblArray.size());
    EXPECT_DOUBLE_EQ(1.2, dblArray[0]);
    EXPECT_DOUBLE_EQ(2.3, dblArray[1]);
    EXPECT_DOUBLE_EQ(3.4, dblArray[2]);
}


TEST(Configuration, Iterator) {
    std::string json =
        "{"
        "   \"int_array\" : [1, \"2\", 3, \"4\", 5],"
        "   \"string_array\" : [\"Hello\", \"World\"],"
        "   \"int_val\" : 100,"
        "   \"double_val\" : 2.3"
        "}";

    Configuration conf;
    auto status = conf.parseFromString(json);
    ASSERT_TRUE(status.ok()) << status.toString();

    static std::unordered_set<std::string> keys{"int_array",
                                                "string_array",
                                                "int_val",
                                                "double_val"};

    int32_t idx = 0;
    conf.forEachKey([&idx] (const std::string& key) {
        EXPECT_TRUE(keys.find(key) != keys.end());
        idx++;
    });
    EXPECT_EQ(keys.size(), idx);

    idx = 0;
    conf.forEachItem([&idx] (const std::string& key, const folly::dynamic& val) {
        if (key == "int_array") {
            ASSERT_TRUE(val.isArray());
            EXPECT_EQ(5L, val.size());
        } else if (key == "string_array") {
            ASSERT_TRUE(val.isArray());
            EXPECT_EQ(2L, val.size());
        } else if (key == "int_val") {
            ASSERT_TRUE(val.isInt());
            EXPECT_EQ(100, val.asInt());
        } else if (key == "double_val") {
            ASSERT_TRUE(val.isDouble());
            EXPECT_DOUBLE_EQ(2.3, val.asDouble());
        } else {
            ASSERT_TRUE(false);
        }
        idx++;
    });
    EXPECT_EQ(keys.size(), idx);
}

}   // namespace conf
}   // namespace nebula

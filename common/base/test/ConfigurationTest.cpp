/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include <gtest/gtest.h>
#include "base/Configuration.h"
#include "fs/TempFile.h"

using vesoft::fs::TempFile;

namespace vesoft {

TEST(Configuration, Basic) {
    std::string json = "{"
                            "\"int\": 123, "
                            "\"double\": 3.14, "
                            "\"bool\": true, "
                            "\"string\": \"string\""
                       "}";
    Configuration conf;
    auto status = conf.parseFromString(json);
    ASSERT_TRUE(status.ok()) << status.msg();

    {
        int64_t val;
        status = conf.fetchAsInt("int", val);
        ASSERT_TRUE(status.ok()) << status.msg();
        ASSERT_EQ(123, val);
    }
    {
        double val;
        status = conf.fetchAsDouble("double", val);
        ASSERT_TRUE(status.ok()) << status.msg();
        ASSERT_DOUBLE_EQ(3.14, val);
    }
    {
        bool val;
        status = conf.fetchAsBool("bool", val);
        ASSERT_TRUE(status.ok()) << status.msg();
        ASSERT_TRUE(val);
    }
    {
        std::string val;
        status = conf.fetchAsString("string", val);
        ASSERT_TRUE(status.ok()) << status.msg();
        ASSERT_EQ("string", val);
    }

    {
        double val;
        status = conf.fetchAsDouble("int", val);
        ASSERT_FALSE(status.ok()) << status.msg();
    }
    {
        int64_t val;
        status = conf.fetchAsInt("string", val);
        ASSERT_FALSE(status.ok()) << status.msg();
    }
    {
        bool val;
        status = conf.fetchAsBool("nonexist", val);
        ASSERT_FALSE(status.ok()) << status.msg();
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
    ASSERT_TRUE(status.ok()) << status.msg();

    Configuration subconf;
    status = conf.fetchAsSubConf("subconf", subconf);
    ASSERT_TRUE(status.ok()) << status.msg();

    std::string val;
    status = subconf.fetchAsString("key", val);
    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ("value", val);

    status = conf.fetchAsSubConf("notsub", subconf);
    ASSERT_FALSE(status.ok()) << status.msg();
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
    ASSERT_TRUE(status.ok()) << status.msg();
    {
        int64_t val;
        status = conf.fetchAsInt("int", val);
        ASSERT_TRUE(status.ok()) << status.msg();
        ASSERT_EQ(123, val);
    }
    {
        double val;
        status = conf.fetchAsDouble("double", val);
        ASSERT_TRUE(status.ok()) << status.msg();
        ASSERT_DOUBLE_EQ(3.14, val);
    }
}

}   // namespace vesoft


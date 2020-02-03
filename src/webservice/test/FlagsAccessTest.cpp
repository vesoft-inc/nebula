/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/json.h>
#include "webservice/WebService.h"
#include "webservice/test/TestUtils.h"

DEFINE_int32(int32_test, 10, "Test flag for int32 type");
DEFINE_int64(int64_test, 10, "Test flag for int64 type");
DEFINE_bool(bool_test, false, "Test flag for bool type");
DEFINE_double(double_test, 3.14159, "Test flag for double type");
DEFINE_string(string_test, "Hello World", "Test flag for string type");
DEFINE_uint32(crash_test, 1024, "The flag could not be read, test for issue #312");

namespace nebula {

class FlagsAccessTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;
        VLOG(1) << "Starting web service...";
        webSvc_ = std::make_unique<WebService>();
        auto status = webSvc_->start();
        ASSERT_TRUE(status.ok()) << status;
    }

    void TearDown() override {
        webSvc_.reset();
        VLOG(1) << "Web service stopped";
    }

private:
    std::unique_ptr<WebService> webSvc_;
};

TEST(FlagsAccessTest, GetSetTest) {
    std::string resp;
    ASSERT_TRUE(getUrl("/get_flags?flags=int32_test", resp));
    EXPECT_EQ(folly::stringPrintf("int32_test=%d\n", FLAGS_int32_test), resp);

    ASSERT_TRUE(getUrl("/get_flags?flags=int64_test,bool_test,string_test", resp));
    EXPECT_EQ(folly::stringPrintf("int64_test=%ld\nbool_test=%s\nstring_test=\"%s\"\n",
                                  FLAGS_int64_test,
                                  (FLAGS_bool_test ? "1" : "0"),
                                  FLAGS_string_test.c_str()),
              resp);

    ASSERT_TRUE(getUrl("/set_flag?flag=int64_test&value=20", resp));
    ASSERT_EQ("true", resp);
    ASSERT_TRUE(getUrl("/get_flags?flags=int64_test", resp));
    EXPECT_EQ(std::string("int64_test=20\n"), resp);

    ASSERT_TRUE(getUrl("/get_flags", resp));
    ASSERT_TRUE(resp.find("crash_test=nullptr") != std::string::npos);
}

TEST(FlagsAccessTest, JsonTest) {
    std::string resp;
    ASSERT_TRUE(getUrl("/get_flags?flags=double_test&returnjson", resp));
    auto json = folly::parseJson(resp);
    ASSERT_TRUE(json.isArray());
    ASSERT_EQ(1UL, json.size());
    ASSERT_TRUE(json[0].isObject());
    ASSERT_EQ(2UL, json[0].size());

    auto it = json[0].find("name");
    ASSERT_NE(json[0].items().end(), it);
    ASSERT_TRUE(it->second.isString());
    EXPECT_EQ("double_test", it->second.getString());

    it = json[0].find("value");
    ASSERT_NE(json[0].items().end(), it);
    ASSERT_TRUE(it->second.isDouble());
    EXPECT_DOUBLE_EQ(FLAGS_double_test, it->second.getDouble());
}


TEST(FlagsAccessTest, VerboseTest) {
    std::string resp;
    ASSERT_TRUE(getUrl("/get_flags?flags=int32_test&returnjson&verbose", resp));
    auto json = folly::parseJson(resp);
    ASSERT_TRUE(json.isArray());
    ASSERT_EQ(1UL, json.size());
    ASSERT_TRUE(json[0].isObject());
    ASSERT_EQ(7UL, json[0].size());

    auto it = json[0].find("name");
    ASSERT_NE(json[0].items().end(), it);
    ASSERT_TRUE(it->second.isString());
    EXPECT_EQ("int32_test", it->second.getString());

    it = json[0].find("value");
    ASSERT_NE(json[0].items().end(), it);
    ASSERT_TRUE(it->second.isInt());
    EXPECT_EQ(FLAGS_int32_test, it->second.getInt());

    it = json[0].find("type");
    ASSERT_NE(json[0].items().end(), it);
    ASSERT_TRUE(it->second.isString());
    EXPECT_EQ("int32", it->second.getString());

    it = json[0].find("file");
    ASSERT_NE(json[0].items().end(), it);
    ASSERT_TRUE(it->second.isString());
    EXPECT_EQ(__FILE__, it->second.getString());

    it = json[0].find("is_default");
    ASSERT_NE(json[0].items().end(), it);
    ASSERT_TRUE(it->second.isBool());
    EXPECT_TRUE(it->second.getBool());
}


TEST(FlagsAccessTest, ErrorTest) {
    std::string resp;
    ASSERT_TRUE(getUrl("/get_flags123?flags=int32_test", resp));
    EXPECT_TRUE(resp.empty());
}

}  // namespace nebula


int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    ::testing::AddGlobalTestEnvironment(new nebula::FlagsAccessTestEnv());

    return RUN_ALL_TESTS();
}


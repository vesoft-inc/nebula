/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include <folly/json.h>
#include "common/webservice/WebService.h"
#include "common/webservice/test/TestUtils.h"

DEFINE_int32(int32_test, 10, "Test flag for int32 type");
DEFINE_int64(int64_test, 10, "Test flag for int64 type");
DEFINE_bool(bool_test, false, "Test flag for bool type");
DEFINE_double(double_test, 3.14159, "Test flag for double type");
DEFINE_string(string_test, "Hello World", "Test flag for string type");
DEFINE_uint32(uint32_test, 10, "Test flag for uint32 type");
DEFINE_uint64(uint64_test, 10, "Test flag for uint64 type");

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
    ASSERT_TRUE(getUrl("/flags?flags=int32_test", resp));
    EXPECT_EQ(folly::stringPrintf("int32_test=%d\n", FLAGS_int32_test), resp);

    ASSERT_TRUE(getUrl("/flags?flags=int64_test,bool_test,string_test", resp));
    EXPECT_EQ(folly::stringPrintf("int64_test=%ld\nbool_test=%s\nstring_test=\"%s\"\n",
                                  FLAGS_int64_test,
                                  (FLAGS_bool_test ? "1" : "0"),
                                  FLAGS_string_test.c_str()),
              resp);

    folly::dynamic data = folly::dynamic::object("int64_test", 20);
    auto status = putUrl("/flags", data);
    ASSERT_TRUE(status.ok());
    folly::dynamic json = folly::parseJson(status.value());
    ASSERT_EQ(0, json["errCode"].asInt());
    ASSERT_TRUE(getUrl("/flags?flags=int64_test", resp));
    EXPECT_EQ(std::string("int64_test=20\n"), resp);

    ASSERT_TRUE(getUrl("/flags?flags=uint32_test", resp));
    EXPECT_EQ(folly::stringPrintf("uint32_test=%u\n", FLAGS_uint32_test), resp);

    ASSERT_TRUE(getUrl("/flags?flags=uint64_test", resp));
    EXPECT_EQ(folly::stringPrintf("uint64_test=%lu\n", FLAGS_uint64_test), resp);
}

TEST(FlagsAccessTest, TestSetFlagsFailure) {
    folly::dynamic data = folly::dynamic::object("int64_test", 20)("float_test", 10.0f);
    auto status = putUrl("/flags", data);
    ASSERT_TRUE(status.ok());
    folly::dynamic json = folly::parseJson(status.value());
    folly::dynamic failedOptions = json["failedOptions"];
    ASSERT_TRUE(failedOptions.isArray());
    ASSERT_EQ(failedOptions, folly::dynamic::array("float_test"));
}

TEST(FlagsAccessTest, JsonTest) {
    std::string resp;
    ASSERT_TRUE(getUrl("/flags?flags=double_test&format=json", resp));
    auto json = folly::parseJson(resp)["flags"];
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
    ASSERT_TRUE(getUrl("/flags?flags=int32_test&format=json&verbose=true", resp));
    auto json = folly::parseJson(resp)["flags"];
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

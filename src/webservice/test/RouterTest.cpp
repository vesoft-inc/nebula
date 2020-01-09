/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "webservice/Router.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <proxygen/lib/http/HTTPMessage.h>

class RouterTest : public ::testing::Test {
public:
    void SetUp() override {
        router_ = std::make_unique<nebula::web::Router>("test");
    }

protected:
    std::unique_ptr<nebula::web::Router> router_;
};

TEST_F(RouterTest, TestNoPathParams) {
    router_->get("/foo/bar")
        .handler([](nebula::web::PathParams&& params) -> proxygen::RequestHandler* {
            EXPECT_TRUE(params.empty());
            return nullptr;
        });
    proxygen::HTTPMessage msg;
    msg.setMethod(proxygen::HTTPMethod::GET);
    msg.setURL("https://localhost/test/foo/bar");
    ASSERT_EQ(router_->dispatch(&msg), nullptr);

    msg.setURL("https://localhost/test/foo/bar/");
    ASSERT_EQ(router_->dispatch(&msg), nullptr);

    msg.setMethod(proxygen::HTTPMethod::PUT);
    ASSERT_NE(router_->dispatch(&msg), nullptr);

    msg.setMethod(proxygen::HTTPMethod::GET);
    msg.setURL("https://localhost/foo/bar");
    ASSERT_NE(router_->dispatch(&msg), nullptr);

    msg.setURL("https://localhost/test/foo");
    ASSERT_NE(router_->dispatch(&msg), nullptr);

    msg.setURL("https://localhost/test/");
    ASSERT_NE(router_->dispatch(&msg), nullptr);
}

TEST_F(RouterTest, TestPathParams) {
    router_->get("/foo/:bar/baz")
        .handler([](nebula::web::PathParams&& params) -> proxygen::RequestHandler* {
            EXPECT_EQ(1, params.size());
            auto iter = params.find("bar");
            EXPECT_NE(iter, params.end());
            return nullptr;
        });

    proxygen::HTTPMessage msg;
    msg.setMethod(proxygen::HTTPMethod::PUT);
    msg.setURL("https://localhost/test/foo/nebula/baz");
    ASSERT_NE(router_->dispatch(&msg), nullptr);

    msg.setMethod(proxygen::HTTPMethod::GET);
    ASSERT_EQ(router_->dispatch(&msg), nullptr);

    msg.setURL("https://localhost/test/foo/nebula");
    ASSERT_NE(router_->dispatch(&msg), nullptr);

    msg.setURL("https://localhost/foo/nebula");
    ASSERT_NE(router_->dispatch(&msg), nullptr);

    msg.setURL("https://localhost/foo/nebula/baz");
    ASSERT_NE(router_->dispatch(&msg), nullptr);
}

TEST_F(RouterTest, TestDiffHttpMethod) {
    int getCount = 0;
    router_->get("/foo/:bar/baz").handler([&getCount](auto&& params) {
        EXPECT_EQ(params.size(), 1);
        getCount++;
        return nullptr;
    });
    int putCount = 0;
    router_->put("/foo/:bar/baz").handler([&putCount](auto&& params) {
        EXPECT_EQ(params.size(), 1);
        putCount++;
        return nullptr;
    });
    proxygen::HTTPMessage msg;
    msg.setMethod(proxygen::HTTPMethod::PUT);
    msg.setURL("https://localhost/test/foo/nebula/baz");
    ASSERT_EQ(router_->dispatch(&msg), nullptr);

    msg.setURL("https://localhost/test/foo/graph/baz");
    ASSERT_EQ(router_->dispatch(&msg), nullptr);

    msg.setMethod(proxygen::HTTPMethod::GET);
    msg.setURL("https://localhost/test/foo/nebula/baz");
    ASSERT_EQ(router_->dispatch(&msg), nullptr);

    ASSERT_EQ(getCount, 1);
    ASSERT_EQ(putCount, 2);
}

TEST_F(RouterTest, TestNoPrefix) {
    nebula::web::Router router("");
    router.get("/foo/:bar/:baz")
        .handler([](nebula::web::PathParams&& params) -> proxygen::RequestHandler* {
            EXPECT_EQ(params.size(), 2);
            auto iter = params.find("bar");
            EXPECT_NE(iter, params.end());
            iter = params.find("baz");
            EXPECT_NE(iter, params.end());
            return nullptr;
        });

    proxygen::HTTPMessage msg;
    msg.setMethod(proxygen::HTTPMethod::GET);
    msg.setURL("https://localhost/foo/nebula/graph");
    ASSERT_EQ(router.dispatch(&msg), nullptr);

    msg.setURL("https://localhost/foo/nebula/graph/");
    ASSERT_EQ(router.dispatch(&msg), nullptr);

    msg.setURL("https://localhost/foo/nebula");
    ASSERT_NE(router.dispatch(&msg), nullptr);

    msg.setURL("https://localhost/foo");
    ASSERT_NE(router.dispatch(&msg), nullptr);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

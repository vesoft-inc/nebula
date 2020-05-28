/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/webservice/Router.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/lib/http/HTTPMessage.h>

class RouterTest : public ::testing::Test {
public:
    void SetUp() override {
        router_ = std::make_unique<nebula::web::Router>("test");
    }

    void TearDown() override {
        router_.reset();
    }

    static void found(const nebula::web::Router* router, const proxygen::HTTPMessage* msg) {
        std::unique_ptr<proxygen::RequestHandler> handler(router->dispatch(msg));
        ASSERT_FALSE(handler);
    }

    static void notFound(const nebula::web::Router* router, const proxygen::HTTPMessage* msg) {
        std::unique_ptr<proxygen::RequestHandler> handler(router->dispatch(msg));
        ASSERT_TRUE(handler);
    }

    void found(const proxygen::HTTPMessage* msg) const {
        found(router_.get(), msg);
    }

    void notFound(const proxygen::HTTPMessage* msg) const {
        notFound(router_.get(), msg);
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
    found(&msg);

    msg.setURL("https://localhost/test/foo/bar/");
    found(&msg);

    msg.setMethod(proxygen::HTTPMethod::PUT);
    notFound(&msg);

    msg.setMethod(proxygen::HTTPMethod::GET);
    msg.setURL("https://localhost/foo/bar");
    notFound(&msg);

    msg.setURL("https://localhost/test/foo");
    notFound(&msg);

    msg.setURL("https://localhost/test/");
    notFound(&msg);
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
    notFound(&msg);

    msg.setMethod(proxygen::HTTPMethod::GET);
    found(&msg);

    msg.setURL("https://localhost/test/foo/nebula");
    notFound(&msg);

    msg.setURL("https://localhost/foo/nebula");
    notFound(&msg);

    msg.setURL("https://localhost/foo/nebula/baz");
    notFound(&msg);
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
    found(&msg);

    msg.setURL("https://localhost/test/foo/graph/baz");
    found(&msg);

    msg.setMethod(proxygen::HTTPMethod::GET);
    msg.setURL("https://localhost/test/foo/nebula/baz");
    found(&msg);

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
    found(&router, &msg);

    msg.setURL("https://localhost/foo/nebula/graph/");
    found(&router, &msg);

    msg.setURL("https://localhost/foo/nebula");
    notFound(&router, &msg);

    msg.setURL("https://localhost/foo");
    notFound(&router, &msg);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

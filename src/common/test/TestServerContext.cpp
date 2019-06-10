/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "test/ServerContext.h"


namespace nebula {
namespace test {

class TestServer final : public apache::thrift::ServerInterface {
public:
    TestServer() {}
    ~TestServer() {}
    std::unique_ptr<apache::thrift::AsyncProcessor> getProcessor() override {
        return nullptr;
    }
};

TEST(ServerContext, mockCommon) {
    auto sc = std::make_unique<ServerContext>();
    auto handler = std::make_shared<TestServer>();
    sc->mockCommon("test", 0, handler);
}

}  // namespace test
}  // namespace nebula

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

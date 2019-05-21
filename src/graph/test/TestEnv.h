/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_TEST_TESTENV_H_
#define GRAPH_TEST_TESTENV_H_

#include "base/Base.h"
#include "fs/TempDir.h"
#include "client/cpp/GraphClient.h"
#include "test/ServerContext.h"
#include <gtest/gtest.h>
#include "TestUtils.h"

namespace nebula {
namespace graph {

class GraphClient;
class TestEnv : public ::testing::Environment {
public:
    TestEnv();
    virtual ~TestEnv();

    void SetUp() override;

    void TearDown() override;
    // Obtain the system assigned listening port
    uint16_t graphServerPort() const;
    uint16_t metaServerPort() const;
    uint16_t storageServerPort() const;

    std::unique_ptr<GraphClient> getClient() const;

private:
    nebula::fs::TempDir                             metaRootPath_{"/tmp/MetaTest.XXXXXX"};
    nebula::fs::TempDir                             storageRootPath_{"/tmp/StorageTest.XXXXXX"};
    std::unique_ptr<test::ServerContext>            metaServer_;
    std::unique_ptr<test::ServerContext>            storageServer_;
    std::unique_ptr<test::ServerContext>            graphServer_;
    std::unique_ptr<meta::MetaClient>               mClient_;
};


extern TestEnv *gEnv;

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_TEST_TESTENV_H_

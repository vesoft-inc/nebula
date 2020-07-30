/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef MOCK_TEST_TESTENV_H_
#define MOCK_TEST_TESTENV_H_

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/clients/graph/GraphClient.h"
#include "common/clients/meta/MetaClient.h"
#include "common/clients/storage/GraphStorageClient.h"
#include <gtest/gtest.h>
#include "mock/MockServer.h"

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

    meta::MetaClient* getMetaClient();
    storage::GraphStorageClient* getStorageClient();

    std::unique_ptr<GraphClient> getGraphClient() const;
private:
    std::unique_ptr<MockServer>                     mServer_{nullptr};
    std::unique_ptr<meta::MetaClient>               mClient_{nullptr};
    std::unique_ptr<storage::GraphStorageClient>    sClient_{nullptr};
    uint16_t                                        graphPort_{0};
    uint16_t                                        metaPort_{0};
    uint16_t                                        storagePort_{0};
};


extern TestEnv *gEnv;

}   // namespace graph
}   // namespace nebula

#endif  // MOCK_TEST_TESTENV_H_

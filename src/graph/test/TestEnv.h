/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_TEST_TESTENV_H_
#define GRAPH_TEST_TESTENV_H_

#include "base/Base.h"
#include "fs/TempDir.h"
#include "client/cpp/lib/NebulaClientImpl.h"
#include "test/ServerContext.h"
#include <gtest/gtest.h>
#include "TestUtils.h"
#include "graph/GraphFlags.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace graph {

class NebulaClientImpl;
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

    std::unique_ptr<NebulaClientImpl> getClient(const std::string& user = "root",
                                                const std::string& password = "nebula") const;

    meta::ClientBasedGflagsManager* gflagsManager();

    test::ServerContext* storageServer();

    meta::MetaClient* metaClient();

    const std::string getMetaRootPath() {
        return metaRootPath_.path();
    }

    const std::string getStorageRootPath() {
        return storageRootPath_.path();
    }

private:
    nebula::fs::TempDir                             metaRootPath_{"/tmp/MetaTest.XXXXXX"};
    nebula::fs::TempDir                             storageRootPath_{"/tmp/StorageTest.XXXXXX"};
    std::unique_ptr<test::ServerContext>            metaServer_{nullptr};
    std::unique_ptr<test::ServerContext>            storageServer_{nullptr};
    std::unique_ptr<test::ServerContext>            graphServer_{nullptr};
    std::unique_ptr<meta::MetaClient>               mClient_{nullptr};
    std::unique_ptr<meta::ClientBasedGflagsManager> gflagsManager_{nullptr};
};


extern TestEnv *gEnv;

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_TEST_TESTENV_H_

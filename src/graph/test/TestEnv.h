/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_TEST_TESTENV_H_
#define GRAPH_TEST_TESTENV_H_

#include "base/Base.h"
#include <gtest/gtest.h>
#include "thread/NamedThread.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "graph/GraphService.h"
#include "client/cpp/GraphClient.h"

namespace apache {
namespace thrift {
class ThriftServer;
}
}

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
    uint16_t serverPort() const;

    std::unique_ptr<GraphClient> getClient() const;

private:
    std::unique_ptr<apache::thrift::ThriftServer>   server_;
    std::unique_ptr<thread::NamedThread>            thread_;
};


extern TestEnv *gEnv;

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_TEST_TESTENV_H_

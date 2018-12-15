/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_TEST_TESTENV_H_
#define VGRAPHD_TEST_TESTENV_H_

#include "base/Base.h"
#include <gtest/gtest.h>
#include "thread/NamedThread.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "vgraphd/VGraphService.h"
#include "client/cpp/GraphDbClient.h"

namespace apache {
namespace thrift {
class ThriftServer;
}
}

namespace vesoft {
namespace vgraph {

class GraphDbClient;
class TestEnv : public ::testing::Environment {
public:
    TestEnv();
    virtual ~TestEnv();

    void SetUp() override;

    void TearDown() override;
    // Obtain the system assigned listening port
    uint16_t serverPort() const;

    std::unique_ptr<GraphDbClient> getClient() const;

private:
    std::unique_ptr<apache::thrift::ThriftServer>   server_;
    std::unique_ptr<thread::NamedThread>            thread_;
};


extern TestEnv *gEnv;

}   // namespace vgraph
}   // namespace vesoft

#endif  // VGRAPHD_TEST_TESTENV_H_

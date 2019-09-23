/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "test/ServerContext.h"
#include "graph/GraphService.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>


namespace nebula {
namespace graph {

class TestUtils {
public:
    static std::unique_ptr<test::ServerContext> mockGraphServer(uint32_t port) {
        auto sc = std::make_unique<test::ServerContext>();
        auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
        auto interface = std::make_shared<GraphService>();
        auto status = interface->init(threadPool);
        CHECK(status.ok()) << status;
        sc->mockCommon("graph", port, interface);
        LOG(INFO) << "Starting the graph Daemon on port " << sc->port_;
        return sc;
    }
};

}  // namespace graph
}  // namespace nebula


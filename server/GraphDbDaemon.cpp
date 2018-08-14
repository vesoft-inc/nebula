/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "server/GraphDbServiceHandler.h"

DEFINE_int32(port, 34500, "GraphDB daemon listening port");


namespace vesoft {
namespace vgraph {
}  // namespace vgraph
}  // namespace vesoft


int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);

    using namespace vesoft::vgraph;

    LOG(INFO) << "Starting the vGraph Daemon on port " << FLAGS_port;

    auto handler = std::make_shared<GraphDbServiceHandler>();
    auto server = std::make_shared<apache::thrift::ThriftServer>();
    CHECK(!!server) << "Failed to create the thrift server";

    server->setInterface(handler);
    server->setPort(FLAGS_port);

    server->serve();  // Will wait until the server shuts down

    LOG(INFO) << "The vGraph Daemon on port " << FLAGS_port << " stopped";
}


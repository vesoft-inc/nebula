/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "signal/SignalUtils.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "meta/MetaServiceHandler.h"


void exit_signal_handler(int signal_num);

DEFINE_int32(port, 45500, "Meta daemon listening port");

static std::shared_ptr<apache::thrift::ThriftServer> server;

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);

    LOG(INFO) << "Starting the meta Daemon on port " << FLAGS_port;

    // Setup the signal handlers
    SYSTEM_SIGNAL_SETUP("metad");

    auto handler = std::make_shared<nebula::meta::MetaServiceHandler>();
    server = std::make_shared<apache::thrift::ThriftServer>();
    CHECK(!!server) << "Failed to create the thrift server";

    server->setInterface(handler);
    server->setPort(FLAGS_port);

    server->serve();  // Will wait until the server shuts down

    LOG(INFO) << "The storage Daemon on port " << FLAGS_port << " stopped";
}

void exit_signal_handler(int signal_num) {
    LOG(INFO) << "metad interrupt, signal number is : " << signal_num;
    server->stop();
    ::exit(signal_num);
}

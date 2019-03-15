/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "meta/MetaServiceHandler.h"
#include "meta/MetaHttpHandler.h"
#include "webservice/WebService.h"

DEFINE_int32(port, 45500, "Meta daemon listening port");

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);

    LOG(INFO) << "Starting Meta HTTP Service";
    nebula::WebService::registerHandler("/meta", [] {
        return new nebula::meta::MetaHttpHandler();
    });
    nebula::WebService::start();

    LOG(INFO) << "Starting the meta Daemon on port " << FLAGS_port;

    auto handler = std::make_shared<nebula::meta::MetaServiceHandler>();
    auto server = std::make_shared<apache::thrift::ThriftServer>();
    CHECK(!!server) << "Failed to create the thrift server";

    server->setInterface(handler);
    server->setPort(FLAGS_port);

    server->serve();  // Will wait until the server shuts down

    LOG(INFO) << "The storage Daemon on port " << FLAGS_port << " stopped";
}


/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include <folly/json.h>
#include "graph/GraphHttpHandler.h"
#include "webservice/WebService.h"
#include "process/ProcessUtils.h"
#include "graph/GraphFlags.h"


namespace nebula {
namespace graph {

using nebula::WebService;

class GraphHttpHandlerTest : public TestBase {
protected:
    void SetUp() override {
        TestBase::SetUp();

        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;
        VLOG(1) << "Starting web service...";
        WebService::registerHandler("/get_graph", [] {
            return new graph::GraphHttpHandler();
        });
        auto status = WebService::start();
        ASSERT_TRUE(status.ok()) << status;
    }

    void TearDown() override {
        WebService::stop();
        VLOG(1) << "Web service stopped";

        TestBase::TearDown();
    }
};


bool getUrl(const std::string& urlPath, std::string& respBody) {
    auto url = folly::stringPrintf("http://%s:%d%s",
                                   FLAGS_ws_ip.c_str(),
                                   FLAGS_ws_http_port,
                                   urlPath.c_str());
    VLOG(1) << "Retrieving url: " << url;

    auto command = folly::stringPrintf("/usr/bin/curl -G \"%s\" 2> /dev/null",
                                       url.c_str());
    auto result = ProcessUtils::runCommand(command.c_str());
    if (!result.ok()) {
        LOG(ERROR) << "Failed to run curl: " << result.status();
        return false;
    }
    respBody = result.value();
    return true;
}


TEST_F(GraphHttpHandlerTest, GraphStatusTest) {
    std::string pidFile = FLAGS_graph_pid_file;
    FLAGS_graph_pid_file = gEnv->getPidFileName();
    {
        std::string resp;
        ASSERT_TRUE(getUrl("/get_graph?graph=status", resp));
        ASSERT_EQ(std::string("status=running\n"), resp);
    }
    {
        std::string resp;
        ASSERT_TRUE(getUrl("/get_graph?graph=status&returnjson", resp));
        auto json = folly::parseJson(resp);
        ASSERT_TRUE(json.isArray());
        ASSERT_EQ(1UL, json.size());
        ASSERT_TRUE(json[0].isObject());
        ASSERT_EQ(2UL, json[0].size());

        auto it = json[0].find("name");
        ASSERT_NE(json[0].items().end(), it);
        ASSERT_TRUE(it->second.isString());
        ASSERT_EQ("status", it->second.getString());

        it = json[0].find("value");
        ASSERT_NE(json[0].items().end(), it);
        ASSERT_TRUE(it->second.isString());
        ASSERT_EQ("running", it->second.getString());
    }

    {
        std::string resp;
        ASSERT_TRUE(getUrl("/get_graph123?graph=status", resp));
        ASSERT_TRUE(resp.empty());
    }
    FLAGS_graph_pid_file = pidFile;
}

}  // namespace graph
}  // namespace nebula


/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include <fstream>
#include "fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include "meta/processors/admin/HBProcessor.h"

DECLARE_bool(hosts_whitelist_enabled);

namespace nebula {
namespace meta {

TEST(HBProcessorTest, HBTest) {
    fs::TempDir rootPath("/tmp/HBTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    const ClusterID kClusterId = 10;
    {
        for (auto i = 0; i < 5; i++) {
            cpp2::HBReq req;
            req.set_in_storaged(true);
            nebula::cpp2::HostAddr thriftHost;
            thriftHost.set_ip(i);
            thriftHost.set_port(i);
            req.set_host(std::move(thriftHost));
            req.set_cluster_id(kClusterId);
            req.set_in_storaged(true);
            auto* processor = HBProcessor::instance(kv.get(), kClusterId);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        }
        auto hosts = ActiveHostsMan::getActiveHosts(kv.get(), 1);
        ASSERT_EQ(5, hosts.size());
        sleep(3);
        ASSERT_EQ(0, ActiveHostsMan::getActiveHosts(kv.get(), 1).size());

        LOG(INFO) << "Test for invalid host!";
        cpp2::HBReq req;
        nebula::cpp2::HostAddr thriftHost;
        thriftHost.set_ip(11);
        thriftHost.set_port(11);
        req.set_host(std::move(thriftHost));
        req.set_cluster_id(1);
        req.set_in_storaged(true);
        auto* processor = HBProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_WRONGCLUSTER, resp.code);
    }
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


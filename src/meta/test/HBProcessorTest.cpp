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
#include <common/time/TimeUtils.h>
#include "meta/processors/HBProcessor.h"

DECLARE_int32(expired_hosts_check_interval_sec);
DECLARE_int32(expired_threshold_sec);

namespace nebula {
namespace meta {

using nebula::cpp2::SupportedType;
using apache::thrift::FragileConstructor::FRAGILE;

TEST(HBProcessorTest, HBTest) {
    fs::TempDir rootPath("/tmp/HBTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    {
        std::vector<nebula::cpp2::HostAddr> thriftHosts;
        for (auto i = 0; i < 10; i++) {
            thriftHosts.emplace_back(FRAGILE, i, i);
        }
        cpp2::AddHostsReq req;
        req.set_hosts(std::move(thriftHosts));
        auto* processor = AddHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    {
        cpp2::ListHostsReq req;
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(10, resp.hosts.size());
        for (auto i = 0; i < 10; i++) {
            ASSERT_EQ(i, resp.hosts[i].ip);
            ASSERT_EQ(i, resp.hosts[i].port);
        }
    }
    {
        FLAGS_expired_hosts_check_interval_sec = 1;
        FLAGS_expired_threshold_sec = 1;
        for (auto i = 0; i < 5; i++) {
            cpp2::HBReq req;
            nebula::cpp2::HostAddr thriftHost(FRAGILE, i, i);
            req.set_host(std::move(thriftHost));
            auto* processor = HBProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        }
        auto hosts = ActiveHostsManHolder::hostsMan()->getActiveHosts();
        ASSERT_EQ(5, hosts.size());
        sleep(3);
        ASSERT_EQ(0, ActiveHostsManHolder::hostsMan()->getActiveHosts().size());

        LOG(INFO) << "Test for invalid host!";
        cpp2::HBReq req;
        nebula::cpp2::HostAddr thriftHost(FRAGILE, 11, 11);
        req.set_host(std::move(thriftHost));
        auto* processor = HBProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_INVALID_HOST, resp.code);
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



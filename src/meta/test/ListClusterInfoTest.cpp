/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/processors/admin/ListClusterInfoProcessor.h"
#include "meta/test/TestUtils.h"
#include "utils/Utils.h"

namespace nebula {
namespace meta {

namespace {
const char root_dir[] = "/tmp/create_backup_test.XXXXXX";
const char data_dir[] = "/tmp/create_backup_test.XXXXXX/data";
}   // namespace

class TestStorageService : public storage::cpp2::StorageAdminServiceSvIf {
public:
    folly::Future<storage::cpp2::ListClusterInfoResp> future_listClusterInfo(
        const storage::cpp2::ListClusterInfoReq& req) override {
        UNUSED(req);
        folly::Promise<storage::cpp2::ListClusterInfoResp> pro;
        auto f = pro.getFuture();
        storage::cpp2::ListClusterInfoResp resp;
        storage::cpp2::ResponseCommon result;
        std::vector<storage::cpp2::PartitionResult> partRetCode;
        result.set_failed_parts(partRetCode);
        resp.set_result(result);
        nebula::cpp2::DirInfo dir;
        dir.set_root(root_dir);
        dir.set_data({data_dir});
        resp.set_dir(std::move(dir));
        pro.setValue(std::move(resp));
        return f;
    }
};

TEST(ProcessorTest, ListClusterInfoTest) {
    auto rpcServer = std::make_unique<mock::RpcServer>();
    auto handler = std::make_shared<TestStorageService>();
    rpcServer->start("storage-admin", 0, handler);
    LOG(INFO) << "Start storage server on " << rpcServer->port_;

    std::string localIp("127.0.0.1");

    LOG(INFO) << "Now test interfaces with retry to leader!";

    fs::TempDir rootPath(root_dir);
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    HostAddr host(localIp, rpcServer->port_);
    HostAddr storageHost = Utils::getStoreAddrFromAdminAddr(host);

    auto client = std::make_unique<AdminClient>(kv.get());
    std::vector<HostAddr> hosts;
    hosts.emplace_back(storageHost);
    meta::TestUtils::registerHB(kv.get(), hosts);

    {
        cpp2::ListClusterInfoReq req;
        auto* processor = ListClusterInfoProcessor::instance(kv.get(), client.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        LOG(INFO) << folly::to<int>(resp.get_code());
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        for (auto s : resp.get_storage_servers()) {
            ASSERT_EQ(storageHost, s.get_host());
            ASSERT_EQ(s.get_dir().get_root(), root_dir);
            ASSERT_EQ(s.get_dir().get_data()[0], data_dir);
        }
    }
}
}   // namespace meta
}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}

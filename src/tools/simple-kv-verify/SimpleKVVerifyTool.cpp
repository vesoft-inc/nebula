/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "meta/SchemaManager.h"
#include "meta/client/MetaClient.h"
#include "storage/client/StorageClient.h"
#include <folly/executors/IOThreadPoolExecutor.h>

DEFINE_string(meta_server_addrs, "", "meta server address");
DEFINE_string(space_name, "test", "Specify the space name");
DEFINE_int32(io_threads, 8, "Client io threads");
DEFINE_int32(testing_times, 128, "Verification times");

namespace nebula {
namespace storage {

class SimpleKVVerifyTool {
public:
    int32_t init() {
        auto addrs = nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
        if (!addrs.ok()) {
            LOG(ERROR) << "Meta Service Address Illegal: " << FLAGS_meta_server_addrs;
            return EXIT_FAILURE;
        }

        auto threadFactory = std::make_shared<folly::NamedThreadFactory>("benchmark-netio");
        auto ioExecutor = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_io_threads,
                                                                        std::move(threadFactory));
        metaClient_ = std::make_unique<nebula::meta::MetaClient>(ioExecutor,
                                                                 std::move(addrs.value()));

        // load data try 3 time
        bool loadDataOk = metaClient_->waitForMetadReady(3);
        if (!loadDataOk) {
            // Resort to retrying in the background
            LOG(ERROR) << "Failed to synchronously wait for meta service ready";
            return EXIT_FAILURE;
        }

        auto spaceResult = metaClient_->getSpaceIdByNameFromCache(FLAGS_space_name);
        if (!spaceResult.ok()) {
            LOG(ERROR) << "Get SpaceID Failed: " << spaceResult.status();
            return EXIT_FAILURE;
        }
        space_ = spaceResult.value();
        LOG(INFO) << "Space ID: " << space_;

        storageClient_ = std::make_unique<storage::StorageClient>(ioExecutor,
                                                                  metaClient_.get());
        return EXIT_SUCCESS;
    }

    void runInsert(std::unordered_map<std::string, std::string>& data) {
        std::vector<nebula::cpp2::Pair> pairs;
        for (int32_t i = 0; i < 1000; i ++) {
            auto key = std::to_string(folly::Random::rand32(1000000000));
            auto value = std::to_string(folly::Random::rand32(1000000000));
            data[key] = value;
            pairs.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                               std::move(key), std::move(value));
        }

        auto future = storageClient_->put(space_, std::move(pairs));
        auto resp = std::move(future).get();
        if (!resp.succeeded()) {
            LOG(ERROR) << "Put Failed";
            return;
        }

        if (!resp.failedParts().empty()) {
            for (const auto& partEntry : resp.failedParts()) {
                LOG(ERROR) << "Put Failed in " << partEntry.first << ", Code: "
                           << static_cast<int32_t>(partEntry.second);
            }
            return;
        }
        LOG(INFO) << "Put Successfully";
    }


    void runCheck(std::unordered_map<std::string, std::string>& pairs) {
        std::vector<std::string> keys;
        for (auto& pair : pairs) {
            keys.emplace_back(pair.first);
        }

        auto future = storageClient_->get(space_, std::move(keys));
        auto resp = std::move(future).get();
        if (!resp.succeeded()) {
            LOG(ERROR) << "Get Failed";
            return;
        }

        if (!resp.failedParts().empty()) {
            for (const auto& partEntry : resp.failedParts()) {
                LOG(ERROR) << "Put Failed in " << partEntry.first << ", Code: "
                           << static_cast<int32_t>(partEntry.second);
            }
            return;
        }

        for (auto& pair : pairs) {
            auto key = pair.first;
            bool found = false;
            for (const auto& result : resp.responses()) {
                auto iter = result.values.find(key);
                if (iter != result.values.end()) {
                    if (iter->second != pairs[key]) {
                        LOG(ERROR) << "Check Fail: key = " << key << ", values: "
                                   << iter->second << " != " << pairs[key];
                        return;
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOG(ERROR) << "Check Fail: key = " << key << " not found";
            }
        }

        LOG(INFO) << "Get Successfully";
    }

private:
    std::unique_ptr<nebula::storage::StorageClient> storageClient_;
    std::unique_ptr<nebula::meta::MetaClient> metaClient_;
    nebula::GraphSpaceID space_;
};

}  // namespace storage
}  // namespace nebula

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    nebula::storage::SimpleKVVerifyTool checker;

    if (checker.init() != EXIT_SUCCESS) return EXIT_FAILURE;

    for (int32_t i = 0; i < FLAGS_testing_times; i++) {
        std::unordered_map<std::string, std::string> data;
        checker.runInsert(data);
        checker.runCheck(data);
    }
    return EXIT_SUCCESS;
}

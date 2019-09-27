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

std::unique_ptr<nebula::storage::StorageClient> storageClient;
std::unique_ptr<nebula::meta::MetaClient> metaClient;
int32_t space;

int init() {
    auto addrs = nebula::network::NetworkUtils::toHosts("192.168.8.5:54500");
    if (!addrs.ok()) {
        return EXIT_FAILURE;
    }

    auto threadFactory = std::make_shared<folly::NamedThreadFactory>("benchmark-netio");
    auto ioExecutor = std::make_shared<folly::IOThreadPoolExecutor>(128, std::move(threadFactory));
    metaClient = std::make_unique<nebula::meta::MetaClient>(ioExecutor, std::move(addrs.value()));

    // load data try 3 time
    bool loadDataOk = metaClient->waitForMetadReady(3);
    if (!loadDataOk) {
        // Resort to retrying in the background
        LOG(INFO) << "Failed to synchronously wait for meta service ready";
        return EXIT_FAILURE;
    }

    space = metaClient->getSpaceIdByNameFromCache("demo").value();
    storageClient = std::make_unique<nebula::storage::StorageClient>(ioExecutor, metaClient.get());
    LOG(INFO) << "Space ID: " << space;
    return EXIT_SUCCESS;
}

void runInsertRound(std::unordered_map<std::string, std::string>& round_kvs) {
    std::vector<nebula::cpp2::Pair> pairs;
    for (int i = 0; i < 1000; i ++) {
        std::string key = std::to_string(folly::Random::rand32(1000000000));
        std::string value = std::to_string(folly::Random::rand32(1000000000));
        round_kvs[key] = value;

        nebula::cpp2::Pair kv;
        kv.set_key(key);
        kv.set_value(value);
        std::vector<nebula::cpp2::Pair> kvs;
        pairs.emplace_back(std::move(kv));
    }
    auto future = storageClient->put(space, std::move(pairs));
    auto resp = std::move(future).get();
    if (!resp.succeeded()) {
        LOG(INFO) << "Client Put Failed";
        return;
    }
    auto& result = resp.responses()[0];
    if (!result.result.failed_codes.empty()) {
        auto partId = result.result.failed_codes[0].part_id;
        auto code = result.result.failed_codes[0].code;
        LOG(INFO) << "Client Put Failed in " << partId << ", Code: " << static_cast<int32_t>(code);
        return;
    }

    LOG(INFO) << "Insert Done: " << round_kvs.size();
}


void runCheckRound(std::unordered_map<std::string, std::string>& round_kvs) {
    for (auto& kv : round_kvs) {
        std::string key = kv.first;
        std::vector<std::string> keys;
        keys.emplace_back(key);

        auto future = storageClient->get(space, std::move(keys));
        auto resp = std::move(future).get();
        if (!resp.succeeded()) {
            LOG(INFO) << "Client Get Failed";
            return;
        }
        auto& result = resp.responses()[0];
        if (!result.result.failed_codes.empty()) {
            auto partId = result.result.failed_codes[0].part_id;
            auto code = result.result.failed_codes[0].code;
            LOG(INFO) << "Client Get Failed in " << partId << ", Code: "
                      << static_cast<int32_t>(code);
            return;
        }

        std::string& actual_value = result.values[key];
        std::string& expect_value = round_kvs[key];
        if (actual_value != expect_value) {
            LOG(INFO) << "Check Fail: " << actual_value << " | " << expect_value;
            return;
        }
    }

    LOG(INFO) << "Check Done: " << round_kvs.size();
}


int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    if (init() != EXIT_SUCCESS) return EXIT_FAILURE;

    while (true) {
        std::unordered_map<std::string, std::string> round_kvs;
        runInsertRound(round_kvs);
        sleep(1);

        runCheckRound(round_kvs);
        sleep(1);
    }
    return EXIT_SUCCESS;
}

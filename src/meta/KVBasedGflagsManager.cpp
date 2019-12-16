/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/KVBasedGflagsManager.h"
#include "meta/processors/Common.h"
#include "meta/MetaServiceUtils.h"

namespace nebula {
namespace meta {

KVBasedGflagsManager::KVBasedGflagsManager(kvstore::KVStore* kv) {
    kvstore_ = dynamic_cast<kvstore::NebulaStore*>(kv);
    CHECK_NOTNULL(kvstore_);
}

KVBasedGflagsManager::~KVBasedGflagsManager() {
    kvstore_ = nullptr;
}

Status KVBasedGflagsManager::init() {
    auto gflagsModule = cpp2::ConfigModule::UNKNOWN;
    getGflagsModule(gflagsModule);
    gflagsDeclared_ = declareGflags(gflagsModule);
    return registerGflags(gflagsDeclared_);
}

folly::Future<StatusOr<bool>>
KVBasedGflagsManager::setConfig(const cpp2::ConfigModule& module, const std::string& name,
                                const cpp2::ConfigType& type, const VariantType &value,
                                const bool isForce) {
    UNUSED(module); UNUSED(name); UNUSED(type); UNUSED(value); UNUSED(isForce);
    LOG(FATAL) << "Unimplement!";
    return Status::NotSupported();
}

folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>>
KVBasedGflagsManager::getConfig(const cpp2::ConfigModule& module, const std::string& name) {
    UNUSED(module); UNUSED(name);
    LOG(FATAL) << "Unimplement!";
    return Status::NotSupported();
}

folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>>
KVBasedGflagsManager::listConfigs(const cpp2::ConfigModule& module) {
    UNUSED(module);
    LOG(FATAL) << "Unimplement!";
    return Status::NotSupported();
}

Status KVBasedGflagsManager::registerGflags(const std::vector<cpp2::ConfigItem>& gflagsDeclared) {
    // based on kv, so we will retry until register config successfully
    bool succeeded = false;
    while (!succeeded) {
        std::vector<kvstore::KV> data;
        folly::SharedMutex::WriteHolder wHolder(LockUtils::configLock());
        for (const auto& item : gflagsDeclared) {
            auto module = item.get_module();
            auto name = item.get_name();
            auto type = item.get_type();
            auto mode = item.get_mode();
            auto value = item.get_value();

            std::string configKey = MetaServiceUtils::configKey(module, name);
            std::string configValue;
            // ignore config which has been registered before
            auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, configKey, &configValue);
            if (ret == kvstore::ResultCode::SUCCEEDED) {
                continue;
            }
            configValue = MetaServiceUtils::configValue(type, mode, value);
            data.emplace_back(std::move(configKey), std::move(configValue));
        }

        if (!data.empty()) {
            // still have some gflags unregistered, try to write
            folly::Baton<true, std::atomic> baton;
            kvstore_->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
                                    [&] (kvstore::ResultCode code) {
                if (code == kvstore::ResultCode::SUCCEEDED) {
                    succeeded = true;
                } else if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                    auto leader = kvstore_->partLeader(kDefaultSpaceId, kDefaultPartId);
                    if (ok(leader)) {
                        auto addr = value(std::move(leader));
                        // HostAddr(0, 0) means no leader exists
                        if (addr != HostAddr(0, 0)) {
                            succeeded = true;
                        }
                    }
                }
                baton.post();
            });
            baton.wait();
        } else {
            // all gflags has been registered through leader
            succeeded = true;
        }

        if (!succeeded) {
            LOG(INFO) << "Register gflags failed, try after 1s";
            sleep(1);
        } else {
            LOG(INFO) << "Register gflags ok";
        }
    }
    return Status::OK();
}

}  // namespace meta
}  // namespace nebula

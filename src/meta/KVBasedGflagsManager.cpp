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

KVBasedGflagsManager* KVBasedGflagsManager::instance(kvstore::KVStore* kv) {
    static auto gflagsMan = std::unique_ptr<KVBasedGflagsManager>(new KVBasedGflagsManager(kv));
    static std::once_flag initFlag;
    std::call_once(initFlag, &KVBasedGflagsManager::init, gflagsMan.get());
    return gflagsMan.get();
}

KVBasedGflagsManager::KVBasedGflagsManager(kvstore::KVStore* kv) {
    kvstore_ = dynamic_cast<kvstore::NebulaStore*>(kv);
    CHECK_NOTNULL(kvstore_);
}

KVBasedGflagsManager::~KVBasedGflagsManager() {
    kvstore_ = nullptr;
}

void KVBasedGflagsManager::init() {
    CHECK(bgThread_.start());
    declareGflags();
    loadCfgThreadFunc();
    regCfgThreadFunc();
}

folly::Future<Status>
KVBasedGflagsManager::setConfig(const cpp2::ConfigModule& module, folly::StringPiece name,
                                const cpp2::ConfigType& type, const VariantType &value) {
    UNUSED(module);
    UNUSED(name);
    UNUSED(type);
    UNUSED(value);
    LOG(FATAL) << "Unimplement!";
    return Status::OK();
}

folly::Future<StatusOr<std::vector<ConfigItem>>>
KVBasedGflagsManager::getConfig(const cpp2::ConfigModule& module, folly::StringPiece name) {
    UNUSED(module);
    UNUSED(name);
    LOG(FATAL) << "Unimplement!";
    return Status::OK();
}

folly::Future<StatusOr<std::vector<ConfigItem>>>
KVBasedGflagsManager::listConfigs(const cpp2::ConfigModule& module) {
    UNUSED(module);
    LOG(FATAL) << "Unimplement!";
    return Status::OK();
}

folly::Future<Status>
KVBasedGflagsManager::registerConfig(const cpp2::ConfigModule& module, const std::string& name,
                                     const cpp2::ConfigType& type, const cpp2::ConfigMode& mode,
                                     const VariantType& value) {
    UNUSED(module);
    UNUSED(name);
    UNUSED(type);
    UNUSED(mode);
    UNUSED(value);
    LOG(FATAL) << "Unimplement!";
    return Status::OK();
}

void KVBasedGflagsManager::loadCfgThreadFunc() {
    std::string flag;
    gflags::GetCommandLineOption("load_config_interval_secs", &flag);
    size_t delayMS = stoi(flag) * 1000 + folly::Random::rand32(900);
    bgThread_.addDelayTask(delayMS, &KVBasedGflagsManager::loadCfgThreadFunc, this);
    LOG(INFO) << "Load configs after " << delayMS << " ms";

    folly::SharedMutex::ReadHolder rHolder(LockUtils::configLock());
    auto prefix = MetaServiceUtils::configKeyPrefix(module_);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return;
    }

    std::vector<ConfigItem> items;
    while (iter->valid()) {
        auto key = iter->key();
        auto value = iter->val();
        auto item = MetaServiceUtils::parseConfigValue(value);
        auto configName = MetaServiceUtils::parseConfigKey(key);
        item.set_module(configName.first);
        item.set_name(configName.second);
        items.emplace_back(toConfigItem(item));
        iter->next();
    }

    updateConfigCache(items);
}

void KVBasedGflagsManager::regCfgThreadFunc() {
    std::vector<kvstore::KV> data;
    folly::SharedMutex::WriteHolder wHolder(LockUtils::configLock());
    for (const auto& item : gflagsDeclared_) {
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
        data.emplace_back(configKey, configValue);
    }

    bool succeeded = false;
    if (!data.empty()) {
        if (kvstore_->isLeader(kDefaultSpaceId, kDefaultPartId)) {
            folly::Baton<true, std::atomic> baton;
            kvstore_->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
                                    [&] (kvstore::ResultCode code) {
                if (code == kvstore::ResultCode::SUCCEEDED) {
                    succeeded = true;
                }
                baton.post();
            });
            baton.wait();
        }
    } else {
        succeeded = true;
    }

    if (!succeeded) {
        std::string flag;
        gflags::GetCommandLineOption("load_config_interval_secs", &flag);
        size_t delayMS = stoi(flag) * 1000 + folly::Random::rand32(900);
        bgThread_.addDelayTask(delayMS, &KVBasedGflagsManager::regCfgThreadFunc, this);
        LOG(INFO) << "Register fail, try it after " << delayMS << " ms ";
    }
}

}  // namespace meta
}  // namespace nebula

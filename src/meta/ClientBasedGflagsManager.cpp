/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ClientBasedGflagsManager.h"

namespace nebula {
namespace meta {

ClientBasedGflagsManager* ClientBasedGflagsManager::instance(MetaClient* client) {
    static auto gflagsMan = std::unique_ptr<ClientBasedGflagsManager>(
            new ClientBasedGflagsManager(client));
    static std::once_flag initFlag;
    std::call_once(initFlag, &ClientBasedGflagsManager::init, gflagsMan.get());
    return gflagsMan.get();
}

ClientBasedGflagsManager::ClientBasedGflagsManager(MetaClient *client) {
    metaClient_ = client;
    CHECK_NOTNULL(metaClient_);
}

ClientBasedGflagsManager::~ClientBasedGflagsManager() {
    if (nullptr != metaClient_) {
        metaClient_ = nullptr;
    }
}

void ClientBasedGflagsManager::init() {
    CHECK(bgThread_.start());
    declareGflags();
    loadCfgThreadFunc();
    regCfgThreadFunc();
}

template<typename ValueType>
folly::Future<Status> ClientBasedGflagsManager::set(const cpp2::ConfigModule& module,
                                                    const std::string& name,
                                                    const cpp2::ConfigType& type,
                                                    const ValueType& value) {
    std::string valueStr;
    valueStr.append(reinterpret_cast<const char*>(&value), sizeof(value));

    auto ret = metaClient_->setConfig(module, name, type, valueStr).get();
    if (ret.ok()) {
        return Status::OK();
    }
    return ret.status();
}

template<>
folly::Future<Status> ClientBasedGflagsManager::set<std::string>(const cpp2::ConfigModule& module,
                                                                 const std::string& name,
                                                                 const cpp2::ConfigType& type,
                                                                 const std::string& value) {
    auto ret = metaClient_->setConfig(module, name, type, value).get();
    if (ret.ok()) {
        return Status::OK();
    }
    return ret.status();
}

folly::Future<Status>
ClientBasedGflagsManager::setConfig(const cpp2::ConfigModule& module, folly::StringPiece name,
                                    const cpp2::ConfigType& type, const VariantType &value) {
    switch (type) {
        case cpp2::ConfigType::INT64:
            return set(module, name.str(), type, boost::get<int64_t>(value));
        case cpp2::ConfigType::DOUBLE:
            return set(module, name.str(), type, boost::get<double>(value));
        case cpp2::ConfigType::BOOL:
            return set(module, name.str(), type, boost::get<bool>(value));
        case cpp2::ConfigType::STRING:
            return set(module, name.str(), type, boost::get<std::string>(value));
        default:
            return Status::Error("parse value type error");
    }
}

folly::Future<StatusOr<std::vector<ConfigItem>>>
ClientBasedGflagsManager::getConfig(const cpp2::ConfigModule& module, folly::StringPiece name) {
    auto status = metaClient_->getConfig(module, name.str()).get();
    if (!status.ok()) {
        return status.status();
    }

    auto items = status.value();
    std::vector<ConfigItem> ret;
    ret.resize(items.size());
    std::transform(items.begin(), items.end(), ret.begin(), toConfigItem);
    return ret;
}

folly::Future<StatusOr<std::vector<ConfigItem>>>
ClientBasedGflagsManager::listConfigs(const cpp2::ConfigModule& module) {
    auto status = metaClient_->listConfigs(module).get();
    if (!status.ok()) {
        return status.status();
    }

    auto items = status.value();
    std::vector<ConfigItem> ret;
    ret.resize(items.size());
    std::transform(items.begin(), items.end(), ret.begin(), toConfigItem);
    return ret;
}

folly::Future<Status>
ClientBasedGflagsManager::registerConfig(const cpp2::ConfigModule& module, const std::string& name,
                                         const cpp2::ConfigType& type, const cpp2::ConfigMode& mode,
                                         const VariantType& value) {
    std::string valueStr;
    switch (type) {
        case cpp2::ConfigType::INT64: {
            int64_t val = boost::get<int64_t>(value);
            valueStr.append(reinterpret_cast<const char*>(&val), sizeof(val));
            break;
        }
        case cpp2::ConfigType::DOUBLE: {
            double val = boost::get<double>(value);
            valueStr.append(reinterpret_cast<const char*>(&val), sizeof(val));
            break;
        }
        case cpp2::ConfigType::BOOL: {
            bool val = boost::get<bool>(value);
            valueStr.append(reinterpret_cast<const char*>(&val), sizeof(val));
            break;
        }
        case cpp2::ConfigType::STRING: {
            valueStr = boost::get<std::string>(value);
            break;
        }
        default:
            return Status::Error("parse value type error");
    }

    auto item = toThriftConfigItem(module, name, type, mode, valueStr);
    std::vector<cpp2::ConfigItem> items;
    items.emplace_back(item);
    auto ret = metaClient_->regConfig(items).get();
    if (ret.ok()) {
        return Status::OK();
    }
    return ret.status();
}

void ClientBasedGflagsManager::loadCfgThreadFunc() {
    // only load current module's config is enough
    auto ret = listConfigs(module_).get();
    if (ret.ok()) {
        auto items = ret.value();
        updateConfigCache(items);
    } else {
        LOG(INFO) << "Load configs failed: " << ret.status();
    }

    std::string flag;
    gflags::GetCommandLineOption("load_config_interval_secs", &flag);
    size_t delayMS = stoi(flag) * 1000 + folly::Random::rand32(900);
    bgThread_.addDelayTask(delayMS, &ClientBasedGflagsManager::loadCfgThreadFunc, this);
    LOG(INFO) << "Load configs after " << delayMS << " ms";
}

void ClientBasedGflagsManager::regCfgThreadFunc() {
    auto status = metaClient_->regConfig(gflagsDeclared_).get();
    if (status.ok()) {
        LOG(INFO) << "Register flags ok " << gflagsDeclared_.size();
        return;
    }

    std::string flag;
    gflags::GetCommandLineOption("load_config_interval_secs", &flag);
    size_t delayMS = stoi(flag) * 1000 + folly::Random::rand32(900);
    bgThread_.addDelayTask(delayMS, &ClientBasedGflagsManager::regCfgThreadFunc, this);
    LOG(INFO) << "Register fail, try it after " << delayMS << " ms " << status.status();
}

}  // namespace meta
}  // namespace nebula

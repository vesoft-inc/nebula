/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ClientBasedGflagsManager.h"

DECLARE_int32(load_config_interval_secs);

namespace nebula {
namespace meta {

ClientBasedGflagsManager::ClientBasedGflagsManager(MetaClient *client) {
    metaClient_ = client;
    CHECK_NOTNULL(metaClient_);
}

ClientBasedGflagsManager::~ClientBasedGflagsManager() {
    metaClient_ = nullptr;
}

Status ClientBasedGflagsManager::init() {
    module_ = metaClient_->getGflagsModule();
    declareGflags();
    return registerGflags();
}

template<typename ValueType>
folly::Future<StatusOr<bool>> ClientBasedGflagsManager::set(const cpp2::ConfigModule& module,
                                                            const std::string& name,
                                                            const cpp2::ConfigType& type,
                                                            const ValueType& value) {
    std::string valueStr;
    valueStr.append(reinterpret_cast<const char*>(&value), sizeof(value));
    return metaClient_->setConfig(module, name, type, std::move(valueStr));
}

template<>
folly::Future<StatusOr<bool>>
ClientBasedGflagsManager::set<std::string>(const cpp2::ConfigModule& module,
                                           const std::string& name,
                                           const cpp2::ConfigType& type,
                                           const std::string& value) {
    return metaClient_->setConfig(module, name, type, value);
}

folly::Future<StatusOr<bool>>
ClientBasedGflagsManager::setConfig(const cpp2::ConfigModule& module, const std::string& name,
                                    const cpp2::ConfigType& type, const VariantType &value) {
    switch (type) {
        case cpp2::ConfigType::INT64:
            return set(module, name, type, boost::get<int64_t>(value));
        case cpp2::ConfigType::DOUBLE:
            return set(module, name, type, boost::get<double>(value));
        case cpp2::ConfigType::BOOL:
            return set(module, name, type, boost::get<bool>(value));
        case cpp2::ConfigType::STRING:
            return set(module, name, type, boost::get<std::string>(value));
        default:
            return Status::Error("parse value type error");
    }
}

folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>>
ClientBasedGflagsManager::getConfig(const cpp2::ConfigModule& module, const std::string& name) {
    return metaClient_->getConfig(module, name);
}

folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>>
ClientBasedGflagsManager::listConfigs(const cpp2::ConfigModule& module) {
    return metaClient_->listConfigs(module);
}

folly::Future<StatusOr<bool>>
ClientBasedGflagsManager::registerConfig(const cpp2::ConfigModule& module, const std::string& name,
                                         const cpp2::ConfigType& type, const cpp2::ConfigMode& mode,
                                         const std::string& value) {
    auto item = toThriftConfigItem(module, name, type, mode, value);
    std::vector<cpp2::ConfigItem> items;
    items.emplace_back(std::move(item));
    return metaClient_->regConfig(items);
}

Status ClientBasedGflagsManager::registerGflags() {
    auto status = metaClient_->regConfig(gflagsDeclared_).get();
    if (status.ok()) {
        LOG(INFO) << "Register gflags ok " << gflagsDeclared_.size();
        return Status::OK();
    }
    return Status::Error("Register gflags failed");
}

}  // namespace meta
}  // namespace nebula

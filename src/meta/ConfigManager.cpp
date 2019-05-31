/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ConfigManager.h"

DECLARE_int32(load_data_interval_secs);
DECLARE_int32(load_config_interval_secs);

namespace nebula {
namespace meta {

std::unique_ptr<ConfigManager> ConfigManager::instance_ = nullptr;

ConfigManager* ConfigManager::instance(MetaClient* client) {
    if (!instance_.get() && client != nullptr) {
        instance_ = std::unique_ptr<ConfigManager>(new ConfigManager(client));
        static std::once_flag initFlag;
        std::call_once(initFlag, &ConfigManager::init, instance_.get());
    }
    return instance_.get();
}

ConfigManager::ConfigManager(MetaClient *client) {
    metaClient_ = client;
    CHECK_NOTNULL(metaClient_);
}

ConfigManager::~ConfigManager() {
    if (nullptr != metaClient_) {
        metaClient_ = nullptr;
    }
}

void ConfigManager::init() {
    DECLARE("", cpp2::ConfigModule::META, "load_data_interval_secs", VariantTypeEnum::INT64,
            FLAGS_load_data_interval_secs);
    DECLARE("", cpp2::ConfigModule::META, "load_config_interval_secs", VariantTypeEnum::INT64,
            FLAGS_load_config_interval_secs);
}

template<typename ValueType>
folly::Future<Status> ConfigManager::set(const std::string& space,
                                         const cpp2::ConfigModule& module,
                                         const std::string& name,
                                         const cpp2::ConfigType& type,
                                         const ValueType& value) {
    std::string valueStr;
    valueStr.append(reinterpret_cast<const char*>(&value), sizeof(value));

    auto ret = metaClient_->setConfig(space, module, name, type, valueStr).get();
    if (ret.ok()) {
        return Status::OK();
    }
    return ret.status();
}

template<>
folly::Future<Status> ConfigManager::set<std::string>(const std::string& space,
                                                      const cpp2::ConfigModule& module,
                                                      const std::string& name,
                                                      const cpp2::ConfigType& type,
                                                      const std::string& value) {
    auto ret = metaClient_->setConfig(space, module, name, type, value).get();
    if (ret.ok()) {
        return Status::OK();
    }
    return ret.status();
}

folly::Future<Status>
ConfigManager::setConfig(folly::StringPiece space, const cpp2::ConfigModule& module,
                         folly::StringPiece name, const VariantType &value,
                         bool checkRegistered) {
    VariantTypeEnum type = static_cast<VariantTypeEnum>(value.which());
    if (checkRegistered) {
        auto status = metaClient_->isCfgRegistered(space.str(), module, name.str(), type);
        if (status.code() != Status::kCfgRegistered) {
            return status;
        }
    }

    auto cacheRet = metaClient_->setConfigToCache(space.str(), module, name.str(), value,
                                                  type);
    if (!cacheRet.ok()) {
        return cacheRet;
    }
    switch (type) {
        case VariantTypeEnum::INT64:
            return set(space.str(), module, name.str(), cpp2::ConfigType::INT64,
                       boost::get<int64_t>(value));
        case VariantTypeEnum::DOUBLE:
            return set(space.str(), module, name.str(), cpp2::ConfigType::DOUBLE,
                       boost::get<double>(value));
        case VariantTypeEnum::BOOL:
            return set(space.str(), module, name.str(), cpp2::ConfigType::BOOL,
                       boost::get<bool>(value));
        case VariantTypeEnum::STRING:
            return set(space.str(), module, name.str(), cpp2::ConfigType::STRING,
                       boost::get<std::string>(value));
        default:
            return Status::Error("parse value type error");
    }
}

folly::Future<Status>
ConfigManager::setConfig(folly::StringPiece space, const cpp2::ConfigModule& module,
                         folly::StringPiece name, const char* value,
                         bool checkRegistered) {
    std::string str = value;
    VariantType val = str;
    return setConfig(space, module, name, val, checkRegistered);
}

folly::Future<StatusOr<ConfigItem>>
ConfigManager::getConfig(folly::StringPiece space, const cpp2::ConfigModule& module,
                         folly::StringPiece name, const VariantTypeEnum& type) {
    cpp2::ConfigType cType;
    switch (type) {
        case VariantTypeEnum::INT64:
            cType = cpp2::ConfigType::INT64;
            break;
        case VariantTypeEnum::DOUBLE:
            cType = cpp2::ConfigType::DOUBLE;
            break;
        case VariantTypeEnum::BOOL:
            cType = cpp2::ConfigType::BOOL;
            break;
        case VariantTypeEnum::STRING:
            cType = cpp2::ConfigType::STRING;
            break;
        default:
            return Status::Error("parse value type error");
    }
    auto ret = metaClient_->getConfig(space.str(), module, name.str(), cType).get();
    if (ret.ok()) {
        return ret.value();
    }
    return ret.status();
}

folly::Future<StatusOr<std::vector<ConfigItem>>>
ConfigManager::listConfigs(folly::StringPiece space, const cpp2::ConfigModule& module) {
    return metaClient_->listConfigs(space.str(), module).get();
}

folly::Future<Status>
ConfigManager::registerConfig(const std::string& space, const cpp2::ConfigModule& module,
                              const std::string& name, const VariantTypeEnum& type,
                              const VariantType& defaultValue) {
    auto status = metaClient_->isCfgRegistered(space, module, name, type);
    if (status == Status::CfgNotFound()) {
        return setConfig(space, module, name, defaultValue, false).get();
    }
    return status;
}

folly::Future<Status>
ConfigManager::registerConfig(const std::string& space, const cpp2::ConfigModule& module,
                              const std::string& name, const VariantTypeEnum& type,
                              const char* defaultValue) {
    std::string str = defaultValue;
    VariantType val = str;
    return registerConfig(space, module, name, type, val);
}

Status ConfigManager::declare(const std::string& space, const cpp2::ConfigModule& module,
                              const std::string& name, const VariantTypeEnum& type,
                              const VariantType& defaultValue) {
    return metaClient_->setConfigToCache(space, module, name, defaultValue, type);
}

Status ConfigManager::declare(const std::string& space, const cpp2::ConfigModule& module,
                              const std::string& name, const VariantTypeEnum& type,
                              const int& defaultValue) {
    VariantType val = static_cast<int64_t>(defaultValue);
    return declare(space, module, name, type, val);
}

Status ConfigManager::declare(const std::string& space, const cpp2::ConfigModule& module,
                              const std::string& name, const VariantTypeEnum& type,
                              const char* defaultValue) {
    std::string str = defaultValue;
    VariantType val = str;
    return declare(space, module, name, type, val);
}

StatusOr<int64_t>
ConfigManager::getConfigAsInt64(folly::StringPiece space, const cpp2::ConfigModule& module,
                                folly::StringPiece name) {
    auto ret = metaClient_->getConfigFromCache(space.str(), module, name.str(),
                                               VariantTypeEnum::INT64);
    if (ret.ok()) {
        return boost::get<int64_t>(ret.value().value_);
    }
    return ret.status();
}

StatusOr<double>
ConfigManager::getConfigAsDouble(folly::StringPiece space, const cpp2::ConfigModule& module,
                                 folly::StringPiece name) {
    auto ret = metaClient_->getConfigFromCache(space.str(), module, name.str(),
                                               VariantTypeEnum::DOUBLE);
    if (ret.ok()) {
        return boost::get<double>(ret.value().value_);
    }
    return ret.status();
}

StatusOr<bool>
ConfigManager::getConfigAsBool(folly::StringPiece space, const cpp2::ConfigModule& module,
                               folly::StringPiece name) {
    auto ret = metaClient_->getConfigFromCache(space.str(), module, name.str(),
                                               VariantTypeEnum::BOOL);
    if (ret.ok()) {
        return boost::get<bool>(ret.value().value_);
    }
    return ret.status();
}

StatusOr<std::string>
ConfigManager::getConfigAsString(folly::StringPiece space, const cpp2::ConfigModule& module,
                                 folly::StringPiece name) {
    auto ret = metaClient_->getConfigFromCache(space.str(), module, name.str(),
                                               VariantTypeEnum::STRING);
    if (ret.ok()) {
        return boost::get<std::string>(ret.value().value_);
    }
    return ret.status();
}

}  // namespace meta
}  // namespace nebula

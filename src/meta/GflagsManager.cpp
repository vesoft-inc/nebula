/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/GflagsManager.h"

namespace nebula {
namespace meta {

const std::string kConfigUnknown = "UNKNOWN"; // NOLINT

GflagsManager* GflagsManager::instance(MetaClient* client) {
    static auto gflagsMan = std::unique_ptr<GflagsManager>(new GflagsManager(client));
    static std::once_flag initFlag;
    std::call_once(initFlag, &GflagsManager::init, gflagsMan.get());
    return gflagsMan.get();
}

GflagsManager::GflagsManager(MetaClient *client) {
    metaClient_ = client;
    CHECK_NOTNULL(metaClient_);
}

GflagsManager::~GflagsManager() {
    if (nullptr != metaClient_) {
        metaClient_ = nullptr;
    }
}

template<typename ValueType>
std::string GflagsManager::gflagsValueToThriftValue(const gflags::CommandLineFlagInfo& flag) {
    std::string ret;
    auto value = folly::to<ValueType>(flag.current_value);
    ret.append(reinterpret_cast<const char*>(&value), sizeof(ValueType));
    return ret;
}

template<>
std::string GflagsManager::gflagsValueToThriftValue<std::string>(
        const gflags::CommandLineFlagInfo& flag) {
    return flag.current_value;
}

void GflagsManager::init() {
    declareGflags();
    metaClient_->loadCfgThreadFunc(module_);
    metaClient_->regCfgThreadFunc(gflagsDeclared_);
}

void GflagsManager::declareGflags() {
    // get current process according to gflags pid_file
    std::string pid;
    if (gflags::GetCommandLineOption("pid_file", &pid)) {
        if (pid.find("nebula-graphd") != std::string::npos) {
            module_ = cpp2::ConfigModule::GRAPH;
        } else if (pid.find("nebula-storaged") != std::string::npos) {
            module_ = cpp2::ConfigModule::STORAGE;
        } else if (pid.find("nebula-metad") != std::string::npos) {
            module_ = cpp2::ConfigModule::META;
        } else {
            LOG(ERROR) << "Should not reach here";
        }
    } else {
        LOG(ERROR) << "Should not reach here";
    }

    // declare all gflags in GflagsManager
    std::vector<gflags::CommandLineFlagInfo> flags;
    gflags::GetAllFlags(&flags);
    for (auto& flag : flags) {
        auto& name = flag.name;
        auto& type = flag.type;
        cpp2::ConfigType cType;
        cpp2::ConfigMode mode = cpp2::ConfigMode::IMMUTABLE;
        VariantType defaultValue;
        std::string valueStr;

        // TODO: all int32 and uint32 are converted to int64
        if (type == "uint32" || type == "int32" || type == "int64") {
            cType = cpp2::ConfigType::INT64;
            defaultValue = folly::to<int64_t>(flag.current_value);
            valueStr = gflagsValueToThriftValue<int64_t>(flag);
        } else if (type == "double") {
            cType = cpp2::ConfigType::DOUBLE;
            defaultValue = folly::to<double>(flag.current_value);
        } else if (type == "bool") {
            cType = cpp2::ConfigType::BOOL;
            defaultValue = folly::to<bool>(flag.current_value);
            valueStr = gflagsValueToThriftValue<bool>(flag);
        } else if (type == "string") {
            cType = cpp2::ConfigType::STRING;
            defaultValue = flag.current_value;
            valueStr = gflagsValueToThriftValue<std::string>(flag);
        } else {
            LOG(INFO) << "Not able to declare " << name << " of " << type;
            continue;
        }

        if (name == "load_data_interval_secs" || name == "load_config_interval_secs") {
            mode = cpp2::ConfigMode::MUTABLE;
        }

        // declare all gflags in cache
        metaClient_->declareConfig(module_, name, cType, mode, defaultValue);
        gflagsDeclared_.emplace_back(
                toThriftConfigItem(module_, name, cType, mode, valueStr));
    }
}

template<typename ValueType>
folly::Future<Status> GflagsManager::set(const cpp2::ConfigModule& module,
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
folly::Future<Status> GflagsManager::set<std::string>(const cpp2::ConfigModule& module,
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
GflagsManager::setConfig(const cpp2::ConfigModule& module, folly::StringPiece name,
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

folly::Future<Status>
GflagsManager::setConfig(const cpp2::ConfigModule& module, folly::StringPiece name,
                         const cpp2::ConfigType& type, const char* value) {
    std::string str = value;
    VariantType val = str;
    return setConfig(module, name, type, val);
}

folly::Future<StatusOr<std::vector<ConfigItem>>>
GflagsManager::getConfig(const cpp2::ConfigModule& module, folly::StringPiece name) {
    auto ret = metaClient_->getConfig(module, name.str()).get();
    if (ret.ok()) {
        return ret.value();
    }
    return ret.status();
}

folly::Future<StatusOr<std::vector<ConfigItem>>>
GflagsManager::listConfigs(const cpp2::ConfigModule& module) {
    return metaClient_->listConfigs(module).get();
}

folly::Future<Status>
GflagsManager::registerConfig(const cpp2::ConfigModule& module, const std::string& name,
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

StatusOr<int64_t>
GflagsManager::getConfigAsInt64(folly::StringPiece name) {
    auto ret = metaClient_->getConfigFromCache(module_, name.str(), cpp2::ConfigType::INT64);
    if (ret.ok()) {
        return boost::get<int64_t>(ret.value().value_);
    }
    return ret.status();
}

StatusOr<double>
GflagsManager::getConfigAsDouble(folly::StringPiece name) {
    auto ret = metaClient_->getConfigFromCache(module_, name.str(), cpp2::ConfigType::DOUBLE);
    if (ret.ok()) {
        return boost::get<double>(ret.value().value_);
    }
    return ret.status();
}

StatusOr<bool>
GflagsManager::getConfigAsBool(folly::StringPiece name) {
    auto ret = metaClient_->getConfigFromCache(module_, name.str(), cpp2::ConfigType::BOOL);
    if (ret.ok()) {
        return boost::get<bool>(ret.value().value_);
    }
    return ret.status();
}

StatusOr<std::string>
GflagsManager::getConfigAsString(folly::StringPiece name) {
    auto ret = metaClient_->getConfigFromCache(module_, name.str(), cpp2::ConfigType::STRING);
    if (ret.ok()) {
        return boost::get<std::string>(ret.value().value_);
    }
    return ret.status();
}

Status GflagsManager::isCfgRegistered(const cpp2::ConfigModule& module, const std::string& name,
                                      const cpp2::ConfigType& type) {
    return metaClient_->isCfgRegistered(module, name, type);
}

cpp2::ConfigItem GflagsManager::toThriftConfigItem(const cpp2::ConfigModule& module,
                                                   const std::string& name,
                                                   const cpp2::ConfigType& type,
                                                   const cpp2::ConfigMode& mode,
                                                   const std::string& value) {
    cpp2::ConfigItem item;
    item.set_module(module);
    item.set_name(name);
    item.set_type(type);
    item.set_mode(mode);
    item.set_value(value);
    return item;
}

std::string ConfigModuleToString(const cpp2::ConfigModule& module) {
    auto it = cpp2::_ConfigModule_VALUES_TO_NAMES.find(module);
    if (it == cpp2::_ConfigModule_VALUES_TO_NAMES.end()) {
        return kConfigUnknown;
    } else {
        return it->second;
    }
}

std::string ConfigModeToString(const cpp2::ConfigMode& mode) {
    auto it = cpp2::_ConfigMode_VALUES_TO_NAMES.find(mode);
    if (it == cpp2::_ConfigMode_VALUES_TO_NAMES.end()) {
        return kConfigUnknown;
    } else {
        return it->second;
    }
}

std::string ConfigTypeToString(const cpp2::ConfigType& type) {
    auto it = cpp2::_ConfigType_VALUES_TO_NAMES.find(type);
    if (it == cpp2::_ConfigType_VALUES_TO_NAMES.end()) {
        return kConfigUnknown;
    } else {
        return it->second;
    }
}

}  // namespace meta
}  // namespace nebula

/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/GflagsManager.h"

namespace nebula {
namespace meta {

GflagsManager::~GflagsManager() {
    bgThread_.stop();
    bgThread_.wait();
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

    // declare all gflags in ClientBasedGflagsManager
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
        declareConfig(module_, name, cType, mode, defaultValue);
        gflagsDeclared_.emplace_back(toThriftConfigItem(module_, name, cType, mode, valueStr));
    }
}

StatusOr<int64_t> GflagsManager::getConfigAsInt64(folly::StringPiece name) {
    auto ret = getConfigFromCache(module_, name.str(), cpp2::ConfigType::INT64);
    if (ret.ok()) {
        return boost::get<int64_t>(ret.value().value_);
    }
    return ret.status();
}

StatusOr<double> GflagsManager::getConfigAsDouble(folly::StringPiece name) {
    auto ret = getConfigFromCache(module_, name.str(), cpp2::ConfigType::DOUBLE);
    if (ret.ok()) {
        return boost::get<double>(ret.value().value_);
    }
    return ret.status();
}

StatusOr<bool> GflagsManager::getConfigAsBool(folly::StringPiece name) {
    auto ret = getConfigFromCache(module_, name.str(), cpp2::ConfigType::BOOL);
    if (ret.ok()) {
        return boost::get<bool>(ret.value().value_);
    }
    return ret.status();
}

StatusOr<std::string> GflagsManager::getConfigAsString(folly::StringPiece name) {
    auto ret = getConfigFromCache(module_, name.str(), cpp2::ConfigType::STRING);
    if (ret.ok()) {
        return boost::get<std::string>(ret.value().value_);
    }
    return ret.status();
}

StatusOr<ConfigItem> GflagsManager::getConfigFromCache(const cpp2::ConfigModule& module,
                                                       const std::string& name,
                                                       const cpp2::ConfigType& type) {
    if (!configReady_) {
        return Status::Error("Config cache not ready!");
    }
    {
        folly::RWSpinLock::ReadHolder holder(configCacheLock_);
        auto it = metaConfigMap_.find({module, name});
        if (it != metaConfigMap_.end()) {
            if (it->second.type_ != type) {
                return Status::CfgErrorType();
            }
            return it->second;
        }
        return Status::CfgNotFound();
    }
}

Status GflagsManager::declareConfig(const cpp2::ConfigModule& module,
                                    const std::string& name,
                                    const cpp2::ConfigType& type,
                                    const cpp2::ConfigMode& mode,
                                    const VariantType& value) {
    {
        folly::RWSpinLock::WriteHolder holder(configCacheLock_);
        std::pair<cpp2::ConfigModule, std::string> key = {module, name};
        ConfigItem item(module, name, type, mode, value);
        metaConfigMap_[key] = item;
        return Status::OK();
    }
}

Status GflagsManager::isCfgRegistered(const cpp2::ConfigModule& module, const std::string& name,
                                      const cpp2::ConfigType& type) {
    if (!configReady_) {
        return Status::Error("Config cache not ready!");
    }

    std::pair<cpp2::ConfigModule, std::string> key = {module, name};
    {
        folly::RWSpinLock::ReadHolder holder(configCacheLock_);
        auto it = metaConfigMap_.find(key);
        if (it == metaConfigMap_.end()) {
            return Status::CfgNotFound();
        } else if (it->second.type_ != type) {
            return Status::CfgErrorType();
        }
    }
    return Status::CfgRegistered();
}

void GflagsManager::updateConfigCache(const std::vector<ConfigItem>& items) {
    MetaConfigMap metaConfigMap;
    for (auto& item : items) {
        std::pair<cpp2::ConfigModule, std::string> key = {item.module_, item.name_};
        metaConfigMap.emplace(std::move(key), std::move(item));
    }
    {
        // For any configurations that is in meta, update in cache to replace previous value
        folly::RWSpinLock::WriteHolder holder(configCacheLock_);
        for (const auto& entry : metaConfigMap) {
            auto& key = entry.first;
            if (metaConfigMap_.find(key) == metaConfigMap_.end() ||
                    metaConfigMap[key].value_ != metaConfigMap_[key].value_) {
                updateGflagsValue(entry.second);
                LOG(INFO) << "update config in cache " << key.second
                          << " to " << metaConfigMap[key].value_;
                metaConfigMap_[key] = entry.second;
            }
        }
    }
    configReady_ = true;
}

void GflagsManager::updateGflagsValue(const ConfigItem& item) {
    if (item.mode_ != cpp2::ConfigMode::MUTABLE) {
        return;
    }

    std::string metaValue;
    switch (item.type_) {
        case cpp2::ConfigType::INT64:
            metaValue = folly::to<std::string>(boost::get<int64_t>(item.value_));
            break;
        case cpp2::ConfigType::DOUBLE:
            metaValue = folly::to<std::string>(boost::get<double>(item.value_));
            break;
        case cpp2::ConfigType::BOOL:
            metaValue = boost::get<bool>(item.value_) ? "true" : "false";
            break;
        case cpp2::ConfigType::STRING:
            metaValue = boost::get<std::string>(item.value_);
            break;
    }

    std::string curValue;
    if (!gflags::GetCommandLineOption(item.name_.c_str(), &curValue)) {
        return;
    } else if (curValue != metaValue) {
        LOG(INFO) << "update " << item.name_ << " from " << curValue << " to " << metaValue;
        gflags::SetCommandLineOption(item.name_.c_str(), metaValue.c_str());
    }
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

ConfigItem GflagsManager::toConfigItem(const cpp2::ConfigItem& item) {
    VariantType value;
    switch (item.get_type()) {
        case cpp2::ConfigType::INT64:
            value = *reinterpret_cast<const int64_t*>(item.get_value().data());
            break;
        case cpp2::ConfigType::BOOL:
            value = *reinterpret_cast<const bool*>(item.get_value().data());
            break;
        case cpp2::ConfigType::DOUBLE:
            value = *reinterpret_cast<const double*>(item.get_value().data());
            break;
        case cpp2::ConfigType::STRING:
            value = item.get_value();
            break;
    }
    return ConfigItem(item.get_module(), item.get_name(), item.get_type(), item.get_mode(), value);
}

}  // namespace meta
}  // namespace nebula


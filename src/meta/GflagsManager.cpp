/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/GflagsManager.h"
#include "base/Configuration.h"
#include "fs/FileUtils.h"

DEFINE_string(gflags_mode_json, "share/resources/gflags.json", "gflags mode json for service");

namespace nebula {
namespace meta {

template <typename ValueType>
std::string GflagsManager::gflagsValueToThriftValue(const gflags::CommandLineFlagInfo& flag) {
    std::string ret;
    auto value = folly::to<ValueType>(flag.current_value);
    ret.append(reinterpret_cast<const char*>(&value), sizeof(ValueType));
    return ret;
}

template <>
std::string GflagsManager::gflagsValueToThriftValue<std::string>(
        const gflags::CommandLineFlagInfo& flag) {
    return flag.current_value;
}

std::unordered_map<std::string, std::pair<cpp2::ConfigMode, bool>>
GflagsManager::parseConfigJson(const std::string& path) {
    std::unordered_map<std::string, std::pair<cpp2::ConfigMode, bool>> configModeMap;
    Configuration conf;
    if (!conf.parseFromFile(path).ok()) {
        LOG(ERROR) << "Load gflags json failed";
        return configModeMap;
    }
    static std::vector<std::string> keys = {"MUTABLE"};
    static std::vector<cpp2::ConfigMode> modes = {cpp2::ConfigMode::MUTABLE};
    for (size_t i = 0; i < keys.size(); i++) {
        std::vector<std::string> values;
        if (!conf.fetchAsStringArray(keys[i].c_str(), values).ok()) {
            continue;
        }
        cpp2::ConfigMode mode = modes[i];
        for (const auto& name : values) {
            configModeMap[name] = {mode, false};
        }
    }
    static std::string nested = "NESTED";
    std::vector<std::string> values;
    if (conf.fetchAsStringArray(nested.c_str(), values).ok()) {
        for (const auto& name : values) {
            // all nested gflags regard as mutable ones
            configModeMap[name] = {cpp2::ConfigMode::MUTABLE, true};
        }
    }

    return configModeMap;
}

std::vector<cpp2::ConfigItem> GflagsManager::declareGflags(const cpp2::ConfigModule& module) {
    std::vector<cpp2::ConfigItem> configItems;
    if (module == cpp2::ConfigModule::UNKNOWN) {
        return configItems;
    }
    auto mutableConfig = parseConfigJson(FLAGS_gflags_mode_json);
    // Get all flags by listing all defined gflags
    std::vector<gflags::CommandLineFlagInfo> flags;
    gflags::GetAllFlags(&flags);
    for (auto& flag : flags) {
        auto& name = flag.name;
        auto& type = flag.type;
        cpp2::ConfigType cType;
        VariantType value;
        std::string valueStr;

        // We only register mutable configs to meta
        cpp2::ConfigMode mode = cpp2::ConfigMode::MUTABLE;
        bool isNested = false;
        auto iter = mutableConfig.find(name);
        if (iter != mutableConfig.end()) {
            isNested = iter->second.second;
        } else {
            continue;
        }

        // TODO: all int32/uint32/uint64 gflags are converted to int64 for now
        if (type == "uint32" || type == "int32" || type == "int64" || type == "uint64") {
            cType = cpp2::ConfigType::INT64;
            value = folly::to<int64_t>(flag.current_value);
            valueStr = gflagsValueToThriftValue<int64_t>(flag);
        } else if (type == "double") {
            cType = cpp2::ConfigType::DOUBLE;
            value = folly::to<double>(flag.current_value);
        } else if (type == "bool") {
            cType = cpp2::ConfigType::BOOL;
            value = folly::to<bool>(flag.current_value);
            valueStr = gflagsValueToThriftValue<bool>(flag);
        } else if (type == "string") {
            cType = cpp2::ConfigType::STRING;
            value = flag.current_value;
            valueStr = gflagsValueToThriftValue<std::string>(flag);
            // only string gflags can be nested
            if (isNested) {
                cType = cpp2::ConfigType::NESTED;
            }
        } else {
            LOG(INFO) << "Not able to declare " << name << " of " << type;
            continue;
        }

        configItems.emplace_back(toThriftConfigItem(module, name, cType, mode, valueStr));
    }
    LOG(INFO) << "Prepare to register " << configItems.size() << " gflags to meta";
    return configItems;
}

void GflagsManager::getGflagsModule(cpp2::ConfigModule& gflagsModule) {
    // get current process according to gflags pid_file
    gflags::CommandLineFlagInfo pid;
    if (gflags::GetCommandLineFlagInfo("pid_file", &pid)) {
        auto defaultPid = pid.default_value;
        if (defaultPid.find("nebula-graphd") != std::string::npos) {
            gflagsModule = cpp2::ConfigModule::GRAPH;
        } else if (defaultPid.find("nebula-storaged") != std::string::npos) {
            gflagsModule = cpp2::ConfigModule::STORAGE;
        } else if (defaultPid.find("nebula-metad") != std::string::npos) {
            gflagsModule = cpp2::ConfigModule::META;
        } else {
            LOG(ERROR) << "Should not reach here";
        }
    } else {
        LOG(INFO) << "Unknown config module";
    }
}

std::string toThriftValueStr(const cpp2::ConfigType& type, const VariantType& value) {
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
        case cpp2::ConfigType::STRING:
        case cpp2::ConfigType::NESTED: {
            valueStr = boost::get<std::string>(value);
            break;
        }
    }
    return valueStr;
}

cpp2::ConfigItem toThriftConfigItem(const cpp2::ConfigModule& module,
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

}   // namespace meta
}   // namespace nebula

/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/GflagsManager.h"

DEFINE_int32(load_config_interval_secs, 2 * 60, "Load config interval");

namespace nebula {
namespace meta {

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
    // declare all gflags in ClientBasedGflagsManager
    std::vector<gflags::CommandLineFlagInfo> flags;
    gflags::GetAllFlags(&flags);
    for (auto& flag : flags) {
        auto& name = flag.name;
        auto& type = flag.type;
        cpp2::ConfigType cType;
        // default config type will take effect after reboot
        cpp2::ConfigMode mode = cpp2::ConfigMode::REBOOT;
        VariantType value;
        std::string valueStr;

        // TODO: all int32 and uint32 are converted to int64
        if (type == "uint32" || type == "int32" || type == "int64") {
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
        } else {
            LOG(INFO) << "Not able to declare " << name << " of " << type;
            continue;
        }

        if (name == "load_data_interval_secs" || name == "load_config_interval_secs") {
            mode = cpp2::ConfigMode::MUTABLE;
        }
        if (module_ == cpp2::ConfigModule::META) {
            // all config of meta is immutable for now
            mode = cpp2::ConfigMode::IMMUTABLE;
        }

        LOG(INFO) << name << " " << value;
        gflagsDeclared_.emplace_back(toThriftConfigItem(module_, name, cType, mode, valueStr));
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
        case cpp2::ConfigType::STRING: {
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

}  // namespace meta
}  // namespace nebula


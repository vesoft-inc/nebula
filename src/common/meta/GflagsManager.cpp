/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/meta/GflagsManager.h"
#include "common/conf/Configuration.h"
#include "common/fs/FileUtils.h"

DEFINE_string(gflags_mode_json, "share/resources/gflags.json", "gflags mode json for service");

namespace nebula {
namespace meta {

Value GflagsManager::gflagsValueToValue(const std::string &type, const std::string &flagValue) {
    // all int32/uint32/uint64 gflags are converted to int64 for now
    folly::StringPiece view(type);
    if (view.startsWith("int") || view.startsWith("uint")) {
        return Value(folly::to<int64_t>(flagValue));
    } else if (type == "double") {
        return Value(folly::to<double>(flagValue));
    } else if (type == "bool") {
        return Value(folly::to<bool>(flagValue));
    } else if (type == "string") {
        return Value(folly::to<std::string>(flagValue));
    } else if (type == "map") {
        auto value = Value(folly::to<std::string>(flagValue));
        VLOG(1) << "Nested value: " << value;
        // transform to map value
        conf::Configuration conf;
        auto status = conf.parseFromString(value.getStr());
        if (!status.ok()) {
            LOG(ERROR) << "Parse value: " << value
                        << " failed: " << status;
            return Value::kNullValue;
        }
        Map map;
        conf.forEachItem([&map] (const std::string& key, const folly::dynamic& val) {
            map.kvs.emplace(key, val.asString());
        });
        value.setMap(std::move(map));
        return value;
    }
    LOG(WARNING) << "Unknown type: " << type;
    return Value::kEmpty;
}

std::unordered_map<std::string, std::pair<cpp2::ConfigMode, bool>>
GflagsManager::parseConfigJson(const std::string& path) {
    // The default conf for gflags flags mode
    std::unordered_map<std::string, std::pair<cpp2::ConfigMode, bool>> configModeMap {
        {"max_edge_returned_per_vertex", {cpp2::ConfigMode::MUTABLE, false}},
        {"minloglevel", {cpp2::ConfigMode::MUTABLE, false}},
        {"v", {cpp2::ConfigMode::MUTABLE, false}},
        {"heartbeat_interval_secs", {cpp2::ConfigMode::MUTABLE, false}},
        {"meta_client_retry_times", {cpp2::ConfigMode::MUTABLE, false}},
        {"slow_op_threshhold_ms", {cpp2::ConfigMode::MUTABLE, false}},
        {"wal_ttl", {cpp2::ConfigMode::MUTABLE, false}},
        {"clean_wal_interval_secs", {cpp2::ConfigMode::MUTABLE, false}},
        {"custom_filter_interval_secs", {cpp2::ConfigMode::MUTABLE, false}},
        {"accept_partial_success", {cpp2::ConfigMode::MUTABLE, false}},

        {"rocksdb_db_options", {cpp2::ConfigMode::MUTABLE, true}},
        {"rocksdb_column_family_options", {cpp2::ConfigMode::MUTABLE, true}},
        {"rocksdb_block_based_table_options", {cpp2::ConfigMode::MUTABLE, true}},
    };
    conf::Configuration conf;
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
        auto type = flag.type;

        // We only register mutable configs to meta
        cpp2::ConfigMode mode = cpp2::ConfigMode::MUTABLE;
        auto iter = mutableConfig.find(name);
        if (iter != mutableConfig.end()) {
            // isNested
            if (iter->second.second) {
                type = "map";
            }
        } else {
            continue;
        }

        Value value = gflagsValueToValue(type, flag.current_value);
        if (value.empty()) {
            LOG(INFO) << "Not able to declare " << name << " of " << flag.type;
            continue;
        }
        if (value.isNull()) {
            LOG(ERROR) << "Parse gflags: " << name
                       << ", value: " << flag.current_value
                       << " failed.";
            continue;
        }
        cpp2::ConfigItem item;
        item.set_name(name);
        item.set_module(module);
        item.set_mode(mode);
        item.set_value(std::move(value));
        configItems.emplace_back(std::move(item));
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

std::string GflagsManager::ValueToGflagString(const Value &val) {
    switch (val.type()) {
        case Value::Type::BOOL: {
            return val.getBool() ? "true" : "false";
        }
        case Value::Type::INT: {
            return folly::to<std::string>(val.getInt());
        }
        case Value::Type::FLOAT: {
            return folly::to<std::string>(val.getFloat());
        }
        case Value::Type::STRING: {
            return val.getStr();
        }
        case Value::Type::MAP: {
            auto& kvs = val.getMap().kvs;
            std::vector<std::string> values(kvs.size());
            std::transform(kvs.begin(), kvs.end(), values.begin(),
                    [](const auto &iter) -> std::string {
                        std::stringstream out;
                        out << "\"" << iter.first << "\"" << ":" << "\"" << iter.second << "\"";
                        return out.str();
                    });

            std::stringstream os;
            os << "{" << folly::join(",", values) << "}";
            return os.str();
        }
        default: {
            LOG(FATAL) << "Unsupported type for gflags";
        }
    }
}
}   // namespace meta
}   // namespace nebula

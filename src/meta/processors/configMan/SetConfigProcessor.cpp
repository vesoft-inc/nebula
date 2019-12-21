/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/configMan/SetConfigProcessor.h"
#include "base/Configuration.h"

namespace nebula {
namespace meta {

std::unordered_set<std::string> SetConfigProcessor::mutableFields_ = {
    // rocksdb_column_family_options
    "disable_auto_compactions",
    // TODO: write_buffer_size will cause rocksdb crash
    // "write_buffer_size",
    "max_write_buffer_number",
    "level0_file_num_compaction_trigger",
    "level0_slowdown_writes_trigger",
    "level0_stop_writes_trigger",
    "target_file_size_base",
    "target_file_size_multiplier",
    "max_bytes_for_level_base",
    "max_bytes_for_level_multiplier",
    "ttl",

    // rocksdb_db_options
    "max_total_wal_size",
    "delete_obsolete_files_period_micros",
    "max_background_jobs",
    "base_background_compactions",
    "max_background_compactions",
    "stats_dump_period_sec",
    "compaction_readahead_size",
    "writable_file_max_buffer_size",
    "bytes_per_sync",
    "wal_bytes_per_sync",
    "delayed_write_rate",
    "avoid_flush_during_shutdown",
    "max_open_files"
};

void SetConfigProcessor::process(const cpp2::SetConfigReq& req) {
    std::vector<kvstore::KV> data;
    auto module = req.get_item().get_module();
    auto name = req.get_item().get_name();
    auto type = req.get_item().get_type();
    auto value = req.get_item().get_value();
    auto isForce = req.get_force();

    folly::SharedMutex::WriteHolder wHolder(LockUtils::configLock());
    cpp2::ErrorCode code = cpp2::ErrorCode::SUCCEEDED;

    do {
        if (type != cpp2::ConfigType::NESTED) {
            if (module != cpp2::ConfigModule::ALL) {
                // When we set config of a specified module, check if it exists.
                // If it exists and is mutable, update it.
                code = setOneConfig(module, name, type, value, isForce, data);
                if (code != cpp2::ErrorCode::SUCCEEDED) {
                    break;
                }
            } else {
                // When we set config of all module, then try to set it of every module.
                code = setOneConfig(cpp2::ConfigModule::GRAPH, name, type, value, isForce, data);
                if (code != cpp2::ErrorCode::SUCCEEDED) {
                    break;
                }
                code = setOneConfig(cpp2::ConfigModule::STORAGE, name, type, value, isForce, data);
                if (code != cpp2::ErrorCode::SUCCEEDED) {
                    break;
                }
            }
        } else {
            // For those nested options like FLAGS_rocksdb_db_options, if any field has changed,
            // we update them and put it back
            if (module != cpp2::ConfigModule::ALL) {
                code = setNestedConfig(module, name, type, value, data);
                if (code != cpp2::ErrorCode::SUCCEEDED) {
                    break;
                }
            } else {
                code = setNestedConfig(cpp2::ConfigModule::GRAPH, name, type, value, data);
                if (code != cpp2::ErrorCode::SUCCEEDED) {
                    break;
                }
                code = setNestedConfig(cpp2::ConfigModule::STORAGE, name, type, value, data);
                if (code != cpp2::ErrorCode::SUCCEEDED) {
                    break;
                }
            }
        }

        if (!data.empty()) {
            doPut(std::move(data));
        }
        return;
    } while (false);

    resp_.set_code(code);
    onFinished();
}

cpp2::ErrorCode SetConfigProcessor::setOneConfig(const cpp2::ConfigModule& module,
                                                 const std::string& name,
                                                 const cpp2::ConfigType& type,
                                                 const std::string& value,
                                                 const bool isForce,
                                                 std::vector<kvstore::KV>& data) {
    std::string configKey = MetaServiceUtils::configKey(module, name);
    auto ret = doGet(std::move(configKey));
    if (!ret.ok()) {
        return cpp2::ErrorCode::E_NOT_FOUND;
    }

    cpp2::ConfigItem item = MetaServiceUtils::parseConfigValue(ret.value());
    cpp2::ConfigMode curMode = item.get_mode();
    if (curMode == cpp2::ConfigMode::IMMUTABLE && !isForce) {
        return cpp2::ErrorCode::E_CONFIG_IMMUTABLE;
    }
    std::string configValue = MetaServiceUtils::configValue(type, curMode, value);
    data.emplace_back(std::move(configKey), std::move(configValue));
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode SetConfigProcessor::setNestedConfig(const cpp2::ConfigModule& module,
                                                    const std::string& name,
                                                    const cpp2::ConfigType& type,
                                                    const std::string& updateList,
                                                    std::vector<kvstore::KV>& data) {
    std::string configKey = MetaServiceUtils::configKey(module, name);
    auto ret = doGet(std::move(configKey));
    if (!ret.ok()) {
        return cpp2::ErrorCode::E_NOT_FOUND;
    }

    cpp2::ConfigItem item = MetaServiceUtils::parseConfigValue(ret.value());
    cpp2::ConfigMode curMode = item.get_mode();
    if (curMode == cpp2::ConfigMode::IMMUTABLE) {
        return cpp2::ErrorCode::E_CONFIG_IMMUTABLE;
    }

    Configuration conf;
    auto confRet = conf.parseFromString(item.get_value());
    CHECK(confRet.ok());

    std::vector<std::string> updateFields;
    folly::split(",", updateList, updateFields, true);
    bool updated = false;
    for (const auto& field : updateFields) {
        auto pos = field.find("=");
        if (pos == std::string::npos) {
            LOG(ERROR) << "Should not reach here";
            continue;
        }
        auto key = field.substr(0, pos);
        auto value = field.substr(pos + 1);
        // TODO: Maybe need to handle illegal value here
        if (mutableFields_.count(key) && conf.updateStringField(key.c_str(), value).ok()) {
            updated = true;
        } else {
            LOG(ERROR) << "Unsupported configs " << key;
            return cpp2::ErrorCode::E_NOT_FOUND;
        }
    }

    if (updated) {
        std::string configValue = MetaServiceUtils::configValue(type, curMode, conf.dumpToString());
        data.emplace_back(std::move(configKey), std::move(configValue));
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula

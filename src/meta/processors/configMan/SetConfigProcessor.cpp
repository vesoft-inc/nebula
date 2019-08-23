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
    "snap_refresh_nanos",
    "disable_auto_compactions",
    "write_buffer_size",
    "compression",
    "level0_file_num_compaction_trigger",
    "max_bytes_for_level_base",
    "snap_refresh_nanos",
    "block_size",
    "block_restart_interval"
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
    auto mode = req.get_item().get_mode();
    auto value = req.get_item().get_value();

    folly::SharedMutex::WriteHolder wHolder(LockUtils::configLock());
    if (type != cpp2::ConfigType::NESTED) {
        if (module != cpp2::ConfigModule::ALL) {
            // When we set config of a specified module, check if it exists.
            // If it exists and is mutable, update it.
            setOneConfig(module, name, type, mode, value, data);
        } else {
            // When we set config of all module, then try to set it of every module.
            setOneConfig(cpp2::ConfigModule::GRAPH, name, type, mode, value, data);
            setOneConfig(cpp2::ConfigModule::META, name, type, mode, value, data);
            setOneConfig(cpp2::ConfigModule::STORAGE, name, type, mode, value, data);
        }
    } else {
        // For those nested options like FLAGS_rocksdb_db_options, if any field has changed,
        // we update them and put it back
        if (module != cpp2::ConfigModule::ALL) {
            setNestedConfig(module, name, type, mode, value, data);
        } else {
            setNestedConfig(cpp2::ConfigModule::GRAPH, name, type, mode, value, data);
            setNestedConfig(cpp2::ConfigModule::META, name, type, mode, value, data);
            setNestedConfig(cpp2::ConfigModule::STORAGE, name, type, mode, value, data);
        }
    }

    if (!data.empty()) {
        doPut(std::move(data));
    } else {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
    }
}

void SetConfigProcessor::setOneConfig(const cpp2::ConfigModule& module, const std::string& name,
                                      const cpp2::ConfigType& type, const cpp2::ConfigMode& mode,
                                      const std::string& value, std::vector<kvstore::KV>& data) {
    std::string configKey = MetaServiceUtils::configKey(module, name);
    auto ret = doGet(std::move(configKey));
    if (!ret.ok()) {
        return;
    }

    cpp2::ConfigItem item = MetaServiceUtils::parseConfigValue(ret.value());
    if (item.get_mode() == cpp2::ConfigMode::IMMUTABLE) {
        return;
    }
    std::string configValue = MetaServiceUtils::configValue(type, mode, value);
    data.emplace_back(std::move(configKey), std::move(configValue));
}

void SetConfigProcessor::setNestedConfig(const cpp2::ConfigModule& module, const std::string& name,
                                         const cpp2::ConfigType& type, const cpp2::ConfigMode& mode,
                                         const std::string& updateList,
                                         std::vector<kvstore::KV>& data) {
    std::string configKey = MetaServiceUtils::configKey(module, name);
    auto ret = doGet(std::move(configKey));
    if (!ret.ok()) {
        return;
    }

    cpp2::ConfigItem current = MetaServiceUtils::parseConfigValue(ret.value());
    if (current.get_mode() == cpp2::ConfigMode::IMMUTABLE) {
        return;
    }

    Configuration conf;
    auto confRet = conf.parseFromString(current.get_value());
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
        auto key = field.substr(0, pos).c_str();
        auto value = field.substr(pos + 1);
        // TODO: Maybe need to handle illegal value here
        if (mutableFields_.count(key) && conf.updateStringField(key, value).ok()) {
            updated = true;
        }
    }

    if (updated) {
        std::string configValue = MetaServiceUtils::configValue(type, mode, conf.dumpToString());
        data.emplace_back(std::move(configKey), std::move(configValue));
    }
}

}  // namespace meta
}  // namespace nebula

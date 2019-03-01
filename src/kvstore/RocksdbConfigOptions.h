/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef NEBULA_GRAPH_ROCKSDBCONFIGOPTIONS_H
#define NEBULA_GRAPH_ROCKSDBCONFIGOPTIONS_H

#include "base/Base.h"
#include "rocksdb/db.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/slice_transform.h"
#include "kvstore/Common.h"

#define KVSTORE_CONFIG_FLAG(engine_flag) \
  { ""#engine_flag"", FLAGS_##engine_flag }


namespace nebula {
namespace kvstore {
static std::unordered_map<std::string, std::string> options_map_database;
static std::unordered_map<std::string, std::string> options_map_columnfamilie;
static std::unordered_map<std::string, std::string> options_map_blockbasedtable;
class RocksdbConfigOptions final {
public:
    enum ROCKSDB_OPTION_TYPE {
        DBOptions = 1,
        CFOptions,
        TableOptions };

    RocksdbConfigOptions(const KV_paths rocksdb_paths,
                       int nebula_space_num,
                       bool ignore_unknown_options,
                       bool input_strings_escaped);

    ~RocksdbConfigOptions();

    static bool getRocksdbEngineOptionValue(ROCKSDB_OPTION_TYPE opt_type,
            const char *opt_name, std::string &opt_value);
    static bool getKVPaths(std::string data_paths_str,
            std::string wal_paths_str, KV_paths &rocksdb_paths);
    rocksdb::Status createRocksdbEngineOptions(rocksdb::Options &options);

private:
    rocksdb::Options base_opts_;
    rocksdb::DBOptions db_opts_;
    rocksdb::ColumnFamilyOptions cf_opts_;
    rocksdb::BlockBasedTableOptions bbt_opts_;
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs_;
    const KV_paths dataPaths_;
    int nebula_space_num_;
    bool ignore_unknown_options_;
    bool input_strings_escaped_;

private:
    rocksdb::Status initRocksdbOptions();
    rocksdb::Status checkOptionsCompatibility();
    bool setup_memtable_factory();
    bool setup_compaction_filter_factory();
    bool setup_prefix_extractor();
    bool setup_comparator();
    bool setup_merge_operator();
    bool setup_compaction_filter();
    bool setup_block_cache();
    void load_option_maps();
};
}  // namespace kvstore
}  // namespace nebula
#endif  // NEBULA_GRAPH_ROCKSDBCONFIGOPTIONS_H

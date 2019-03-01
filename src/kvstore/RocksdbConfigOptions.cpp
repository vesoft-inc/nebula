/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "kvstore/RocksdbConfigOptions.h"

#define ROCKSDB_OPTIONS_TYPE_DB  "Rocksdb:DBOptions"
#define ROCKSDB_OPTIONS_TYPE_CF  "Rocksdb:CFOptions"
#define ROCKSDB_OPTIONS_TYPE_BBT "Rocksdb:BlockBasedTable"

// [Version]

DEFINE_string(rocksdb_options_version,
              "5.15.10",
              "rocksdb options version.");
// [DBOptions]
/*
 * Rocksdb
 *
 */
DEFINE_string(manual_wal_flush,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(allow_ingest_behind,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(avoid_flush_during_shutdown,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(avoid_flush_during_recovery,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(info_log_level,
              "DEBUG_LEVEL",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(access_hint_on_compaction_start,
              "NORMAL",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(write_thread_max_yield_usec,
              "100",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(write_thread_slow_yield_usec,
              "3",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(wal_recovery_mode,
              "kPointInTimeRecovery",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(allow_concurrent_memtable_write,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(enable_pipelined_write,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(fail_if_options_file_error,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(stats_dump_period_sec,
              "600",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(wal_bytes_per_sync,
              "0",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(max_total_wal_size,
              "0",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(delayed_write_rate,
              "16777216",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(two_write_queues,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(bytes_per_sync,
              "0",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(writable_file_max_buffer_size,
              "1048576",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(wal_dir,
              "",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(log_file_time_to_roll,
              "0",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(keep_log_file_num,
              "1000",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(WAL_ttl_seconds,
              "0",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(db_write_buffer_size,
              "0",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(table_cache_numshardbits,
              "6",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(max_open_files,
              "-1",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(max_file_opening_threads,
              "16",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(WAL_size_limit_MB,
              "0",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(max_background_flushes,
              "-1",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(db_log_dir,
              "",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(max_background_compactions,
              "-1",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(max_subcompactions,
              "1",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(max_background_jobs,
              "2",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(random_access_max_buffer_size,
              "1048576",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(delete_obsolete_files_period_micros,
              "21600000000",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(skip_stats_update_on_db_open,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(skip_log_error_on_recovery,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(dump_malloc_stats,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(paranoid_checks,
              "true",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(is_fd_close_on_exec,
              "true",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(max_manifest_file_size,
              "1073741824",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(error_if_exists,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(use_adaptive_mutex,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(enable_thread_tracking,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(create_missing_column_families,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(create_if_missing,
              "true",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(manifest_preallocation_size,
              "4194304",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(base_background_compactions,
              "-1",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(use_fsync,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(allow_2pc,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(recycle_log_file_num,
              "0",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(use_direct_io_for_flush_and_compaction,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(compaction_readahead_size,
              "0",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(use_direct_reads,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(allow_mmap_writes,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(preserve_deletes,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(enable_write_thread_adaptive_yield,
              "true",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(max_log_file_size,
              "0",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(allow_fallocate,
              "true",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(allow_mmap_reads,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(new_table_reader_for_compaction_inputs,
              "false",
              ROCKSDB_OPTIONS_TYPE_DB);

DEFINE_string(advise_random_on_open,
              "true",
              ROCKSDB_OPTIONS_TYPE_DB);

// [CFOptions "default"]

DEFINE_string(compaction_pri,
              "kByCompensatedSize",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(merge_operator,
              "nullptr",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(compaction_filter_factory,
              "KeepFilterFactory",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(memtable_insert_with_hint_prefix_extractor,
              "nullptr",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(comparator,
              "leveldb.BytewiseComparator",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(target_file_size_base,
              "67108864",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(max_sequential_skip_in_iterations,
              "8",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(compaction_style,
              "kCompactionStyleLevel",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(max_bytes_for_level_base,
              "268435456",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(bloom_locality,
              "0",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(write_buffer_size,
              "67108864",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(compression_per_level,
              "",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(memtable_huge_page_size,
              "0",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(max_successive_merges,
              "0",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(arena_block_size,
              "8388608",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(target_file_size_multiplier,
              "1",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(max_bytes_for_level_multiplier_additional,
              "1:1:1:1:1:1:1",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(num_levels,
              "7",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(min_write_buffer_number_to_merge,
              "1",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(max_write_buffer_number_to_maintain,
              "0",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(max_write_buffer_number,
              "2",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(compression,
              "kNoCompression",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(level0_stop_writes_trigger,
              "36",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(level0_slowdown_writes_trigger,
              "20",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(compaction_filter,
              "DummyCompactionFilter",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(level0_file_num_compaction_trigger,
              "4",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(max_compaction_bytes,
              "1677721600",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(memtable_prefix_bloom_size_ratio,
              "0.000000",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(hard_pending_compaction_bytes_limit,
              "274877906944",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(ttl,
              "0",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(table_factory,
              "BlockBasedTable",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(soft_pending_compaction_bytes_limit,
              "68719476736",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(prefix_extractor,
              "nullptr",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(bottommost_compression,
              "kDisableCompressionOption",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(force_consistency_checks,
              "false",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(paranoid_file_checks,
              "false",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(max_bytes_for_level_multiplier,
              "10.000000",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(optimize_filters_for_hits,
              "false",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(level_compaction_dynamic_level_bytes,
              "false",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(inplace_update_num_locks,
              "10000",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(inplace_update_support,
              "false",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(disable_auto_compactions,
              "false",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(report_bg_io_stats,
              "false",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(compaction_options_universal,
              "{allow_trivial_move=false;stop_style=kCompactionStopStyleTotalSize;"
              "compression_size_percent=-1;max_size_amplification_percent=200;"
              "max_merge_width=4294967295;min_merge_width=2;size_ratio=1;}",
              ROCKSDB_OPTIONS_TYPE_CF);

DEFINE_string(compaction_options_fifo,
              "{allow_compaction=false;ttl=0;max_table_files_size=1073741824;}",
              ROCKSDB_OPTIONS_TYPE_CF);

//  [TableOptions/BlockBasedTable "default"]
DEFINE_string(pin_top_level_index_and_filter,
              "true",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(enable_index_compression,
              "true",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(read_amp_bytes_per_bit,
              "8589934592",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(format_version,
              "2",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(whole_key_filtering,
              "true",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(block_align,
              "false",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(metadata_block_size,
              "4096",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(block_size_deviation,
              "10",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(partition_filters,
              "false",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(block_size,
              "32768",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(index_block_restart_interval,
              "1",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(no_block_cache,
              "false",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(checksum,
              "kCRC32c",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(index_type,
              "kBinarySearch",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(verify_compression,
              "false",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(pin_l0_filter_and_index_blocks_in_cache,
              "false",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(block_restart_interval,
              "16",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(cache_index_and_filter_blocks_with_high_priority,
              "false",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(cache_index_and_filter_blocks,
              "false",
              ROCKSDB_OPTIONS_TYPE_BBT);

DEFINE_string(hash_index_allow_collision,
              "true",
              ROCKSDB_OPTIONS_TYPE_BBT);

// memtable_factory
DEFINE_string(memtable_factory,
              "nullptr",
              "memtable_factory:[nullptr | skiplist | vector "
              "| hashskiplist | hashlinklist | cuckoo]");

DEFINE_int64(bucket_count, 1000000,
             "memtable_factory:bucket_count parameter to "
             "pass into NewHashSkiplistRepFactory or "
             "NewHashLinkListRepFactory");

DEFINE_int32(hashskiplist_height, 4,
            "memtable_factory:skiplist_height parameter "
            "to pass into NewHashSkiplistRepFactory");

DEFINE_int32(hashskiplist_branching_factor, 4,
            "memtable_factory:branching_factor parameter "
            "to pass into NewHashSkiplistRepFactory");

DEFINE_int32(huge_page_tlb_size, 0,
            "memtable_factory:huge_page_tlb_size parameter "
            "to pass into NewHashLinkListRepFactory");

DEFINE_int32(bucket_entries_logging_threshold, 4096,
             "memtable_factory:bucket_entries_logging_threshold "
             "parameter to pass into "
             "NewHashLinkListRepFactory");

DEFINE_bool(if_log_bucket_dist_when_flash, true,
            "memtable_factory:if_log_bucket_dist_when_flash "
            "parameter to pass into "
            "NewHashLinkListRepFactory");

DEFINE_int32(threshold_use_skiplist, 256,
             "memtable_factory:threshold_use_skiplist parameter to "
             "pass into NewHashLinkListRepFactory");

DEFINE_int64(average_data_size, 64,
             "memtable_factory:average_data_size parameter to "
             "pass into NewHashCuckooRepFactory");

DEFINE_int64(hash_function_count, 4,
             "memtable_factory:hash_function_count parameter "
             "to pass into NewHashCuckooRepFactory");

DEFINE_int32(prefix_length, 8,
             "prefix_extractor:Prefix length to pass "
             "into NewFixedPrefixTransform");

// BlockBasedTable block_cache
DEFINE_int64(block_cache, 4,
             "BlockBasedTable:block_cache : MB");

namespace nebula {
namespace kvstore {

void RocksdbConfigOptions::load_option_maps() {
    options_map_database = {
            KVSTORE_CONFIG_FLAG(manual_wal_flush),
            KVSTORE_CONFIG_FLAG(allow_ingest_behind),
            KVSTORE_CONFIG_FLAG(avoid_flush_during_shutdown),
            KVSTORE_CONFIG_FLAG(avoid_flush_during_recovery),
            KVSTORE_CONFIG_FLAG(info_log_level),
            KVSTORE_CONFIG_FLAG(access_hint_on_compaction_start),
            KVSTORE_CONFIG_FLAG(write_thread_max_yield_usec),
            KVSTORE_CONFIG_FLAG(write_thread_slow_yield_usec),
            KVSTORE_CONFIG_FLAG(wal_recovery_mode),
            KVSTORE_CONFIG_FLAG(allow_concurrent_memtable_write),
            KVSTORE_CONFIG_FLAG(enable_pipelined_write),
            KVSTORE_CONFIG_FLAG(fail_if_options_file_error),
            KVSTORE_CONFIG_FLAG(stats_dump_period_sec),
            KVSTORE_CONFIG_FLAG(wal_bytes_per_sync),
            KVSTORE_CONFIG_FLAG(max_total_wal_size),
            KVSTORE_CONFIG_FLAG(delayed_write_rate),
            KVSTORE_CONFIG_FLAG(two_write_queues),
            KVSTORE_CONFIG_FLAG(bytes_per_sync),
            KVSTORE_CONFIG_FLAG(writable_file_max_buffer_size),
            KVSTORE_CONFIG_FLAG(wal_dir),
            KVSTORE_CONFIG_FLAG(log_file_time_to_roll),
            KVSTORE_CONFIG_FLAG(keep_log_file_num),
            KVSTORE_CONFIG_FLAG(WAL_ttl_seconds),
            KVSTORE_CONFIG_FLAG(db_write_buffer_size),
            KVSTORE_CONFIG_FLAG(table_cache_numshardbits),
            KVSTORE_CONFIG_FLAG(max_open_files),
            KVSTORE_CONFIG_FLAG(max_file_opening_threads),
            KVSTORE_CONFIG_FLAG(WAL_size_limit_MB),
            KVSTORE_CONFIG_FLAG(max_background_flushes),
            KVSTORE_CONFIG_FLAG(db_log_dir),
            KVSTORE_CONFIG_FLAG(max_background_compactions),
            KVSTORE_CONFIG_FLAG(max_subcompactions),
            KVSTORE_CONFIG_FLAG(max_background_jobs),
            KVSTORE_CONFIG_FLAG(random_access_max_buffer_size),
            KVSTORE_CONFIG_FLAG(delete_obsolete_files_period_micros),
            KVSTORE_CONFIG_FLAG(skip_stats_update_on_db_open),
            KVSTORE_CONFIG_FLAG(skip_log_error_on_recovery),
            KVSTORE_CONFIG_FLAG(dump_malloc_stats),
            KVSTORE_CONFIG_FLAG(paranoid_checks),
            KVSTORE_CONFIG_FLAG(is_fd_close_on_exec),
            KVSTORE_CONFIG_FLAG(max_manifest_file_size),
            KVSTORE_CONFIG_FLAG(error_if_exists),
            KVSTORE_CONFIG_FLAG(use_adaptive_mutex),
            KVSTORE_CONFIG_FLAG(enable_thread_tracking),
            KVSTORE_CONFIG_FLAG(create_missing_column_families),
            KVSTORE_CONFIG_FLAG(create_if_missing),
            KVSTORE_CONFIG_FLAG(manifest_preallocation_size),
            KVSTORE_CONFIG_FLAG(base_background_compactions),
            KVSTORE_CONFIG_FLAG(use_fsync),
            KVSTORE_CONFIG_FLAG(allow_2pc),
            KVSTORE_CONFIG_FLAG(recycle_log_file_num),
            KVSTORE_CONFIG_FLAG(use_direct_io_for_flush_and_compaction),
            KVSTORE_CONFIG_FLAG(compaction_readahead_size),
            KVSTORE_CONFIG_FLAG(use_direct_reads),
            KVSTORE_CONFIG_FLAG(allow_mmap_writes),
            KVSTORE_CONFIG_FLAG(preserve_deletes),
            KVSTORE_CONFIG_FLAG(enable_write_thread_adaptive_yield),
            KVSTORE_CONFIG_FLAG(max_log_file_size),
            KVSTORE_CONFIG_FLAG(allow_fallocate),
            KVSTORE_CONFIG_FLAG(allow_mmap_reads),
            KVSTORE_CONFIG_FLAG(new_table_reader_for_compaction_inputs),
            KVSTORE_CONFIG_FLAG(advise_random_on_open)
    };

    options_map_columnfamilie = {
            KVSTORE_CONFIG_FLAG(compaction_pri),
            KVSTORE_CONFIG_FLAG(merge_operator),
            KVSTORE_CONFIG_FLAG(compaction_filter_factory),
            KVSTORE_CONFIG_FLAG(memtable_insert_with_hint_prefix_extractor),
            KVSTORE_CONFIG_FLAG(comparator),
            KVSTORE_CONFIG_FLAG(target_file_size_base),
            KVSTORE_CONFIG_FLAG(max_sequential_skip_in_iterations),
            KVSTORE_CONFIG_FLAG(compaction_style),
            KVSTORE_CONFIG_FLAG(max_bytes_for_level_base),
            KVSTORE_CONFIG_FLAG(bloom_locality),
            KVSTORE_CONFIG_FLAG(write_buffer_size),
            KVSTORE_CONFIG_FLAG(compression_per_level),
            KVSTORE_CONFIG_FLAG(memtable_huge_page_size),
            KVSTORE_CONFIG_FLAG(max_successive_merges),
            KVSTORE_CONFIG_FLAG(arena_block_size),
            KVSTORE_CONFIG_FLAG(target_file_size_multiplier),
            KVSTORE_CONFIG_FLAG(max_bytes_for_level_multiplier_additional),
            KVSTORE_CONFIG_FLAG(num_levels),
            KVSTORE_CONFIG_FLAG(min_write_buffer_number_to_merge),
            KVSTORE_CONFIG_FLAG(max_write_buffer_number_to_maintain),
            KVSTORE_CONFIG_FLAG(max_write_buffer_number),
            KVSTORE_CONFIG_FLAG(compression),
            KVSTORE_CONFIG_FLAG(level0_stop_writes_trigger),
            KVSTORE_CONFIG_FLAG(level0_slowdown_writes_trigger),
            KVSTORE_CONFIG_FLAG(compaction_filter),
            KVSTORE_CONFIG_FLAG(level0_file_num_compaction_trigger),
            KVSTORE_CONFIG_FLAG(max_compaction_bytes),
            KVSTORE_CONFIG_FLAG(memtable_prefix_bloom_size_ratio),
            KVSTORE_CONFIG_FLAG(hard_pending_compaction_bytes_limit),
            KVSTORE_CONFIG_FLAG(ttl),
            KVSTORE_CONFIG_FLAG(table_factory),
            KVSTORE_CONFIG_FLAG(soft_pending_compaction_bytes_limit),
            KVSTORE_CONFIG_FLAG(prefix_extractor),
            KVSTORE_CONFIG_FLAG(bottommost_compression),
            KVSTORE_CONFIG_FLAG(force_consistency_checks),
            KVSTORE_CONFIG_FLAG(paranoid_file_checks),
            KVSTORE_CONFIG_FLAG(max_bytes_for_level_multiplier),
            KVSTORE_CONFIG_FLAG(optimize_filters_for_hits),
            KVSTORE_CONFIG_FLAG(level_compaction_dynamic_level_bytes),
            KVSTORE_CONFIG_FLAG(inplace_update_num_locks),
            KVSTORE_CONFIG_FLAG(inplace_update_support),
            KVSTORE_CONFIG_FLAG(disable_auto_compactions),
            KVSTORE_CONFIG_FLAG(report_bg_io_stats),
            KVSTORE_CONFIG_FLAG(compaction_options_universal),
            KVSTORE_CONFIG_FLAG(compaction_options_fifo)
    };

    options_map_blockbasedtable = {
            KVSTORE_CONFIG_FLAG(pin_top_level_index_and_filter),
            KVSTORE_CONFIG_FLAG(enable_index_compression),
            KVSTORE_CONFIG_FLAG(read_amp_bytes_per_bit),
            KVSTORE_CONFIG_FLAG(format_version),
            KVSTORE_CONFIG_FLAG(whole_key_filtering),
            KVSTORE_CONFIG_FLAG(block_align),
            KVSTORE_CONFIG_FLAG(metadata_block_size),
            KVSTORE_CONFIG_FLAG(block_size_deviation),
            KVSTORE_CONFIG_FLAG(partition_filters),
            KVSTORE_CONFIG_FLAG(block_size),
            KVSTORE_CONFIG_FLAG(index_block_restart_interval),
            KVSTORE_CONFIG_FLAG(no_block_cache),
            KVSTORE_CONFIG_FLAG(checksum),
            KVSTORE_CONFIG_FLAG(index_type),
            KVSTORE_CONFIG_FLAG(verify_compression),
            KVSTORE_CONFIG_FLAG(pin_l0_filter_and_index_blocks_in_cache),
            KVSTORE_CONFIG_FLAG(block_restart_interval),
            KVSTORE_CONFIG_FLAG(cache_index_and_filter_blocks_with_high_priority),
            KVSTORE_CONFIG_FLAG(cache_index_and_filter_blocks),
            KVSTORE_CONFIG_FLAG(hash_index_allow_collision)
    };
}

RocksdbConfigOptions::RocksdbConfigOptions(const KV_paths rocksdb_paths,
                                           int nebula_space_num,
                                           bool ignore_unknown_options,
                                           bool input_strings_escaped)
                                           : dataPaths_(rocksdb_paths)
                                           , nebula_space_num_(nebula_space_num)
                                           , ignore_unknown_options_(ignore_unknown_options)
                                           , input_strings_escaped_(input_strings_escaped)  {
    LOG(INFO) << "begin create rocksdb options... ";
}

RocksdbConfigOptions::~RocksdbConfigOptions() {
}

rocksdb::Status RocksdbConfigOptions::createRocksdbEngineOptions(rocksdb::Options &options) {
    // Check Configuration option version and rocksdb version
    if (FLAGS_rocksdb_options_version !=
        folly::stringPrintf("%d.%d.%d", ROCKSDB_MAJOR, ROCKSDB_MINOR, ROCKSDB_PATCH)) {
        LOG(ERROR) << "Configuration options version number does "
                      "not match rocksdb version number .\n" <<
                   "Configuration options version number : " <<
                   FLAGS_rocksdb_options_version.c_str() <<
                   ", rocksdb version number : " <<
                   folly::stringPrintf("%d.%d.%d", ROCKSDB_MAJOR, ROCKSDB_MINOR, ROCKSDB_PATCH);
        return rocksdb::Status::NotSupported(
                "Local rocksdb version incompatible configuration version.");
    }

    // create rocksdb config options
    rocksdb::Status s = initRocksdbOptions();
    if (!s.ok()) {
        LOG(ERROR) << "Create Rocksdb Options error, "
                      "please check rocksdb options. return status : "
                      << s.ToString();
        return s;
    }

    // check options compatibility
    s = checkOptionsCompatibility();
    if (!s.ok()) {
        LOG(ERROR) << "Check rocksdb options compatibility, "
                      "please check rocksdb options and "
                      "rocksdb version. return status : "
                      << s.ToString();
        return s;
    }

    options = std::move(base_opts_);
    return rocksdb::Status::OK();
}

rocksdb::Status RocksdbConfigOptions::checkOptionsCompatibility() {
    for (auto spaceId = 0; spaceId < nebula_space_num_; spaceId++) {
        for (auto& path : dataPaths_) {
            rocksdb::Status s = rocksdb::CheckOptionsCompatibility(
                    KV_DATA_PATH_FORMAT(path.first.c_str(), spaceId),
                    rocksdb::Env::Default(),
                    RocksdbConfigOptions::db_opts_, cf_descs_);
            if (s.code() == rocksdb::Status::kNotFound) {
                LOG(INFO) << "new rocksdb instance, no compatibility "
                             "checks are required. data path is : "
                          << path.first.c_str();
                continue;
            }

            if (!s.ok()) {
                return s;
            }
        }
    }

    return rocksdb::Status::OK();
}

rocksdb::Status RocksdbConfigOptions::initRocksdbOptions() {
    rocksdb::DBOptions base_db_opt;
    rocksdb::ColumnFamilyOptions base_cf_opt;
    rocksdb::Status s;

    base_opts_.create_if_missing = true;
    load_option_maps();
    s = GetDBOptionsFromMap(base_db_opt,
                            options_map_database,
                            &db_opts_);

    if (!s.ok()) {
        return s;
    }

    s = GetColumnFamilyOptionsFromMap(base_cf_opt,
                                      options_map_columnfamilie,
                                      &cf_opts_,
                                      input_strings_escaped_,
                                      ignore_unknown_options_);

    if (!s.ok()) {
        return s;
    }

    base_opts_ = rocksdb::Options(db_opts_, cf_opts_);

    cf_descs_.push_back({rocksdb::kDefaultColumnFamilyName, cf_opts_});

    s = rocksdb::GetBlockBasedTableOptionsFromMap(
            rocksdb::BlockBasedTableOptions(),
            options_map_blockbasedtable,
            &bbt_opts_,
            input_strings_escaped_,
            ignore_unknown_options_);

    if (!s.ok()) {
        return s;
    }

    if (!setup_block_cache()) {
        return rocksdb::Status::Aborted("rocksdb option block_cache error");
    }

    if (!setup_memtable_factory()) {
        return rocksdb::Status::Aborted("rocksdb option memtable_factory error");
    }

    if (!setup_compaction_filter_factory()) {
        return rocksdb::Status::Aborted("rocksdb option compaction_filter_factory error");
    }

    if (!setup_prefix_extractor()) {
        return rocksdb::Status::Aborted("rocksdb option prefix_extractor error");
    }

    if (!setup_comparator()) {
        return rocksdb::Status::Aborted("rocksdb option comparator error");
    }

    if (!setup_merge_operator()) {
        return rocksdb::Status::Aborted("rocksdb option merge_operator error");
    }

    if (!setup_compaction_filter()) {
        return rocksdb::Status::Aborted("rocksdb option compaction_filter error");
    }

    base_opts_.table_factory.reset(NewBlockBasedTableFactory(bbt_opts_));


    return rocksdb::Status::OK();
}

bool RocksdbConfigOptions::getRocksdbEngineOptionValue(
        ROCKSDB_OPTION_TYPE opt_type,
        const char *opt_name,
        std::string &opt_value) {
    switch (opt_type) {
        case (ROCKSDB_OPTION_TYPE::DBOptions):
            if (options_map_database.find(opt_name) != options_map_database.end()) {
                opt_value = options_map_database[opt_name].c_str();
                return true;
            }
            break;
        case (ROCKSDB_OPTION_TYPE::CFOptions):
            if (options_map_database.find(opt_name) != options_map_database.end()) {
                opt_value = options_map_database[opt_name].c_str();
                return true;
            }
            break;
        case (ROCKSDB_OPTION_TYPE::TableOptions):
            if (options_map_blockbasedtable.find(opt_name) != options_map_blockbasedtable.end()) {
                opt_value = options_map_blockbasedtable[opt_name].c_str();
                return true;
            }
            break;
        default:
            LOG(FATAL) << "option type illegal: " << opt_type;
            break;
    }

    return false;
}

typedef enum { error_token,
               nullptr_token,
               skiplist_token,
               vector_token,
               hashskiplist_token,
               hashlinklist_token,
               cuckoo_token } mem_token_t;

mem_token_t mem_factory_lexer(const char *s) {
    // TODO: consider hash table here
    static struct entry_s {
        const char *key;
        mem_token_t token;
    } token_table[] = {
            { "nullptr", nullptr_token },
            { "skiplist", skiplist_token },
            { "vector" , vector_token },
            { "hashskiplist" , hashskiplist_token },
            { "hashlinklist", hashlinklist_token },
            { "cuckoo", cuckoo_token },
            {"", error_token},
    };
    struct entry_s *p = token_table;
    for (; p->key != NULL && std::strcmp(p->key, s) != 0; ++p) continue;
    return p->token;
}

bool RocksdbConfigOptions::setup_memtable_factory() {
    switch (mem_factory_lexer(FLAGS_memtable_factory.c_str())) {
        case nullptr_token :
            return true;
        case skiplist_token :
            base_opts_.memtable_factory.reset(new rocksdb::SkipListFactory);
            break;
        case vector_token :
            base_opts_.memtable_factory.reset(new rocksdb::VectorRepFactory);
            break;
        case hashskiplist_token :
            base_opts_.memtable_factory.reset(rocksdb::NewHashSkipListRepFactory(
                    FLAGS_bucket_count, FLAGS_hashskiplist_height,
                    FLAGS_hashskiplist_branching_factor));
            break;
        case hashlinklist_token :
            base_opts_.memtable_factory.reset(rocksdb::NewHashLinkListRepFactory(
                    FLAGS_bucket_count, FLAGS_huge_page_tlb_size,
                    FLAGS_bucket_entries_logging_threshold,
                    FLAGS_if_log_bucket_dist_when_flash, FLAGS_threshold_use_skiplist));
            break;
        case cuckoo_token :
            base_opts_.memtable_factory.reset(rocksdb::NewHashCuckooRepFactory(
                    static_cast<int>(strtol(FLAGS_write_buffer_size.c_str(), NULL, 10)),

                    FLAGS_average_data_size,
                    static_cast<uint32_t>(FLAGS_hash_function_count)));
            break;
        case error_token : {
            LOG(ERROR) << "Unknown memtable_factory : "
            << FLAGS_memtable_factory.c_str();
            return false;
        }
    }
    return true;
}

bool RocksdbConfigOptions::setup_compaction_filter_factory() {
    // TODO compaction_filter_factory need
    return true;
}

bool RocksdbConfigOptions::setup_prefix_extractor() {
    base_opts_.prefix_extractor.reset(
            rocksdb::NewFixedPrefixTransform(FLAGS_prefix_length));
    return true;
}

bool RocksdbConfigOptions::setup_comparator() {
    // TODO comparator need
    return true;
}

bool RocksdbConfigOptions::setup_merge_operator() {
    // TODO merge_operator need
    return true;
}

bool RocksdbConfigOptions::setup_compaction_filter() {
    // TODO compaction_filter need
    return true;
}

bool RocksdbConfigOptions::setup_block_cache() {
    bbt_opts_.block_cache = rocksdb::NewLRUCache(FLAGS_block_cache * 1024 * 1024);
    return true;
}

bool RocksdbConfigOptions::getKVPaths(std::string data_paths_str,
        std::string wal_paths_str, KV_paths &rocksdb_paths) {
    std::vector<std::string> data_paths, wal_paths;
    folly::split(",", data_paths_str, data_paths, true);
    std::transform(data_paths.begin(), data_paths.end(), data_paths.begin(), [](auto& p) {
        return folly::trimWhitespace(p).str();
    });
    folly::split(",", wal_paths_str, wal_paths, true);
    std::transform(wal_paths.begin(), wal_paths.end(), wal_paths.begin(), [](auto& p) {
        return folly::trimWhitespace(p).str();
    });

    if (data_paths.size() == 0 || data_paths.size() != wal_paths.size()) {
        LOG(ERROR) << "If wal_path is not null , "
                      "the number of data_path and wal_path must be equal.";
        return false;
    }

    // Because the number of data_paths is equal to the number of wal_paths,
    // So can assign values directly according to the i.
    for (uint32_t i = 0 ; i < data_paths.size(); i++) {
        rocksdb_paths.emplace_back(
                KV_path(data_paths.at(i), wal_paths.at(i)));
    }
    return true;
}

}  // namespace kvstore
}  // namespace nebula

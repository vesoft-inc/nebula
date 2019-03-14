/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef NEBULA_GRAPH_ROCKSDBCONFIGFLAGS_H
#define NEBULA_GRAPH_ROCKSDBCONFIGFLAGS_H

#include "base/Base.h"

// [Version]

DECLARE_string(rocksdb_options_version);

DECLARE_bool(rocksdb_disable_wal);

// [DBOptions]
/*
 * Rocksdb
 *
 */
DECLARE_string(manual_wal_flush);

DECLARE_string(allow_ingest_behind);

DECLARE_string(avoid_flush_during_shutdown);

DECLARE_string(avoid_flush_during_recovery);

DECLARE_string(info_log_level);

DECLARE_string(access_hint_on_compaction_start);

DECLARE_string(write_thread_max_yield_usec);

DECLARE_string(write_thread_slow_yield_usec);

DECLARE_string(wal_recovery_mode);

DECLARE_string(allow_concurrent_memtable_write);

DECLARE_string(enable_pipelined_write);

DECLARE_string(fail_if_options_file_error);

DECLARE_string(stats_dump_period_sec);

DECLARE_string(wal_bytes_per_sync);

DECLARE_string(max_total_wal_size);

DECLARE_string(delayed_write_rate);

DECLARE_string(two_write_queues);

DECLARE_string(bytes_per_sync);

DECLARE_string(writable_file_max_buffer_size);

DECLARE_string(wal_dir);

DECLARE_string(log_file_time_to_roll);

DECLARE_string(keep_log_file_num);

DECLARE_string(WAL_ttl_seconds);

DECLARE_string(db_write_buffer_size);

DECLARE_string(table_cache_numshardbits);

DECLARE_string(max_open_files);

DECLARE_string(max_file_opening_threads);

DECLARE_string(WAL_size_limit_MB);

DECLARE_string(max_background_flushes);

DECLARE_string(db_log_dir);

DECLARE_string(max_background_compactions);

DECLARE_string(max_subcompactions);

DECLARE_string(max_background_jobs);

DECLARE_string(random_access_max_buffer_size);

DECLARE_string(delete_obsolete_files_period_micros);

DECLARE_string(skip_stats_update_on_db_open);

DECLARE_string(skip_log_error_on_recovery);

DECLARE_string(dump_malloc_stats);

DECLARE_string(paranoid_checks);

DECLARE_string(is_fd_close_on_exec);

DECLARE_string(max_manifest_file_size);

DECLARE_string(error_if_exists);

DECLARE_string(use_adaptive_mutex);

DECLARE_string(enable_thread_tracking);

DECLARE_string(create_missing_column_families);

DECLARE_string(create_if_missing);

DECLARE_string(manifest_preallocation_size);

DECLARE_string(base_background_compactions);

DECLARE_string(use_fsync);

DECLARE_string(allow_2pc);

DECLARE_string(recycle_log_file_num);

DECLARE_string(use_direct_io_for_flush_and_compaction);

DECLARE_string(compaction_readahead_size);

DECLARE_string(use_direct_reads);

DECLARE_string(allow_mmap_writes);

DECLARE_string(preserve_deletes);

DECLARE_string(enable_write_thread_adaptive_yield);

DECLARE_string(max_log_file_size);

DECLARE_string(allow_fallocate);

DECLARE_string(allow_mmap_reads);

DECLARE_string(new_table_reader_for_compaction_inputs);

DECLARE_string(advise_random_on_open);

// [CFOptions "default"]

DECLARE_string(compaction_pri);

DECLARE_string(memtable_insert_with_hint_prefix_extractor);

DECLARE_string(comparator);

DECLARE_string(target_file_size_base);

DECLARE_string(max_sequential_skip_in_iterations);

DECLARE_string(compaction_style);

DECLARE_string(max_bytes_for_level_base);

DECLARE_string(bloom_locality);

DECLARE_string(write_buffer_size);

DECLARE_string(compression_per_level);

DECLARE_string(memtable_huge_page_size);

DECLARE_string(max_successive_merges);

DECLARE_string(arena_block_size);

DECLARE_string(target_file_size_multiplier);

DECLARE_string(max_bytes_for_level_multiplier_additional);

DECLARE_string(num_levels);

DECLARE_string(min_write_buffer_number_to_merge);

DECLARE_string(max_write_buffer_number_to_maintain);

DECLARE_string(max_write_buffer_number);

DECLARE_string(compression);

DECLARE_string(level0_stop_writes_trigger);

DECLARE_string(level0_slowdown_writes_trigger);

DECLARE_string(level0_file_num_compaction_trigger);

DECLARE_string(max_compaction_bytes);

DECLARE_string(memtable_prefix_bloom_size_ratio);

DECLARE_string(hard_pending_compaction_bytes_limit);

DECLARE_string(ttl);

DECLARE_string(table_factory);

DECLARE_string(soft_pending_compaction_bytes_limit);

DECLARE_string(prefix_extractor);

DECLARE_string(bottommost_compression);

DECLARE_string(force_consistency_checks);

DECLARE_string(paranoid_file_checks);

DECLARE_string(max_bytes_for_level_multiplier);

DECLARE_string(optimize_filters_for_hits);

DECLARE_string(level_compaction_dynamic_level_bytes);

DECLARE_string(inplace_update_num_locks);

DECLARE_string(inplace_update_support);

DECLARE_string(disable_auto_compactions);

DECLARE_string(report_bg_io_stats);

DECLARE_string(compaction_options_universal);

DECLARE_string(compaction_options_fifo);

//  [TableOptions/BlockBasedTable "default"]
DECLARE_string(pin_top_level_index_and_filter);

DECLARE_string(enable_index_compression);

DECLARE_string(read_amp_bytes_per_bit);

DECLARE_string(format_version);

DECLARE_string(whole_key_filtering);

DECLARE_string(block_align);

DECLARE_string(metadata_block_size);

DECLARE_string(block_size_deviation);

DECLARE_string(partition_filters);

DECLARE_string(block_size);

DECLARE_string(index_block_restart_interval);

DECLARE_string(no_block_cache);

DECLARE_string(checksum);

DECLARE_string(index_type);

DECLARE_string(verify_compression);

DECLARE_string(pin_l0_filter_and_index_blocks_in_cache);

DECLARE_string(block_restart_interval);

DECLARE_string(cache_index_and_filter_blocks_with_high_priority);

DECLARE_string(cache_index_and_filter_blocks);

DECLARE_string(hash_index_allow_collision);

// memtable_factory
DECLARE_string(memtable_factory);

DECLARE_int64(bucket_count);

DECLARE_int32(hashskiplist_height);

DECLARE_int32(hashskiplist_branching_factor);

DECLARE_int32(huge_page_tlb_size);

DECLARE_int32(bucket_entries_logging_threshold);

DECLARE_bool(if_log_bucket_dist_when_flash);

DECLARE_int32(threshold_use_skiplist);

DECLARE_int64(average_data_size);

DECLARE_int64(hash_function_count);

DECLARE_int32(prefix_length);

// BlockBasedTable block_cache
DECLARE_int64(block_cache);

#endif  // NEBULA_GRAPH_ROCKSDBCONFIGFLAGS_H

/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/RocksEngineConfig.h"

#include <rocksdb/cache.h>
#include <rocksdb/concurrent_task_limiter.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/utilities/options_util.h>

#include "common/base/Base.h"
#include "common/conf/Configuration.h"
#include "common/fs/FileUtils.h"
#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/EventListener.h"

// [WAL]
DEFINE_bool(rocksdb_disable_wal, false, "Whether to disable the WAL in rocksdb");

DEFINE_bool(rocksdb_wal_sync, false, "Whether WAL writes are synchronized to disk or not");

// [DBOptions]
DEFINE_string(rocksdb_db_options, "{}", "json string of DBOptions, all keys and values are string");

// [CFOptions "default"]
DEFINE_string(rocksdb_column_family_options,
              "{}",
              "json string of ColumnFamilyOptions, all keys and values are string");

//  [TableOptions/BlockBasedTable "default"]
DEFINE_string(rocksdb_block_based_table_options,
              "{}",
              "json string of BlockBasedTableOptions, all keys and values are string");

DEFINE_int32(rocksdb_batch_size, 4 * 1024, "default reserved bytes for one batch operation");

/*
 * For these un-supported string options as below, will need to specify them
 * with gflag.
 */

// BlockBasedTable block_cache
DEFINE_int64(rocksdb_block_cache,
             1024,
             "The default block cache size used in BlockBasedTable. The unit is MB");

DEFINE_bool(disable_page_cache,
            false,
            "Disable page cache to better control memory used by rocksdb.");

DEFINE_int32(rocksdb_row_cache_num, 16 * 1000 * 1000, "Total keys inside the cache");

DEFINE_int32(cache_bucket_exp, 8, "Total buckets number is 1 << cache_bucket_exp");

DEFINE_bool(enable_partitioned_index_filter, false, "True for partitioned index filters");

DEFINE_string(rocksdb_compression,
              "snappy",
              "Compression algorithm used by RocksDB, "
              "options: no, snappy, lz4, lz4hc, zstd, zlib, bzip2, xpress");

DEFINE_string(rocksdb_bottommost_compression,
              "disable",
              "Specify the bottommost level compression algorithm"
              "options: no, snappy, lz4, lz4hc, zstd, zlib, bzip2, xpress, disable");

DEFINE_string(rocksdb_compression_per_level,
              "",
              "Specify per level compression algorithm, "
              "delimited by `:', ignored fields will be "
              "replaced by FLAGS_rocksdb_compression. "
              "e.g. \"no:no:lz4:lz4::zstd\" === "
              "\"no:no:lz4:lz4:lz4:snappy:zstd:snappy\"");

DEFINE_bool(enable_rocksdb_statistics, false, "Whether or not to enable rocksdb's statistics");
DEFINE_string(rocksdb_stats_level, "kExceptHistogramOrTimers", "rocksdb statistics level");

DEFINE_int32(num_compaction_threads, 0, "Number of total compaction threads. 0 means unlimited.");

DEFINE_int32(rocksdb_rate_limit,
             0,
             "write limit in bytes per sec. The unit is MB. 0 means unlimited.");

DEFINE_bool(enable_rocksdb_whole_key_filtering,
            false,
            "Whether or not to enable rocksdb's whole key bloom filter");

DEFINE_bool(enable_rocksdb_prefix_filtering,
            true,
            "Whether or not to enable rocksdb's prefix bloom filter.");

DEFINE_bool(rocksdb_compact_change_level,
            true,
            "If true, compacted files will be moved to the minimum level capable "
            "of holding the data or given level (specified non-negative "
            "target_level).");

DEFINE_int32(rocksdb_compact_target_level,
             -1,
             "If change_level is true and target_level have non-negative "
             "value, compacted files "
             "will be moved to target_level. If change_level is true and "
             "target_level is -1, "
             "compacted files will be moved to the minimum level capable of "
             "holding the data.");

DEFINE_string(rocksdb_table_format,
              "BlockBasedTable",
              "SST file format of rocksdb, only support BlockBasedTable and PlainTable");

DEFINE_string(rocksdb_wal_dir, "", "Rocksdb wal directory");

DEFINE_string(rocksdb_backup_dir, "", "Rocksdb backup directory, only used in PlainTable format");

DEFINE_int32(rocksdb_backup_interval_secs,
             300,
             "Rocksdb backup directory, only used in PlainTable format");

DEFINE_bool(rocksdb_enable_kv_separation,
            false,
            "Whether or not to enable BlobDB (RocksDB key-value separation support)");

DEFINE_uint64(rocksdb_kv_separation_threshold,
              100,
              "RocksDB key value separation threshold in bytes. Values at or above this threshold "
              "will be written to blob files during flush or compaction."
              "This value is only effective when enable_kv_separation is true.");

DEFINE_string(rocksdb_blob_compression,
              "snappy",
              "Compression algorithm for blobs, "
              "options: no,snappy,lz4,lz4hc,zstd,zlib,bzip2");

DEFINE_bool(rocksdb_enable_blob_garbage_collection,
            true,
            "Set this to true to make BlobDB actively relocate valid blobs "
            "from the oldest blob files as they are encountered during compaction");

namespace nebula {
namespace kvstore {

static const std::unordered_map<std::string, rocksdb::CompressionType> kCompressionTypeMap = {
    {"no", rocksdb::kNoCompression},
    {"snappy", rocksdb::kSnappyCompression},
    {"lz4", rocksdb::kLZ4Compression},
    {"lz4hc", rocksdb::kLZ4HCCompression},
    {"zstd", rocksdb::kZSTD},
    {"zlib", rocksdb::kZlibCompression},
    {"bzip2", rocksdb::kBZip2Compression},
    {"xpress", rocksdb::kXpressCompression},
    {"disable", rocksdb::kDisableCompressionOption}};

static rocksdb::Status initRocksdbCompression(rocksdb::Options& baseOpts) {
  // Set the general compression algorithm
  {
    auto it = kCompressionTypeMap.find(FLAGS_rocksdb_compression);
    if (it == kCompressionTypeMap.end()) {
      LOG(ERROR) << "Unsupported compression type: " << FLAGS_rocksdb_compression;
      return rocksdb::Status::InvalidArgument();
    }
    baseOpts.compression = it->second;

    it = kCompressionTypeMap.find(FLAGS_rocksdb_bottommost_compression);
    if (it == kCompressionTypeMap.end()) {
      LOG(ERROR) << "Unsupported compression type: " << FLAGS_rocksdb_bottommost_compression;
      return rocksdb::Status::InvalidArgument();
    }
    baseOpts.bottommost_compression = it->second;
  }
  if (FLAGS_rocksdb_compression_per_level.empty()) {
    return rocksdb::Status::OK();
  }

  // Set the per level compression algorithm, which will override the general
  // one. Given baseOpts.compression is lz4, "no:::::zstd" equals to
  // "no:lz4:lz4:lz4:lz4:zstd:lz4"
  std::vector<std::string> compressions;
  folly::split(":", FLAGS_rocksdb_compression_per_level, compressions, false);
  compressions.resize(baseOpts.num_levels);
  baseOpts.compression_per_level.resize(baseOpts.num_levels);
  for (auto i = 0u; i < compressions.size(); i++) {
    if (compressions[i].empty()) {
      compressions[i] = FLAGS_rocksdb_compression;
    }
    auto it = kCompressionTypeMap.find(compressions[i]);
    if (it == kCompressionTypeMap.end()) {
      LOG(ERROR) << "Unsupported compression type: " << compressions[i];
      return rocksdb::Status::InvalidArgument();
    }
    baseOpts.compression_per_level[i] = it->second;
  }
  LOG(INFO) << "compression per level: " << folly::join(":", compressions);

  return rocksdb::Status::OK();
}

static rocksdb::Status initRocksdbKVSeparation(rocksdb::Options& baseOpts) {
  if (FLAGS_rocksdb_enable_kv_separation) {
    baseOpts.enable_blob_files = true;
    baseOpts.min_blob_size = FLAGS_rocksdb_kv_separation_threshold;

    // set blob compresstion algorithm
    auto it = kCompressionTypeMap.find(FLAGS_rocksdb_blob_compression);
    if (it == kCompressionTypeMap.end()) {
      LOG(ERROR) << "Unsupported compression type: " << FLAGS_rocksdb_blob_compression;
      return rocksdb::Status::InvalidArgument();
    }
    baseOpts.blob_compression_type = it->second;

    // set blob gc
    baseOpts.enable_blob_garbage_collection = FLAGS_rocksdb_enable_blob_garbage_collection;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status initRocksdbOptions(rocksdb::Options& baseOpts,
                                   GraphSpaceID spaceId,
                                   int32_t vidLen) {
  rocksdb::Status s;
  rocksdb::DBOptions dbOpts;
  rocksdb::ColumnFamilyOptions cfOpts;
  rocksdb::BlockBasedTableOptions bbtOpts;

  // DBOptions
  std::unordered_map<std::string, std::string> dbOptsMap;
  if (!loadOptionsMap(dbOptsMap, FLAGS_rocksdb_db_options)) {
    return rocksdb::Status::InvalidArgument();
  }
  s = GetDBOptionsFromMap(rocksdb::DBOptions(), dbOptsMap, &dbOpts, true);
  if (!s.ok()) {
    return s;
  }
  std::shared_ptr<rocksdb::Statistics> stats = getDBStatistics();
  if (stats) {
    dbOpts.statistics = std::move(stats);
    dbOpts.stats_dump_period_sec = 0;  // exposing statistics ourself
  }
  dbOpts.listeners.emplace_back(new EventListener());

  // if rocksdb_wal_dir is set, specify it to rocksdb
  if (!FLAGS_rocksdb_wal_dir.empty()) {
    auto walDir = folly::stringPrintf("%s/rocksdb_wal/%d", FLAGS_rocksdb_wal_dir.c_str(), spaceId);
    if (fs::FileUtils::fileType(walDir.c_str()) == fs::FileType::NOTEXIST) {
      if (!fs::FileUtils::makeDir(walDir)) {
        LOG(FATAL) << "makeDir " << walDir << " failed";
      }
    }
    LOG(INFO) << "set rocksdb wal of space " << spaceId << " to " << walDir;
    dbOpts.wal_dir = walDir;
  }

  // ColumnFamilyOptions
  std::unordered_map<std::string, std::string> cfOptsMap;
  if (!loadOptionsMap(cfOptsMap, FLAGS_rocksdb_column_family_options)) {
    return rocksdb::Status::InvalidArgument();
  }
  s = GetColumnFamilyOptionsFromMap(rocksdb::ColumnFamilyOptions(), cfOptsMap, &cfOpts, true);
  if (!s.ok()) {
    return s;
  }

  baseOpts = rocksdb::Options(dbOpts, cfOpts);

  s = initRocksdbCompression(baseOpts);
  if (!s.ok()) {
    return s;
  }

  s = initRocksdbKVSeparation(baseOpts);
  if (!s.ok()) {
    return s;
  }

  if (FLAGS_disable_page_cache) {
    baseOpts.use_direct_reads = true;
  }

  if (FLAGS_num_compaction_threads > 0) {
    static std::shared_ptr<rocksdb::ConcurrentTaskLimiter> compaction_thread_limiter{
        rocksdb::NewConcurrentTaskLimiter("compaction", FLAGS_num_compaction_threads)};
    baseOpts.compaction_thread_limiter = compaction_thread_limiter;
  }
  if (FLAGS_rocksdb_rate_limit > 0) {
    static std::shared_ptr<rocksdb::RateLimiter> rate_limiter{
        rocksdb::NewGenericRateLimiter(FLAGS_rocksdb_rate_limit * 1024 * 1024)};
    baseOpts.rate_limiter = rate_limiter;
  }

  size_t prefixLength = sizeof(PartitionID) + vidLen;
  if (FLAGS_rocksdb_table_format == "BlockBasedTable") {
    // BlockBasedTableOptions
    std::unordered_map<std::string, std::string> bbtOptsMap;
    if (!loadOptionsMap(bbtOptsMap, FLAGS_rocksdb_block_based_table_options)) {
      return rocksdb::Status::InvalidArgument();
    }
    s = GetBlockBasedTableOptionsFromMap(
        rocksdb::BlockBasedTableOptions(), bbtOptsMap, &bbtOpts, true);
    if (!s.ok()) {
      return s;
    }

    if (FLAGS_rocksdb_block_cache <= 0) {
      bbtOpts.no_block_cache = true;
    } else {
      static std::shared_ptr<rocksdb::Cache> blockCache =
          rocksdb::NewLRUCache(FLAGS_rocksdb_block_cache * 1024 * 1024, FLAGS_cache_bucket_exp);
      bbtOpts.block_cache = blockCache;
    }

    if (FLAGS_rocksdb_row_cache_num) {
      static std::shared_ptr<rocksdb::Cache> rowCache =
          rocksdb::NewLRUCache(FLAGS_rocksdb_row_cache_num, FLAGS_cache_bucket_exp);
      baseOpts.row_cache = rowCache;
    }

    bbtOpts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    if (FLAGS_enable_partitioned_index_filter) {
      bbtOpts.index_type = rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
      bbtOpts.partition_filters = true;
      bbtOpts.cache_index_and_filter_blocks = true;
      bbtOpts.cache_index_and_filter_blocks_with_high_priority = true;
      bbtOpts.pin_top_level_index_and_filter = true;
      bbtOpts.pin_l0_filter_and_index_blocks_in_cache =
          baseOpts.compaction_style == rocksdb::CompactionStyle::kCompactionStyleLevel;
    }
    if (FLAGS_enable_rocksdb_prefix_filtering) {
      baseOpts.prefix_extractor.reset(rocksdb::NewCappedPrefixTransform(prefixLength));
    }
    bbtOpts.whole_key_filtering = FLAGS_enable_rocksdb_whole_key_filtering;
    baseOpts.table_factory.reset(NewBlockBasedTableFactory(bbtOpts));
    baseOpts.create_if_missing = true;
  } else if (FLAGS_rocksdb_table_format == "PlainTable") {
    // wal_dir need to be specified by rocksdb_wal_dir.
    //
    // WAL_ttl_seconds is 0 by default in rocksdb, which will check every 10
    // mins, so rocksdb_backup_interval_secs is set to half of WAL_ttl_seconds
    // by default. WAL_ttl_seconds and rocksdb_backup_interval_secs need to be
    // modify together if necessary
    FLAGS_rocksdb_disable_wal = false;
    if (!FLAGS_enable_rocksdb_prefix_filtering) {
      return rocksdb::Status::InvalidArgument("PlainTable should use prefix bloom filter");
    }
    baseOpts.prefix_extractor.reset(rocksdb::NewCappedPrefixTransform(prefixLength));
    baseOpts.table_factory.reset(rocksdb::NewPlainTableFactory());
    baseOpts.create_if_missing = true;
  } else {
    return rocksdb::Status::NotSupported("Illegal table format");
  }

  return s;
}

bool loadOptionsMap(std::unordered_map<std::string, std::string>& map, const std::string& gflags) {
  conf::Configuration conf;
  auto status = conf.parseFromString(gflags);
  if (!status.ok()) {
    return false;
  }
  conf.forEachItem([&map](const std::string& key, const folly::dynamic& val) {
    LOG(INFO) << "Emplace rocksdb option " << key << "=" << val.asString();
    map.emplace(key, val.asString());
  });
  return true;
}

static std::shared_ptr<rocksdb::Statistics> createDBStatistics() {
  std::shared_ptr<rocksdb::Statistics> dbstats = rocksdb::CreateDBStatistics();
  if (FLAGS_rocksdb_stats_level == "kExceptHistogramOrTimers") {
    dbstats->set_stats_level(rocksdb::StatsLevel::kExceptHistogramOrTimers);
  } else if (FLAGS_rocksdb_stats_level == "kExceptTimers") {
    dbstats->set_stats_level(rocksdb::StatsLevel::kExceptTimers);
  } else if (FLAGS_rocksdb_stats_level == "kExceptDetailedTimers") {
    dbstats->set_stats_level(rocksdb::StatsLevel::kExceptDetailedTimers);
  } else if (FLAGS_rocksdb_stats_level == "kExceptTimeForMutex") {
    dbstats->set_stats_level(rocksdb::StatsLevel::kExceptTimeForMutex);
  } else {
    dbstats->set_stats_level(rocksdb::StatsLevel::kAll);
  }
  return dbstats;
}

std::shared_ptr<rocksdb::Statistics> getDBStatistics() {
  if (FLAGS_enable_rocksdb_statistics) {
    static std::shared_ptr<rocksdb::Statistics> dbstats = createDBStatistics();
    return dbstats;
  }
  return nullptr;
}

}  // namespace kvstore
}  // namespace nebula

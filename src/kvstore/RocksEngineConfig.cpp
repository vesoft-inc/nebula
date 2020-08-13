/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/RocksEngineConfig.h"
#include "kvstore/EventListner.h"
#include "rocksdb/db.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/rate_limiter.h"
#include "base/Configuration.h"
#include "utils/NebulaKeyUtils.h"

// [WAL]
DEFINE_bool(rocksdb_disable_wal,
            false,
            "Whether to disable the WAL in rocksdb");

DEFINE_bool(rocksdb_wal_sync,
            false,
            "Whether WAL writes are synchronized to disk or not");

// [DBOptions]
DEFINE_string(rocksdb_db_options,
              "{}",
              "json string of DBOptions, all keys and values are string");

// [CFOptions "default"]
DEFINE_string(rocksdb_column_family_options,
              "{}",
              "json string of ColumnFamilyOptions, all keys and values are string");

//  [TableOptions/BlockBasedTable "default"]
DEFINE_string(rocksdb_block_based_table_options,
              "{}",
              "json string of BlockBasedTableOptions, all keys and values are string");

DEFINE_int32(rocksdb_batch_size,
             4 * 1024,
             "default reserved bytes for one batch operation");
/*
 * For these un-supported string options as below, will need to specify them with gflag.
 */

// BlockBasedTable block_cache
DEFINE_int64(rocksdb_block_cache, 1024,
             "The default block cache size used in BlockBasedTable. The unit is MB");

DEFINE_bool(enable_partitioned_index_filter, false, "True for partitioned index filters");

DEFINE_string(rocksdb_compression, "snappy", "Compression algorithm used by RocksDB, "
                                             "options: no,snappy,lz4,lz4hc,zstd,zlib,bzip2");
DEFINE_string(rocksdb_compression_per_level, "", "Specify per level compression algorithm, "
                                                 "delimited by `:', ignored fields will be "
                                                 "replaced by FLAGS_rocksdb_compression. "
                                                 "e.g. \"no:no:lz4:lz4::zstd\" === "
                                                 "\"no:no:lz4:lz4:lz4:snappy:zstd:snappy\"");

DEFINE_bool(enable_rocksdb_statistics, false, "Whether or not to enable rocksdb's statistics");
DEFINE_string(rocksdb_stats_level, "kExceptHistogramOrTimers", "rocksdb statistics level");

DEFINE_int32(num_compaction_threads, 0,
            "Number of total compaction threads. 0 means unlimited.");

DEFINE_int32(rate_limit, 0,
            "write limit in bytes per sec. The unit is MB. 0 means unlimited.");

DEFINE_bool(enable_prefix_filtering, false,
            "Whether or not to enable rocksdb's prefix bloom filter.");
DEFINE_bool(enable_whole_key_filtering, true,
            "Whether or not to enable the whole key filtering.");
DEFINE_int32(prefix_length, 12,
            "The prefix length, default value is 12 bytes(PartitionID+VertexID).");

namespace nebula {
namespace kvstore {

class GraphPrefixTransform : public rocksdb::SliceTransform {
private:
    size_t prefix_len_;
    std::string name_;

public:
    explicit GraphPrefixTransform(size_t prefix_len)
        : prefix_len_(prefix_len),
        name_("nebula.GraphPrefix." + std::to_string(prefix_len_)) {}

    const char* Name() const override { return name_.c_str(); }

    rocksdb::Slice Transform(const rocksdb::Slice& src) const override {
        return rocksdb::Slice(src.data(), prefix_len_);
    }

    bool InDomain(const rocksdb::Slice& src) const override {
        return src.size() >= prefix_len_ && NebulaKeyUtils::isDataKey(
                folly::StringPiece(src.data(), 1));
    }
};

static rocksdb::Status initRocksdbCompression(rocksdb::Options &baseOpts) {
    static std::unordered_map<std::string, rocksdb::CompressionType> m = {
        { "no", rocksdb::kNoCompression },
        { "snappy", rocksdb::kSnappyCompression },
        { "lz4", rocksdb::kLZ4Compression },
        { "lz4hc", rocksdb::kLZ4HCCompression },
        { "zstd", rocksdb::kZSTD },
        { "zlib", rocksdb::kZlibCompression },
        { "bzip2", rocksdb::kBZip2Compression }
    };

    // Set the general compression algorithm
    {
        auto it = m.find(FLAGS_rocksdb_compression);
        if (it == m.end()) {
            LOG(ERROR) << "Unsupported compression type: " << FLAGS_rocksdb_compression;
            return rocksdb::Status::InvalidArgument();
        }
        baseOpts.compression = it->second;
    }
    if (FLAGS_rocksdb_compression_per_level.empty()) {
        return rocksdb::Status::OK();
    }

    // Set the per level compression algorithm, which will override the general one.
    // Given baseOpts.compression is lz4, "no:::::zstd" equals to "no:lz4:lz4:lz4:lz4:zstd:lz4"
    std::vector<std::string> compressions;
    folly::split(":", FLAGS_rocksdb_compression_per_level, compressions, false);
    compressions.resize(baseOpts.num_levels);
    baseOpts.compression_per_level.resize(baseOpts.num_levels);
    for (auto i = 0u; i < compressions.size(); i++) {
        if (compressions[i].empty()) {
            compressions[i] = FLAGS_rocksdb_compression;
        }
        auto it = m.find(compressions[i]);
        if (it == m.end()) {
            LOG(ERROR) << "Unsupported compression type: " << compressions[i];
            return rocksdb::Status::InvalidArgument();
        }
        baseOpts.compression_per_level[i] = it->second;
    }
    LOG(INFO) << "compression per level: " << folly::join(":", compressions);

    return rocksdb::Status::OK();
}

rocksdb::Status initRocksdbOptions(rocksdb::Options &baseOpts) {
    rocksdb::Status s;
    rocksdb::DBOptions dbOpts;
    rocksdb::ColumnFamilyOptions cfOpts;
    rocksdb::BlockBasedTableOptions bbtOpts;

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

    std::unordered_map<std::string, std::string> bbtOptsMap;
    if (!loadOptionsMap(bbtOptsMap, FLAGS_rocksdb_block_based_table_options)) {
        return rocksdb::Status::InvalidArgument();
    }
    s = GetBlockBasedTableOptionsFromMap(rocksdb::BlockBasedTableOptions(), bbtOptsMap,
                                         &bbtOpts, true);
    if (!s.ok()) {
        return s;
    }

    if (FLAGS_rocksdb_block_cache <= 0) {
        bbtOpts.no_block_cache = true;
    } else {
        static std::shared_ptr<rocksdb::Cache> blockCache
            = rocksdb::NewLRUCache(FLAGS_rocksdb_block_cache * 1024 * 1024, 8/*shard bits*/);
        bbtOpts.block_cache = blockCache;
    }
    if (FLAGS_num_compaction_threads > 0) {
        static std::shared_ptr<rocksdb::ConcurrentTaskLimiter> compaction_thread_limiter{
            rocksdb::NewConcurrentTaskLimiter("compaction", FLAGS_num_compaction_threads)};
        baseOpts.compaction_thread_limiter = compaction_thread_limiter;
    }
    if (FLAGS_rate_limit > 0) {
        static std::shared_ptr<rocksdb::RateLimiter> rate_limiter{
            rocksdb::NewGenericRateLimiter(FLAGS_rate_limit * 1024 * 1024)};
        baseOpts.rate_limiter = rate_limiter;
    }

    bbtOpts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    if (FLAGS_enable_partitioned_index_filter) {
        bbtOpts.index_type = rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
        bbtOpts.partition_filters = true;
        bbtOpts.cache_index_and_filter_blocks = true;
        bbtOpts.cache_index_and_filter_blocks_with_high_priority = true;
        bbtOpts.pin_l0_filter_and_index_blocks_in_cache =
            baseOpts.compaction_style == rocksdb::CompactionStyle::kCompactionStyleLevel;
    }
    if (FLAGS_enable_prefix_filtering) {
        baseOpts.prefix_extractor.reset(new GraphPrefixTransform(FLAGS_prefix_length));
    }
    bbtOpts.whole_key_filtering = FLAGS_enable_whole_key_filtering;
    baseOpts.table_factory.reset(NewBlockBasedTableFactory(bbtOpts));
    baseOpts.create_if_missing = true;
    return s;
}

bool loadOptionsMap(std::unordered_map<std::string, std::string> &map, const std::string& gflags) {
    Configuration conf;
    auto status = conf.parseFromString(gflags);
    if (!status.ok()) {
        return false;
    }
    conf.forEachItem([&map] (const std::string& key, const folly::dynamic& val) {
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

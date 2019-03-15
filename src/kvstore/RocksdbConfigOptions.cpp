/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "kvstore/RocksdbConfigOptions.h"

// [Version]
DEFINE_string(rocksdb_options_version,
              "5.15.10",
              "rocksdb options version.");

// [WAL]
DEFINE_bool(rocksdb_disable_wal,
              true,
              "rocksdb wal is close by default");

// [DBOptions]
DEFINE_string(rocksdb_db_options,
              "",
              "DBOptions, each option will be given "
              "as <option_name>:<option_value> separated by ;.");

// [CFOptions "default"]
DEFINE_string(rocksdb_column_family_options,
              "",
              "ColumnFamilyOptions, each option will be given "
              "as <option_name>:<option_value> separated by ;.");

//  [TableOptions/BlockBasedTable "default"]
DEFINE_string(rocksdb_block_based_table_options,
              "",
              "BlockBasedTableOptions, each option will be given "
              "as <option_name>:<option_value> separated by ;.");

/*
 * For these un-supported string options as below, will need to specify them with gflag.
 */
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

DEFINE_int64(write_buffer_size,
              67108864,
              "Amount of data to build up in memory (backed by an unsorted log "
              "on disk) before converting to a sorted on-disk file.");

// BlockBasedTable block_cache
DEFINE_int64(block_cache, 4,
             "BlockBasedTable:block_cache : MB");

namespace nebula {
namespace kvstore {
static RocksdbConfigOptions *rco;
std::once_flag createOptionsFlag;
RocksdbConfigOptions::RocksdbConfigOptions()  {
    rco = this;
    LOG(INFO) << "begin create rocksdb options... ";
}

RocksdbConfigOptions::~RocksdbConfigOptions() {
}

rocksdb::Options RocksdbConfigOptions::getRocksdbOptions(
        const std::string &dataPath) {
    rocksdb::Status status;
    std::call_once(createOptionsFlag, [&status] () {
        status = rco->createRocksdbEngineOptions();
        if (!status.ok()) {
            LOG(FATAL) << "Create rocksdb options error : " << status.ToString();
        }
    });

    // check options compatibility
    status = rco->checkOptionsCompatibility(dataPath);
    if (!status.ok()) {
        LOG(FATAL) << "Check options compatibility error : " << status.ToString();
    }
    return baseOpts;
}

rocksdb::Status RocksdbConfigOptions::createRocksdbEngineOptions() {
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

    return rocksdb::Status::OK();
}

rocksdb::Status RocksdbConfigOptions::checkOptionsCompatibility(const std::string &dataPath) {
    std::vector<rocksdb::ColumnFamilyDescriptor> cfDesc;
    cfDesc.push_back({rocksdb::kDefaultColumnFamilyName, cfOpts});
    rocksdb::Status s = rocksdb::CheckOptionsCompatibility(
            dataPath,
            rocksdb::Env::Default(),
            dbOpts, cfDesc);
    if (s.code() == rocksdb::Status::kNotFound) {
        LOG(INFO) << "new rocksdb instance, no compatibility "
                     "checks are required. data path is : "
                  << dataPath;
        return rocksdb::Status::OK();
    }

    if (!s.ok()) {
        return s;
    }

    return rocksdb::Status::OK();
}

rocksdb::Status RocksdbConfigOptions::initRocksdbOptions() {
    rocksdb::Status s;
    s = GetDBOptionsFromString(rocksdb::DBOptions(),
            FLAGS_rocksdb_db_options, &dbOpts);
    if (!s.ok()) {
        return s;
    }

    s = GetColumnFamilyOptionsFromString(rocksdb::ColumnFamilyOptions(),
            FLAGS_rocksdb_column_family_options, &cfOpts);

    if (!s.ok()) {
        return s;
    }

    baseOpts = rocksdb::Options(dbOpts, cfOpts);

    s = GetBlockBasedTableOptionsFromString(rocksdb::BlockBasedTableOptions(),
            FLAGS_rocksdb_block_based_table_options, &bbtOpts);

    if (!s.ok()) {
        return s;
    }

    if (!setupBlockCache()) {
        return rocksdb::Status::Aborted("rocksdb option block_cache error");
    }

    if (!setupMemtableFactory()) {
        return rocksdb::Status::Aborted("rocksdb option memtable_factory error");
    }

    if (!setupPrefixExtractor()) {
        return rocksdb::Status::Aborted("rocksdb option prefix_extractor error");
    }

    baseOpts.table_factory.reset(NewBlockBasedTableFactory(bbtOpts));
    baseOpts.create_if_missing = true;
    return rocksdb::Status::OK();
}

typedef enum { ERRORT,
               NULLPTR,
               SKIPLIST,
               VECTOR,
               HASHSKIPLIST,
               HASHLINKLIST,
               CUCKOO } MemToken;

MemToken memFactoryLexer(const char *s) {
    static struct entry_s {
        const char *key;
        MemToken token;
    } token_table[] = {
            { "nullptr", NULLPTR },
            { "skiplist", SKIPLIST },
            { "vector" , VECTOR },
            { "hashskiplist" , HASHSKIPLIST },
            { "hashlinklist", HASHLINKLIST },
            { "cuckoo", CUCKOO },
            {"", ERRORT},
    };
    struct entry_s *p = token_table;
    for (; p->key != NULL && std::strcmp(p->key, s) != 0; ++p) continue;
    return p->token;
}

bool RocksdbConfigOptions::setupMemtableFactory() {
    switch (memFactoryLexer(FLAGS_memtable_factory.c_str())) {
        case NULLPTR :
            return true;
        case SKIPLIST :
            baseOpts.memtable_factory.reset(new rocksdb::SkipListFactory);
            break;
        case VECTOR :
            baseOpts.memtable_factory.reset(new rocksdb::VectorRepFactory);
            break;
        case HASHSKIPLIST :
            baseOpts.memtable_factory.reset(rocksdb::NewHashSkipListRepFactory(
                    FLAGS_bucket_count, FLAGS_hashskiplist_height,
                    FLAGS_hashskiplist_branching_factor));
            break;
        case HASHLINKLIST :
            baseOpts.memtable_factory.reset(rocksdb::NewHashLinkListRepFactory(
                    FLAGS_bucket_count, FLAGS_huge_page_tlb_size,
                    FLAGS_bucket_entries_logging_threshold,
                    FLAGS_if_log_bucket_dist_when_flash, FLAGS_threshold_use_skiplist));
            break;
        case CUCKOO :
            baseOpts.memtable_factory.reset(rocksdb::NewHashCuckooRepFactory(
                    FLAGS_write_buffer_size, FLAGS_average_data_size,
                    static_cast<uint32_t>(FLAGS_hash_function_count)));
            break;
        case ERRORT : {
            LOG(ERROR) << "Unknown memtable_factory : "
            << FLAGS_memtable_factory.c_str();
            return false;
        }
    }
    return true;
}

bool RocksdbConfigOptions::setupPrefixExtractor() {
    baseOpts.prefix_extractor.reset(
            rocksdb::NewFixedPrefixTransform(FLAGS_prefix_length));
    return true;
}

bool RocksdbConfigOptions::setupBlockCache() {
    bbtOpts.block_cache = rocksdb::NewLRUCache(FLAGS_block_cache * 1024 * 1024);
    return true;
}

}  // namespace kvstore
}  // namespace nebula

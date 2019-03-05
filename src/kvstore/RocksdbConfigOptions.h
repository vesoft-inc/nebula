/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef NEBULA_GRAPH_ROCKSDBCONFIGOPTIONS_H
#define NEBULA_GRAPH_ROCKSDBCONFIGOPTIONS_H

#include <gtest/gtest_prod.h>
#include "base/Base.h"
#include "rocksdb/db.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/slice_transform.h"
#include "kvstore/Common.h"

#define KVSTORE_CONFIG_FLAG(engineflag) \
  { ""#engineflag"", FLAGS_##engineflag }

namespace nebula {
namespace kvstore {
static rocksdb::Options baseOpts;
static rocksdb::DBOptions dbOpts;
static rocksdb::ColumnFamilyOptions cfOpts;
static rocksdb::BlockBasedTableOptions bbtOpts;
static std::unordered_map<std::string, std::string> dbMap;
static std::unordered_map<std::string, std::string> cfMap;
static std::unordered_map<std::string, std::string> bbtMap;

class RocksdbConfigOptions final {
    FRIEND_TEST(RocksdbEngineOptionsTest, versionTest);
    FRIEND_TEST(RocksdbEngineOptionsTest, createOptionsTest);
    FRIEND_TEST(RocksdbEngineOptionsTest, getOptionValueTest);
    FRIEND_TEST(RocksdbEngineOptionsTest, memtableTest);

public:
    enum ROCKSDB_OPTION_TYPE {
        DBOPT = 1,
        CFOPT,
        TABLEOPT };

    RocksdbConfigOptions();

    ~RocksdbConfigOptions();

    static bool getRocksdbEngineOptionValue(ROCKSDB_OPTION_TYPE optType,
            const char *opt_name, std::string &optValue);
    static bool getKVPaths(std::string dataPaths,
            std::string walPaths, KVPaths &kvPaths);
    static rocksdb::Options getRocksdbOptions(const std::string &dataPath,
                                              const std::string &walPath,
                                              bool ignoreUnknownOptions,
                                              bool inputStringsEscaped);

private:
    rocksdb::Status initRocksdbOptions(bool ignoreUnknownOptions, bool inputStringsEscaped);
    rocksdb::Status checkOptionsCompatibility(const std::string &dataPath);
    rocksdb::Status createRocksdbEngineOptions(bool ignoreUnknownOptions,
                                               bool inputStringsEscaped);
    bool setupMemtableFactory();
    bool setupCompactionFilterFactory();
    bool setupPrefixExtractor();
    bool setupComparator();
    bool setupMergeOperator();
    bool setupCompactionFilter();
    bool setupBlockCache();
    void load_option_maps();
};
}  // namespace kvstore
}  // namespace nebula
#endif  // NEBULA_GRAPH_ROCKSDBCONFIGOPTIONS_H

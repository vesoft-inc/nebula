/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_COMPACTIONFILTER_H_
#define KVSTORE_COMPACTIONFILTER_H_

#include <rocksdb/compaction_filter.h>

#include "common/base/Base.h"
#include "common/time/WallClock.h"
#include "kvstore/Common.h"

DECLARE_int32(custom_filter_interval_secs);

namespace nebula {
namespace kvstore {

/**
 * @brief CompactionFilter, built by CompactionFilterFactory
 */
class KVCompactionFilter final : public rocksdb::CompactionFilter {
 public:
  /**
   * @brief Construct a new KVCompactionFilter object
   *
   * @param spaceId
   * @param kvFilter A wrapper of filter function
   */
  KVCompactionFilter(GraphSpaceID spaceId, std::unique_ptr<KVFilter> kvFilter)
      : spaceId_(spaceId), kvFilter_(std::move(kvFilter)) {}

  /**
   * @brief whether remove the key during compaction
   *
   * @param level Levels of key in rocksdb
   * @param key Rocksdb key
   * @param val Rocksdb val
   * @return true Key will not be removed
   * @return false Key will be removed
   */
  bool Filter(int level,
              const rocksdb::Slice& key,
              const rocksdb::Slice& val,
              std::string*,
              bool*) const override {
    return kvFilter_->filter(level,
                             spaceId_,
                             folly::StringPiece(key.data(), key.size()),
                             folly::StringPiece(val.data(), val.size()));
  }

  const char* Name() const override {
    return "KVCompactionFilter";
  }

 private:
  GraphSpaceID spaceId_;
  std::unique_ptr<KVFilter> kvFilter_;
};

/**
 * @brief CompactionFilterFactory, built by CompactionFilterFactoryBuilder
 */
class KVCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  explicit KVCompactionFilterFactory(GraphSpaceID spaceId) : spaceId_(spaceId) {}

  virtual ~KVCompactionFilterFactory() = default;

  /**
   * @brief Create a Compaction Filter object, called by rocksdb when doing compaction
   *
   * @param context Information about compaction
   * @return std::unique_ptr<rocksdb::CompactionFilter>
   */
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    if (context.is_full_compaction || context.is_manual_compaction) {
      LOG(INFO) << "Do full/manual compaction!";
    } else {
      // No worry, by default flush will not go through the custom compaction filter.
      // See CompactionFilterFactory::ShouldFilterTableFileCreation.
      LOG(INFO) << "Do automatic or periodic compaction!";
    }
    return std::make_unique<KVCompactionFilter>(spaceId_, createKVFilter());
  }

  const char* Name() const override {
    return "KVCompactionFilterFactory";
  }

  virtual std::unique_ptr<KVFilter> createKVFilter() = 0;

 private:
  GraphSpaceID spaceId_;
  int32_t lastRunCustomFilterTimeSec_ = 0;
};

/**
 * @brief CompactionFilterFactoryBuilder is a wrapper to build rocksdb CompactionFilterFactory,
 * implemented by storage
 */
class CompactionFilterFactoryBuilder {
 public:
  CompactionFilterFactoryBuilder() = default;

  virtual ~CompactionFilterFactoryBuilder() = default;

  virtual std::shared_ptr<KVCompactionFilterFactory> buildCfFactory(GraphSpaceID spaceId) = 0;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_COMPACTIONFILTER_H_

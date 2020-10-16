/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_COMPACTIONFILTER_H_
#define KVSTORE_COMPACTIONFILTER_H_

#include "common/base/Base.h"
#include "common/time/WallClock.h"
#include <rocksdb/compaction_filter.h>
#include "kvstore/Common.h"

DECLARE_int32(custom_filter_interval_secs);

namespace nebula {
namespace kvstore {

class KVCompactionFilter final : public rocksdb::CompactionFilter {
public:
    KVCompactionFilter(GraphSpaceID spaceId, std::unique_ptr<KVFilter> kvFilter)
        : spaceId_(spaceId)
        , kvFilter_(std::move(kvFilter)) {
    }

    bool Filter(int,
                const rocksdb::Slice& key,
                const rocksdb::Slice& val,
                std::string*,
                bool*) const override {
        return kvFilter_->filter(spaceId_,
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

class KVCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
public:
    explicit KVCompactionFilterFactory(GraphSpaceID spaceId) : spaceId_(spaceId) {}

    virtual ~KVCompactionFilterFactory() = default;

    std::unique_ptr<rocksdb::CompactionFilter>
    CreateCompactionFilter(const rocksdb::CompactionFilter::Context& context) override {
        auto now = time::WallClock::fastNowInSec();
        if (context.is_full_compaction || context.is_manual_compaction) {
            LOG(INFO) << "Do full/manual compaction!";
            lastRunCustomFilterTimeSec_ = now;
            return std::make_unique<KVCompactionFilter>(spaceId_, createKVFilter());
        } else {
            if (FLAGS_custom_filter_interval_secs >= 0
                    && now - lastRunCustomFilterTimeSec_ > FLAGS_custom_filter_interval_secs) {
                LOG(INFO) << "Do custom minor compaction!";
                lastRunCustomFilterTimeSec_ = now;
                return std::make_unique<KVCompactionFilter>(spaceId_, createKVFilter());
            }
            LOG(INFO) << "Do default minor compaction!";
            return std::unique_ptr<rocksdb::CompactionFilter>(nullptr);
        }
    }

    const char* Name() const override {
        return "KVCompactionFilterFactory";
    }

    virtual std::unique_ptr<KVFilter> createKVFilter() = 0;

private:
    GraphSpaceID spaceId_;
    int32_t lastRunCustomFilterTimeSec_ = 0;
};

class CompactionFilterFactoryBuilder {
public:
    CompactionFilterFactoryBuilder() = default;

    virtual ~CompactionFilterFactoryBuilder() = default;

    virtual std::shared_ptr<KVCompactionFilterFactory>
    buildCfFactory(GraphSpaceID spaceId) = 0;
};

}   // namespace kvstore
}   // namespace nebula
#endif   // KVSTORE_COMPACTIONFILTER_H_

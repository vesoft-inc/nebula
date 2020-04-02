/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_COMPACTIONFILTER_H_
#define KVSTORE_COMPACTIONFILTER_H_

#include "base/Base.h"
#include <rocksdb/compaction_filter.h>
#include "utils/NebulaKeyUtils.h"
#include "time/WallClock.h"
#include "kvstore/Common.h"

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
    KVCompactionFilterFactory(GraphSpaceID spaceId, int32_t customFilterIntervalSecs):
        spaceId_(spaceId), customFilterIntervalSecs_(customFilterIntervalSecs) {}

    virtual ~KVCompactionFilterFactory() = default;

    std::unique_ptr<rocksdb::CompactionFilter>
    CreateCompactionFilter(const rocksdb::CompactionFilter::Context& context) override {
        auto now = time::WallClock::fastNowInSec();
        if (context.is_full_compaction) {
            LOG(INFO) << "Do full compaction!";
            lastRunCustomFilterTimeSec_ = now;
            return std::make_unique<KVCompactionFilter>(spaceId_, createKVFilter());
        } else {
            if (customFilterIntervalSecs_ >= 0
                    && now - lastRunCustomFilterTimeSec_ > customFilterIntervalSecs_) {
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
    // -1 means always do default minor compaction.
    // 0 means always do custom minor compaction
    int32_t customFilterIntervalSecs_ = 0;
    int32_t lastRunCustomFilterTimeSec_ = 0;
};

class CompactionFilterFactoryBuilder {
public:
    CompactionFilterFactoryBuilder() = default;

    virtual ~CompactionFilterFactoryBuilder() = default;

    virtual std::shared_ptr<KVCompactionFilterFactory>
    buildCfFactory(GraphSpaceID spaceId, int32_t customFilterIntervalSecs) = 0;
};

}   // namespace kvstore
}   // namespace nebula
#endif   // KVSTORE_COMPACTIONFILTER_H_

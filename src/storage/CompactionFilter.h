/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_COMPACTIONFILTER_H_
#define STORAGE_COMPACTIONFILTER_H_

#include "base/Base.h"
#include <rocksdb/compaction_filter.h>

namespace nebula {
namespace storage {

class NebulaCompactionFilterFactory final : public rocksdb::CompactionFilterFactory {
public:
    std::unique_ptr<rocksdb::CompactionFilter>
    CreateCompactionFilter(const rocksdb::CompactionFilter::Context& context) override {
        return std::make_unique<NebulaCompactionFilter>();
    }

    const char* Name() const override {
        return "NebulaCompactionFilterFactory";
    }
};

class NebulaCompactionFilter final : public rocksdb::CompactionFilter {
public:
    bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& old_val,
                std::string* new_val, bool* value_changed) const override {
        return false;
    }

    const char* Name() const override {
        return "NebulaCompactionFilter";
    }
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_COMPACTIONFILTER_H_


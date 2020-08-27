/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"

namespace nebula {
namespace kvstore {

class EventListener : public rocksdb::EventListener {
public:
    void OnStallConditionsChanged(const rocksdb::WriteStallInfo& info) override {
        LOG(INFO) << "Rocksdb write stall condition changed, column family: " << info.cf_name
                  << ", pre " << static_cast<int32_t>(info.condition.prev)
                  << ", cur " << static_cast<int32_t>(info.condition.cur);
    }

    void OnCompactionBegin(rocksdb::DB*, const rocksdb::CompactionJobInfo& info) override {
        LOG(INFO) << "Rocksdb compact column family: " << info.cf_name
                  << " because of " << static_cast<int32_t>(info.compaction_reason)
                  << ", status: " << info.status.ToString()
                  << ", compacted " << info.input_files.size()
                  << " files into " << info.output_files.size()
                  << ", base level is " << info.base_input_level
                  << ", output level is " << info.output_level;
    }

    void OnCompactionCompleted(rocksdb::DB*, const rocksdb::CompactionJobInfo& info) override {
        LOG(INFO) << "Rocksdb compact column family: " << info.cf_name
                  << " because of " << static_cast<int32_t>(info.compaction_reason)
                  << ", status: " << info.status.ToString()
                  << ", compacted " << info.input_files.size()
                  << " files into " << info.output_files.size()
                  << ", base level is " << info.base_input_level
                  << ", output level is " << info.output_level;
    }
};

}  // namespace kvstore
}  // namespace nebula

/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_MERGEOPERATOR_H_
#define KVSTORE_MERGEOPERATOR_H_

#include "common/base/Base.h"
#include <rocksdb/merge_operator.h>

namespace nebula {
namespace storage {

class NebulaOperator : public rocksdb::MergeOperator {
public:
    const char* Name() const override {
        return "NebulaMergeOperator";
    }

private:
    bool FullMergeV2(const MergeOperationInput& merge_in,
                     MergeOperationOutput* merge_out) const override {
        UNUSED(merge_in);
        UNUSED(merge_out);
        LOG(ERROR) << "NebulaMergeOperator not supported yet";
        return false;
    }

    bool PartialMerge(const rocksdb::Slice& key, const rocksdb::Slice& left_operand,
                      const rocksdb::Slice& right_operand, std::string* new_value,
                      rocksdb::Logger* logger) const override {
        UNUSED(key);
        UNUSED(left_operand);
        UNUSED(right_operand);
        UNUSED(new_value);
        UNUSED(logger);
        LOG(ERROR) << "NebulaMergeOperator not supported yet";
        return false;
    }
};


}  // namespace storage
}  // namespace nebula
#endif  // KVSTORE_MERGEOPERATOR_H_


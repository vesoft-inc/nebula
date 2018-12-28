/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef KVSTORE_MERGEOPERATOR_H_
#define KVSTORE_MERGEOPERATOR_H_

#include "base/Base.h"
#include <rocksdb/merge_operator.h>

namespace nebula {
namespace kvstore {

class NebulaOperator : public rocksdb::MergeOperator {
public:
    const char* Name() const override {
        return "NebulaOperator";
    }

private:
    // Default implementations of the MergeOperator functions
    bool FullMergeV2(const MergeOperationInput& merge_in,
                     MergeOperationOutput* merge_out) const override {
        LOG(FATAL) << "NOT Implement!";
        return false;
    }

    bool PartialMerge(const Slice& key, const Slice& left_operand,
                      const Slice& right_operand, std::string* new_value,
                      Logger* logger) const override {
        LOG(FATL) << "NOT implement!";
        return false;
    }
};


}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_MERGEOPERATOR_H_


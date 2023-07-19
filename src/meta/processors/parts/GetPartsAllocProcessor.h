/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETPARTSALLOCPROCESSOR_H_
#define META_GETPARTSALLOCPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Get partitions distribution and terms for given space, used for meta client loading data.
 *
 */
class GetPartsAllocProcessor : public BaseProcessor<cpp2::GetPartsAllocResp> {
 public:
  static GetPartsAllocProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetPartsAllocProcessor(kvstore);
  }

  void process(const cpp2::GetPartsAllocReq& req);

 private:
  explicit GetPartsAllocProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetPartsAllocResp>(kvstore) {}

  /**
   * @brief Get leader peer's term for each partition
   *
   * @param space space id
   * @return std::unordered_map<PartitionID, TermID>
   */
  std::unordered_map<PartitionID, TermID> getTerm(GraphSpaceID space);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETPARTSALLOCPROCESSOR_H_

/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETSTATISPROCESSOR_H_
#define META_GETSTATISPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Get stats of a job, return error if the job is running or failed
 */
class GetStatsProcessor : public BaseProcessor<cpp2::GetStatsResp> {
 public:
  static GetStatsProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetStatsProcessor(kvstore);
  }

  void process(const cpp2::GetStatsReq& req);

 private:
  explicit GetStatsProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetStatsResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETSTATISPROCESSOR_H_

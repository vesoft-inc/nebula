/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_GETSEGMENTIDPROCESSOR_H_
#define META_GETSEGMENTIDPROCESSOR_H_

#include "common/base/Base.h"
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {
class GetSegmentIdProcessor : public BaseProcessor<cpp2::GetSegmentIdResp> {
 public:
  static GetSegmentIdProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetSegmentIdProcessor(kvstore);
  }

  void process(const cpp2::GetSegmentIdReq& req);

 private:
  explicit GetSegmentIdProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetSegmentIdResp>(kvstore) {
    static bool once = [this]() {
      std::vector<kvstore::KV> kv = {{idKey, std::to_string(0)}};
      doPut(kv);
      return true;
    }();
    UNUSED(once);
  }

  void doPut(std::vector<kvstore::KV> data);

  inline static const string idKey = "segment_cur_id";
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETSEGMENTIDPROCESSOR_H_

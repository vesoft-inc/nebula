/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTTAGINDEXESPROCESSOR_H
#define META_LISTTAGINDEXESPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Get all tag index items by scanning index prefix and then filter out the
 *        indexes with tag type.
 *
 */
class ListTagIndexesProcessor : public BaseProcessor<cpp2::ListTagIndexesResp> {
 public:
  static ListTagIndexesProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListTagIndexesProcessor(kvstore);
  }

  void process(const cpp2::ListTagIndexesReq& req);

 private:
  explicit ListTagIndexesProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListTagIndexesResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTTAGINDEXESPROCESSOR_H

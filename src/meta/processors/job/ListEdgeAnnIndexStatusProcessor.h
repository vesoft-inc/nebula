/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTEDGEANNINDEXSTATUSPROCESSOR_H
#define META_LISTEDGEANNINDEXSTATUSPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Show status all build-edge-ann-index jobs
 */
class ListEdgeAnnIndexStatusProcessor : public BaseProcessor<cpp2::ListIndexStatusResp> {
 public:
  static ListEdgeAnnIndexStatusProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListEdgeAnnIndexStatusProcessor(kvstore);
  }

  void process(const cpp2::ListIndexStatusReq& req);

 private:
  explicit ListEdgeAnnIndexStatusProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListIndexStatusResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTEDGEINDEXSTATUSPROCESSOR_H

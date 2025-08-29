/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_CREATEEDGEANNINDEXPROCESSOR_H
#define META_CREATEEDGEANNINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Create edge ann index on given edge field. This processor has similar logic with the
 *        CreateEdgeIndexProcessor. It only check if the edge ann index could be built
 *        and then create a edge ann index item to save all the edge ann index meta.
 *        After edge ann index created, any vertex inserted satisfying the edge ann index and fields
 *        will trigger corresponding index built.
 *
 */
class CreateEdgeAnnIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static CreateEdgeAnnIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new CreateEdgeAnnIndexProcessor(kvstore);
  }

  void process(const cpp2::CreateEdgeAnnIndexReq& req);

 private:
  explicit CreateEdgeAnnIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATEEDGEINDEXPROCESSOR_H

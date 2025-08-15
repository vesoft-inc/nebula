/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_CREATETAGANNINDEXPROCESSOR_H
#define META_CREATETAGANNINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Create tag index on given tag fields. This processor has similar logic with the
 *        CreateEdgeIndexProcessor. It only check if the tag index could be built
 *        and then create a tag index item to save all the tag index meta.
 *        After tag index created, any vertex inserted satisfying the tag and fields
 *        will trigger corresponding index built.
 *
 */
class CreateTagAnnIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static CreateTagAnnIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new CreateTagAnnIndexProcessor(kvstore);
  }

  void process(const cpp2::CreateTagAnnIndexReq& req);

 private:
  explicit CreateTagAnnIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATETAGINDEXPROCESSOR_H

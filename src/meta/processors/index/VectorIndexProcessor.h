// Copyright (c) 2025 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef META_PROCESSORS_VECTORINDEXPROCESSOR_H_
#define META_PROCESSORS_VECTORINDEXPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class CreateVectorIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static CreateVectorIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new CreateVectorIndexProcessor(kvstore);
  }

  void process(const cpp2::CreateVectorIndexReq& req);

 private:
  explicit CreateVectorIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class DropVectorIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropVectorIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropVectorIndexProcessor(kvstore);
  }

  void process(const cpp2::DropVectorIndexReq& req);

 private:
  explicit DropVectorIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class ListVectorIndexesProcessor : public BaseProcessor<cpp2::ListVectorIndexesResp> {
 public:
  static ListVectorIndexesProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListVectorIndexesProcessor(kvstore);
  }

  void process(const cpp2::ListVectorIndexesReq&);

 private:
  explicit ListVectorIndexesProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListVectorIndexesResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_PROCESSORS_VECTORINDEXPROCESSOR_H_

/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef META_FTINDEXPROCESSOR_H_
#define META_FTINDEXPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Create fulltext index, only inserting the meta info to the meta kv store.
 *        The user should also sign in elasticsearch client and set up listener before really use
 * it.
 *
 */
class CreateFTIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static CreateFTIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new CreateFTIndexProcessor(kvstore);
  }

  void process(const cpp2::CreateFTIndexReq& req);

 private:
  explicit CreateFTIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

/**
 * @brief Drop fulltext index, only deleting the fulltext index item from meta kv store.
 *
 */
class DropFTIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropFTIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropFTIndexProcessor(kvstore);
  }

  void process(const cpp2::DropFTIndexReq& req);

 private:
  explicit DropFTIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

/**
 * @brief Get all the fulltext index info by scaning fulltext index prefix.
 *
 */
class ListFTIndexesProcessor : public BaseProcessor<cpp2::ListFTIndexesResp> {
 public:
  static ListFTIndexesProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListFTIndexesProcessor(kvstore);
  }

  void process(const cpp2::ListFTIndexesReq&);

 private:
  explicit ListFTIndexesProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListFTIndexesResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_FTINDEXPROCESSOR_H_

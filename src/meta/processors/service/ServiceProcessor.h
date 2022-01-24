/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_SERVICEPROCESSOR_H_
#define META_SERVICEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Sign in external service info. It is only used in listeners now, such as ES
 *        service servce info.
 *
 */
class SignInServiceProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static SignInServiceProcessor* instance(kvstore::KVStore* kvstore) {
    return new SignInServiceProcessor(kvstore);
  }

  void process(const cpp2::SignInServiceReq& req);

 private:
  explicit SignInServiceProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

/**
 * @brief Sign out external service info when it is not used any more.
 *
 */
class SignOutServiceProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static SignOutServiceProcessor* instance(kvstore::KVStore* kvstore) {
    return new SignOutServiceProcessor(kvstore);
  }

  void process(const cpp2::SignOutServiceReq& req);

 private:
  explicit SignOutServiceProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

/**
 * @brief Get all service signed in for given external service type. Though
 *        the function name is List*, it is indeed getting signed in info.
 *
 */
class ListServiceClientsProcessor : public BaseProcessor<cpp2::ListServiceClientsResp> {
 public:
  static ListServiceClientsProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListServiceClientsProcessor(kvstore);
  }

  void process(const cpp2::ListServiceClientsReq& req);

 private:
  explicit ListServiceClientsProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListServiceClientsResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SERVICEPROCESSOR_H_

/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTENERPROCESSOR_H_
#define META_LISTENERPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Add typed listeners for given space, assign one listener for each part round robin.
 *        Notice that in each space, listeners could be only added once now.
 *        But, of course, you could remove them and re-add new ones.
 *        It will use heartbeat to instruct the relative storaged to add listeners physically.
 *
 */
class AddListenerProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static AddListenerProcessor* instance(kvstore::KVStore* kvstore) {
    return new AddListenerProcessor(kvstore);
  }

  void process(const cpp2::AddListenerReq& req);

 private:
  explicit AddListenerProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

/**
 * @brief Remove added listener information. Return failed if not found.
 *
 */
class RemoveListenerProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static RemoveListenerProcessor* instance(kvstore::KVStore* kvstore) {
    return new RemoveListenerProcessor(kvstore);
  }

  void process(const cpp2::RemoveListenerReq& req);

 private:
  explicit RemoveListenerProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

/**
 * @brief List all listeners for given spaces and listener type.
 *        And will fill the listener ative state.
 *
 */
class ListListenerProcessor : public BaseProcessor<cpp2::ListListenerResp> {
 public:
  static ListListenerProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListListenerProcessor(kvstore);
  }

  void process(const cpp2::ListListenerReq& req);

 private:
  explicit ListListenerProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListListenerResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // META_LISTENERPROCESSOR_H_

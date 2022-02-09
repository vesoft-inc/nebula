/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_LISTENER_FACTORY_H_
#define KVSTORE_LISTENER_FACTORY_H_

#include "kvstore/Listener.h"
#include "kvstore/plugins/elasticsearch/ESListener.h"

namespace nebula {
namespace kvstore {

/**
 * @brief Factory to build listener
 */
class ListenerFactory {
 public:
  template <typename... Args>
  /**
   * @brief Create a Listener object
   *
   * @param type Type of listener
   * @param args Other parameters
   * @return std::shared_ptr<Listener>
   */
  static std::shared_ptr<Listener> createListener(meta::cpp2::ListenerType type, Args&&... args) {
    if (type == meta::cpp2::ListenerType::ELASTICSEARCH) {
      return std::make_shared<ESListener>(std::forward<Args>(args)...);
    }
    LOG(FATAL) << "Should not reach here";
    return nullptr;
  }
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_LISTENER_FACTORY_H_

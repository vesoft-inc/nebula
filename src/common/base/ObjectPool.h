/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_BASE_OBJECTPOOL_H_
#define COMMON_BASE_OBJECTPOOL_H_

#include <folly/SpinLock.h>

#include <boost/core/noncopyable.hpp>
#include <functional>
#include <list>
#include <type_traits>

#include "common/base/Logging.h"
#include "common/cpp/helpers.h"

namespace nebula {

class Expression;

typedef std::lock_guard<folly::SpinLock> SLGuard;

class ObjectPool final : private boost::noncopyable, private cpp::NonMovable {
 public:
  ObjectPool() {}

  ~ObjectPool() = default;

  void clear() {
    SLGuard g(lock_);
    objects_.clear();
  }

  template <typename T>
  T *add(T *obj) {
    if constexpr (std::is_base_of<Expression, T>::value) {
      VLOG(3) << "New expression added into pool: " << obj->toString();
    }
    SLGuard g(lock_);
    objects_.emplace_back(obj);
    return obj;
  }

  template <typename T, typename... Args>
  T *makeAndAdd(Args &&... args) {
    return add(new T(std::forward<Args>(args)...));
  }

  bool empty() const {
    return objects_.empty();
  }

 private:
  // Holder the ownership of the any object
  class OwnershipHolder {
   public:
    template <typename T>
    explicit OwnershipHolder(T *obj)
        : obj_(obj), deleteFn_([](void *p) { delete reinterpret_cast<T *>(p); }) {}

    ~OwnershipHolder() {
      deleteFn_(obj_);
    }

   private:
    void *obj_;
    std::function<void(void *)> deleteFn_;
  };

  std::list<OwnershipHolder> objects_;

  folly::SpinLock lock_;
};

}  // namespace nebula

#endif  // COMMON_BASE_OBJECTPOOL_H_

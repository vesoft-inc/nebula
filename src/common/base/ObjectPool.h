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

#include "common/base/Arena.h"
#include "common/base/Logging.h"
#include "common/cpp/helpers.h"

namespace nebula {

class Expression;

typedef std::lock_guard<folly::SpinLock> SLGuard;

class ObjectPool final : private boost::noncopyable, private cpp::NonMovable {
 public:
  ObjectPool() {}

  ~ObjectPool() {
    clear();
  }

  void clear() {
    SLGuard g(lock_);
    objects_.clear();
  }

  template <typename T, typename... Args>
  T *makeAndAdd(Args &&... args) {
    lock_.lock();
    void *ptr = arena_.allocateAligned(sizeof(T));
    lock_.unlock();
    return add(new (ptr) T(std::forward<Args>(args)...));
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
        : obj_(obj), deleteFn_([](void *p) { reinterpret_cast<T *>(p)->~T(); }) {}

    ~OwnershipHolder() {
      deleteFn_(obj_);
    }

   private:
    void *obj_;
    std::function<void(void *)> deleteFn_;
  };

  template <typename T>
  T *add(T *obj) {
    SLGuard g(lock_);
    objects_.emplace_back(obj);
    return obj;
  }

  std::list<OwnershipHolder> objects_;
  Arena arena_;

  folly::SpinLock lock_;
};

}  // namespace nebula

#endif  // COMMON_BASE_OBJECTPOOL_H_

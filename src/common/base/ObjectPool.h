/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_OBJECTPOOL_H_
#define UTIL_OBJECTPOOL_H_

#include <functional>
#include <list>
#include <type_traits>

#include <folly/SpinLock.h>

#include "common/cpp/helpers.h"
#include "common/base/Logging.h"

namespace nebula {

class Expression;

class ObjectPool final : private cpp::NonCopyable, private cpp::NonMovable {
public:
    ObjectPool() {}

    ~ObjectPool() = default;

    void clear() {
        folly::SpinLockGuard g(lock_);
        objects_.clear();
    }

    template <typename T>
    T *add(T *obj) {
        if constexpr (std::is_same_v<T, Expression>) {
            VLOG(3) << "New expression added into pool: " << obj->toString();
        }
        folly::SpinLockGuard g(lock_);
        objects_.emplace_back(obj);
        return obj;
    }

    template <typename T, typename... Args>
    T *makeAndAdd(Args&&... args) {
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

}   // namespace nebula

#endif   // UTIL_OBJECTPOOL_H_

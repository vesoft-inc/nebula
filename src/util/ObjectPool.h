/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_OBJECTPOOL_H_
#define UTIL_OBJECTPOOL_H_

#include <functional>
#include <list>

#include <folly/SpinLock.h>

#include "common/cpp/helpers.h"

namespace nebula {

class ObjectPool final : private cpp::NonCopyable, private cpp::NonMovable {
public:
    ObjectPool() {}

    ~ObjectPool() {
        clear();
    }

    void clear() {
        folly::SpinLockGuard g(lock_);
        for (auto &e : objects_) {
            e.deleteFn(e.obj);
        }
        objects_.clear();
    }

    template <typename T>
    T *add(T *obj) {
        folly::SpinLockGuard g(lock_);
        objects_.emplace_back(Element{obj, [](void *p) { delete reinterpret_cast<T *>(p); }});
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
    struct Element {
        void *obj;
        std::function<void(void *)> deleteFn;
    };

    std::list<Element> objects_;

    folly::SpinLock lock_;
};

}   // namespace nebula

#endif   // UTIL_OBJECTPOOL_H_

/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef COMMON_CONCURRENT_THREAD_THREADLOCALPTR_H_
#define COMMON_CONCURRENT_THREAD_THREADLOCALPTR_H_

#include <unistd.h>
#include <pthread.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include "common/concurrent/thread/NamedThread.h"
#include "common/cpp/helpers.h"

/**
 * `ThreadLocalPtr' is based on pthread's thread-specific data key.
 * Beside the normal TLS capability, `ThreadLocalPtr' allows us to collect
 * all of other threads' TLS data, which is usually needed in the udpate-mostly,
 * read-rarely occasions.
 *
 * Please refer to concurrent/test/ThreadLocalPtrTest.cpp for examples.
 */

namespace vesoft {
namespace concurrent {

template <typename T>
class ThreadLocalPtr : public vesoft::cpp::NonCopyable, public vesoft::cpp::NonMovable {
public:
    /**
     * @dtor If provided, the callback would be invoked when an existing
     *       TLS data needs to be destructed.
     */
    ThreadLocalPtr(std::function<void(T*)> dtor = nullptr);
    ~ThreadLocalPtr();
    /**
     * Set or reset TLS data for the current thread.
     */
    void reset(const T *data = nullptr);
    /**
     * Obtain the TLS data of the current thread, `nullptr' if there is none.
     *
     * Please NOTE that constness leaking might happens here.
     */
    T* get() const;
    /**
     * Some convenience operator overloads.
     */
    T* operator->() const;
    T& operator*() const;
    explicit operator bool() const;
    /**
     * Retrieve all TLS data
     */
    std::vector<T*> getAll() const;

private:
    struct MetaBlock {
        const T                *data_;
        pid_t                   tid_;
        ThreadLocalPtr<T>      *owner_;
    };
    MetaBlock* getMetaBlock() const;
    void setMetaBlock(MetaBlock *meta) const;
    void destructData(const T *data);
    void destructMetaBlock(MetaBlock *meta);
    static void destructor(void *meta);

private:
    pthread_key_t                                   key_;
    std::function<void(T*)>                         dtor_;
    std::unordered_map<pid_t, MetaBlock*>           metaPerThread_;
    mutable std::mutex                              lock_;
};

/**
 * Compare with nullptr
 */

template <typename T>
bool operator==(const ThreadLocalPtr<T> &ptr, nullptr_t) {
    return !ptr;
}

template <typename T>
bool operator==(nullptr_t, const ThreadLocalPtr<T> &ptr) {
    return !ptr;
}

template <typename T>
bool operator !=(const ThreadLocalPtr<T> &ptr, nullptr_t) {
    return !!ptr;
}

template <typename T>
bool operator!=(nullptr_t, const ThreadLocalPtr<T> &ptr) {
    return !!ptr;
}

template <typename T>
ThreadLocalPtr<T>::ThreadLocalPtr(std::function<void(T*)> dtor) {
    dtor_ = std::move(dtor);
    auto status = ::pthread_key_create(&key_, destructor);
    if (status != 0) {
        throw std::runtime_error("::pthread_key_create failed with " + std::to_string(status));
    }
}

template <typename T>
ThreadLocalPtr<T>::~ThreadLocalPtr() {
    std::lock_guard<std::mutex> guard(lock_);
    for (auto &e : metaPerThread_) {
        if (e.second->data_ != nullptr) {
            fprintf(stderr, "Outstanding thread still exists, tid: %d, type: %s\n", e.first, typeid(T).name());
        }
        delete e.second;
    }
}

template <typename T>
void ThreadLocalPtr<T>::reset(const T *data) {
    auto *meta = getMetaBlock();
    if (meta == nullptr) {
        meta = new MetaBlock;
        meta->data_ = data;
        meta->owner_ = this;
        meta->tid_ = concurrent::gettid();
        {
            std::lock_guard<std::mutex> guard(lock_);
            metaPerThread_.emplace(meta->tid_, meta);
        }
        setMetaBlock(meta);
    } else {
        auto *old = meta->data_;
        meta->data_ = data;
        destructData(old);
    }
}

template <typename T>
T* ThreadLocalPtr<T>::get() const {
    auto meta = getMetaBlock();
    if (meta != nullptr) {
        return const_cast<T*>(meta->data_);
    }
    return nullptr;
}

template <typename T>
T* ThreadLocalPtr<T>::operator->() const {
    return this->get();
}

template <typename T>
T& ThreadLocalPtr<T>::operator*() const {
    return *this->get();
}

template <typename T>
ThreadLocalPtr<T>::operator bool() const {
    return this->get() != nullptr;
}

template <typename T>
std::vector<T*> ThreadLocalPtr<T>::getAll() const {
    std::vector<T*> all;
    {
        std::lock_guard<std::mutex> guard(lock_);
        for (auto &e : metaPerThread_) {
            all.push_back(const_cast<T*>(e.second->data_));
        }
    }
    return all;
}

template <typename T>
void ThreadLocalPtr<T>::destructor(void *meta) {
    assert(meta != nullptr);
    auto *m = reinterpret_cast<MetaBlock*>(meta);
    m->owner_->destructMetaBlock(m);
}

template <typename T>
typename ThreadLocalPtr<T>::MetaBlock*
ThreadLocalPtr<T>::getMetaBlock() const {
    return reinterpret_cast<MetaBlock*>(::pthread_getspecific(key_));
}

template <typename T>
void ThreadLocalPtr<T>::setMetaBlock(MetaBlock *meta) const {
    ::pthread_setspecific(key_, meta);
}

template <typename T>
void ThreadLocalPtr<T>::destructData(const T *data) {
    if (dtor_ != nullptr && data != nullptr) {
        dtor_(const_cast<T*>(data));
    }
}

template <typename T>
void ThreadLocalPtr<T>::destructMetaBlock(MetaBlock *meta) {
    {
        std::lock_guard<std::mutex> guard(lock_);
        auto iter = metaPerThread_.find(meta->tid_);
        if (iter != metaPerThread_.end()) {
            metaPerThread_.erase(iter);
        }
    }
    destructData(meta->data_);
    delete meta;
}

}   // namespace concurrent
}   // namespace vesoft

#endif  // COMMON_CONCURRENT_THREAD_THREADLOCALPTR_H_

/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_CHECKPOINTER_H_
#define COMMON_BASE_CHECKPOINTER_H_

namespace nebula {

template <typename T>
bool inline checkPointer(const T *lhs, const T *rhs) {
    if (lhs == rhs) {
        return true;
    } else if (lhs != nullptr && rhs != nullptr) {
        return *lhs == *rhs;
    } else {
        return false;
    }
}

template <typename T, template <typename, typename...> class DeRefAble>
bool inline checkPointer(const DeRefAble<T> &lhs, const DeRefAble<T> &rhs) {
    if (lhs == rhs) {
        return true;
    } else if (lhs != nullptr && rhs != nullptr) {
        return *lhs == *rhs;
    } else {
        return false;
    }
}

}   // namespace nebula

#endif   // COMMON_BASE_CHECKPOINTER_H_

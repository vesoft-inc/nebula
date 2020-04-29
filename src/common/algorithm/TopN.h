/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_ALGORITHM_TOPN_H_
#define COMMON_ALGORITHM_TOPN_H_

#include "base/Base.h"

template <class T>
class TopN final {
public:
    using CMP = std::function<bool(const T& lhs, const T& rhs)>;
    TopN(std::vector<T>&& v, CMP less, CMP eq) {
        n_ = v.size();
        less_ = less;
        eq_ = eq;
        topN_ = std::move(v);
        for (auto p = (n_ - 1) /2; p >= 0; --p) {
            down(p);
        }
    }

    void push(T&& val) {
        if (eq_(val, topN_[0])) {
            return;
        }

        if (!less_(val, topN_[0])) {
            topN_[0] = std::move(val);
            down(0);
        }
    }

    std::vector<T> topN() {
        std::sort(topN_.begin(), topN_.end(), [this] (const T& lhs, const T& rhs) {
                    if (!eq_(lhs, rhs)) {
                        return !less_(lhs, rhs);
                    }
                    return false;
                });
        return std::move(topN_);
    }

private:
    void down(int64_t parent) {
        auto child = parent * 2 + 1;
        if (child >= n_) {
            return;
        }

        if ((child + 1 < n_) && less_(topN_[child + 1], topN_[child])) {
            ++child;
        }

        if (topN_[child] < topN_[parent]) {
            std::swap(topN_[child], topN_[parent]);
            down(child);
        }
    }

private:
    std::vector<T>      topN_;
    int64_t             n_;
    CMP                 less_;
    CMP                 eq_;
};
#endif

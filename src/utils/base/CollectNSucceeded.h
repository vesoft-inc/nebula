/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_COLLECTNSUCCEEDED_H_
#define COMMON_BASE_COLLECTNSUCCEEDED_H_

#include "common/base/Base.h"
#include <folly/futures/Future.h>

namespace nebula {

/*********************************************************************
 * Here is a future aggregator, along with its variation
 *
 * It is based on folly future libraries. Please read
 * folly/futures/README.md for information about folly futures
 *
 * The aggregator uses an evaluator to evaluate the future results.
 * The evaluator can be a lambda function, or std::function, or any
 * class/struct which overloads operator(). The evaluator takes two
 * parameters.
 *
 *  operator() (size_t idx, T& value)
 *
 * The first parameter ##idx## is the position of the future being
 * evaluated in the future list. The second parameter is the value
 * of the future
 *
 * Only the future having value will be checked by the evaluator.
 * Futures having exceptions will be considered as failure
 *
 * The aggregator is **NOT** thread-safe
 *
 * The aggregator returns a future with an array of pairs
 * <size_t, T>. The first element of the pair is the index to the
 * given futures, and the second is the value of the succeeded
 * future.
 *
 * The returned future fulfils when the given number of futures
 * succeed, or when all given futures fulfil. In either case, an
 * array of succeeded values will be returned
 *
 ********************************************************************/

template<typename FutureIter>
using FutureReturnType =
    typename std::iterator_traits<FutureIter>::value_type::value_type;

// The result pair list.
// The first element in pair represents the index in Requests list.
template<typename FutureIter>
using SucceededResultList = std::vector<std::pair<size_t, FutureReturnType<FutureIter>>>;


template <class FutureIter, typename ResultEval>
folly::Future<SucceededResultList<FutureIter>> collectNSucceeded(
    FutureIter first,
    FutureIter last,
    size_t n,  // NUmber of succeeded futures required
    ResultEval&& eval);


// A convenient form of ##collectNSucceeded##
template <class Collection, typename ResultEval>
auto collectNSucceeded(Collection&& c,
                       size_t n,
                       ResultEval&& eval)
    -> decltype(collectNSucceeded(c.begin(), c.end(), n, eval)) {
    return collectNSucceeded(c.begin(),
                             c.end(),
                             n,
                             std::forward<ResultEval>(eval));
}

}  // namespace nebula

#include "common/base/CollectNSucceeded.inl"

#endif  // COMMON_BASE_COLLECTNSUCCEEDED_H_

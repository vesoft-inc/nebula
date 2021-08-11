/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/ExceptionWrapper.h>

namespace nebula {

// Not thread-safe, all futures need to be on the same executor
template <class FutureIter, typename ResultEval>
folly::Future<SucceededResultList<FutureIter>> collectNSucceeded(
        FutureIter first,
        FutureIter last,
        size_t n,
        ResultEval&& eval) {
    using Result = SucceededResultList<FutureIter>;
    if (n == 0) {
        return folly::Future<Result>(Result());
    }

    struct Context {
        Context(size_t total, ResultEval&& e)
            : eval(std::forward<ResultEval>(e))
            , nTotal(total) {}

        ResultEval eval;
        Result results;
        std::atomic<size_t> numCompleted = {0};
        std::atomic<size_t> nSucceeded = {0};
        folly::Promise<Result> promise;
        size_t nTotal;
    };

    size_t total = size_t(std::distance(first, last));
    DCHECK_GE(total, 0U);

    if (total < n) {
        return folly::Future<Result>(
            folly::exception_wrapper(
                std::runtime_error("Not enough futures")));
    }

    auto ctx = std::make_shared<Context>(
        total, std::forward<ResultEval>(eval));

    // for each succeeded Future, add to the result list, until
    // we have required number of futures, at which point we fulfil
    // the promise with the result list
    for (size_t index = 0; first != last; ++first, ++index) {
        first->setCallback_([n, ctx, index] (auto,
                folly::Try<FutureReturnType<FutureIter>>&& t) {
            if (!ctx->promise.isFulfilled()) {
                if (!t.hasException()) {
                    if (ctx->eval(index, t.value())) {
                        ++ctx->nSucceeded;
                    }
                    ctx->results.emplace_back(index, std::move(t.value()));
                }
                if ((++ctx->numCompleted) == ctx->nTotal || ctx->nSucceeded == n) {
                    // Done
                    VLOG(2) << "Set Value [completed="
                            << ctx->numCompleted
                            << ", total=" << ctx->nTotal
                            << ", Result list size="
                            << ctx->results.size()
                            << "]";
                    ctx->promise.setValue(std::move(ctx->results));
                }
            }
        });
    }

    return ctx->promise.getFuture();
}

}  // namespace nebula

/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

namespace vesoft {

// Not thread-safe
template <class FutureIter, typename ResultEval>
folly::Future<SucceededResultList<FutureIter>> collectNSucceeded(
        FutureIter first,
        FutureIter last,
        size_t n,
        ResultEval eval) {
    struct CollectNSucceededContext {
        SucceededResultList<FutureIter> results;
        std::atomic<size_t> numCompleted = {0};
        folly::Promise<SucceededResultList<FutureIter>> promise;
        size_t nTotal;
    };
    auto ctx = std::make_shared<CollectNSucceededContext>();
    ctx->nTotal = size_t(std::distance(first, last));

    if (ctx->nTotal < n) {
        ctx->promise.setException(
            std::runtime_error("Not enough futures"));
    } else {
        // for each succeeded Future, add to the result list, until
        // we have numSucceededRequired futures, at which point we
        // fulfil the promise with the result list
        size_t idx = 0;
        for (auto it = first; it != last; ++it, ++idx) {
            auto&& r = it->getTry();
            if (!ctx->promise.isFulfilled()) {
                if (!r.hasException() && eval(idx, r.value())) {
                    ctx->results.emplace_back(idx, std::move(r.value()));
                }
                if (++ctx->numCompleted == ctx->nTotal ||
                    ctx->results.size() == n) {
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
        }
    }

    return ctx->promise.getFuture();
}

}  // namespace vesoft

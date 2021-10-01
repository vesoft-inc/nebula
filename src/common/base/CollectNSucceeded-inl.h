/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <folly/ExceptionWrapper.h>

namespace nebula {

// Not thread-safe, all futures need to be on the same executor
template <class FutureIter, typename ResultEval>
folly::SemiFuture<SucceededResultList<FutureIter>> collectNSucceeded(
    FutureIter first,
    FutureIter last,
    size_t n,
    ResultEval&& eval) {
  using Result = SucceededResultList<FutureIter>;
  if (n == 0) {
    return folly::SemiFuture<Result>(Result());
  }

  struct Context {
    Context(size_t total, ResultEval&& e)
        : eval(std::forward<ResultEval>(e))
        , results(total)
        , nTotal(total) {}

    ResultEval eval;
    std::vector<folly::Optional<folly::Try<FutureReturnType<FutureIter>>>> results;
    std::atomic<size_t> nCompleted = {0};
    std::atomic<size_t> nSucceeded = {0};
    std::atomic<bool> fulfilled = false;
    folly::Promise<Result> promise;
    const size_t nTotal;
  };

  size_t total = size_t(std::distance(first, last));
  DCHECK_GE(total, 0U);

  if (total < n) {
    return folly::SemiFuture<Result>(
        folly::exception_wrapper(std::runtime_error("Not enough futures")));
  }

  std::vector<folly::futures::detail::DeferredWrapper> executors;
  folly::futures::detail::stealDeferredExecutors(executors, first, last);

  // for each succeeded Future, add to the result list, until
  // we have required number of futures, at which point we fulfil
  // the promise with the result list
  auto ctx = std::make_shared<Context>(total, std::forward<ResultEval>(eval));
  for (size_t index = 0; first != last; ++first, ++index) {
    first->setCallback_([n, ctx, index](auto&&,
                                        folly::Try<FutureReturnType<FutureIter>>&& t) {
      VLOG(3) << "Received the callback from the future " << index;

      // relaxed because this guards control but does not guard data
      auto const c = 1 + ctx->nCompleted.fetch_add(1, std::memory_order_relaxed);
      auto s = ctx->nSucceeded.load();

      if (!ctx->fulfilled.load()) {
        if (!t.hasException()) {
          if (ctx->eval(index, t.value())) {
            // release because the stored values in all threads must be visible below
            // acquire because no stored value is permitted to be fetched early
            s = 1 + ctx->nSucceeded.fetch_add(1, std::memory_order_acq_rel);
          }
        } else {
          // Got exception
          DLOG(ERROR) << "Exception receided: " << t.exception().what().toStdString();
        }

        ctx->results[index] = std::move(t);
        if (c < ctx->nTotal && s < n) {
          return;
        }

        bool fulfilled = false;
        if (!ctx->fulfilled.compare_exchange_strong(fulfilled, true)) {
          // Already fulfilled
          return;
        }

        Result collectedResults;
        collectedResults.reserve(ctx->nCompleted.load());
        for (size_t i = 0; i < ctx->nTotal; ++i) {
          auto& entry = ctx->results[i];
          if (entry.hasValue() && entry.value().hasValue()) {
            collectedResults.emplace_back(i, std::move(entry).value().value());
          }
        }

        // Done
        VLOG(2) << "Set Value [total=" << ctx->nTotal
                << ", completed=" << c
                << ", succeeded=" << s
                << ", Result list size=" << collectedResults.size() << "]";
        ctx->promise.setValue(std::move(collectedResults));
      }
    });
    VLOG(3) << "Set up future callback " << index;
  }

  auto future = ctx->promise.getSemiFuture();
  if (!executors.empty()) {
    VLOG(3) << "Wiring the nested executors";
    future = std::move(future).defer(
        [](folly::Try<typename decltype(future)::value_type>&& t) {
          return std::move(t).value();
        });
    const auto& deferredExecutor = folly::futures::detail::getDeferredExecutor(future);
    deferredExecutor->setNestedExecutors(std::move(executors));
  }
  return future;
}

}  // namespace nebula

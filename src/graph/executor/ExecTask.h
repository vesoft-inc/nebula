/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <functional>

#include "common/concurrent/NebulaTask.h"

namespace nebula {
namespace graph {

class QueryContext;

template <typename F>
class ExecTask : public concurrency::NebulaTask {
 public:
  using Func = std::function<void()>;

  ExecTask(QueryContext *qctx, F f, Func attachFunc = kDoNothing, Func detachFunc = kDoNothing)
      : qctx_(qctx),
        func_(std::move(f)),
        attachFunc_(std::move(attachFunc)),
        detachFunc_(std::move(detachFunc)) {}

  template <typename... Args>
  auto operator()(Args &&... args) noexcept {
    attachFunc_();
    auto result = func_(std::forward<Args>(args)...);
    detachFunc_();
    return result;
  }

  static Func kDoNothing;

 private:
  QueryContext *qctx_{nullptr};
  F func_;
  Func attachFunc_;
  Func detachFunc_;
};

}  // namespace graph
}  // namespace nebula

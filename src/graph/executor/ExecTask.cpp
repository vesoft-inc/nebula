/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <graph/executor/ExecTask.h>

namespace nebula {
namespace graph {

template <typename F>
typename ExecTask<F>::Func ExecTask<F>::kDoNothing = []() {};

}  // namespace graph
}  // namespace nebula

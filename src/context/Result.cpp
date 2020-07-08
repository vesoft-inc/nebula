/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "context/Result.h"

namespace nebula {
namespace graph {

const Result Result::kEmptyResult = ResultBuilder().iter(Iterator::Kind::kDefault).finish();

const std::vector<Result> Result::kEmptyResultList;

}   // namespace graph
}   // namespace nebula

/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/context/Result.h"

namespace nebula {
namespace graph {

const Result& Result::EmptyResult() {
  static Result kEmptyResult =
      ResultBuilder().value(Value()).iter(Iterator::Kind::kDefault).build();
  return kEmptyResult;
}

const std::vector<Result>& Result::EmptyResultList() {
  static std::vector<Result> kEmptyResultList;
  return kEmptyResultList;
}

ResultBuilder& ResultBuilder::iter(Iterator::Kind kind) {
  DCHECK(kind == Iterator::Kind::kDefault || core_.value)
      << "Must set value when creating non-default iterator";
  switch (kind) {
    case Iterator::Kind::kDefault:
      return iter(std::make_unique<DefaultIter>(core_.value, core_.checkMemory));
    case Iterator::Kind::kSequential:
      return iter(std::make_unique<SequentialIter>(core_.value, core_.checkMemory));
    case Iterator::Kind::kGetNeighbors:
      return iter(std::make_unique<GetNeighborsIter>(core_.value, core_.checkMemory));
    case Iterator::Kind::kProp:
      return iter(std::make_unique<PropIter>(core_.value, core_.checkMemory));
    default:
      LOG(FATAL) << "Invalid Iterator kind" << static_cast<uint8_t>(kind);
  }
}

}  // namespace graph
}  // namespace nebula

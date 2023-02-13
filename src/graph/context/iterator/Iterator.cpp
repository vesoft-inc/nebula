/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/context/iterator/Iterator.h"

#include "common/memory/MemoryUtils.h"

DECLARE_int32(num_rows_to_check_memory);
DECLARE_double(system_memory_high_watermark_ratio);

namespace nebula {
namespace graph {

bool Iterator::hitsSysMemoryHighWatermark() const {
  if (checkMemory_) {
    if (numRowsModN_ >= FLAGS_num_rows_to_check_memory) {
      numRowsModN_ -= FLAGS_num_rows_to_check_memory;
    }
    if (UNLIKELY(numRowsModN_ == 0)) {
      if (memory::MemoryUtils::kHitMemoryHighWatermark.load()) {
        throw std::runtime_error(
            folly::sformat("Used memory hits the high watermark({}) of total system memory.",
                           FLAGS_system_memory_high_watermark_ratio));
      }
    }
  }
  return false;
}

std::ostream& operator<<(std::ostream& os, Iterator::Kind kind) {
  switch (kind) {
    case Iterator::Kind::kDefault:
      os << "default";
      break;
    case Iterator::Kind::kSequential:
      os << "sequential";
      break;
    case Iterator::Kind::kGetNeighbors:
      os << "get neighbors";
      break;
    case Iterator::Kind::kProp:
      os << "Prop";
      break;
  }
  os << " iterator";
  return os;
}

}  // namespace graph
}  // namespace nebula

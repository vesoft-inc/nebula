/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/index/IndexExecutor.h"

DEFINE_int32(max_rows_returned_per_lookup, INT_MAX,
             "Max rows count returned when lookup vertices or edges");

namespace nebula {
namespace storage {
}  // namespace storage
}  // namespace nebula

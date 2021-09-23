/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/geo/io/Geometry.h"

namespace nebula {

class WKBReader {
 public:
  WKBReader() {}

  ~WKBReader() {}

  StatusOr<std::unique_ptr<Geometry>> read(std::string wkb) { UNUSED(wkb); }
};

}  // namespace nebula

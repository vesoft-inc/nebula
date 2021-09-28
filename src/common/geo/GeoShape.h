/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

namespace nebula {

enum class GeoShape : uint32_t {
  UNKNOWN = 0,  // illegal
  POINT = 1,
  LINESTRING = 2,
  POLYGON = 3,
};

}  // namespace nebula

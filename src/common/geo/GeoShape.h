/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

namespace nebula {

enum class ShapeType : uint32_t {
  Point = 1,
  LineString = 2,
  Polygon = 3,
};

}  // namespace nebula

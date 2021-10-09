/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/datatypes/Geography.h"

namespace nebula {

bool intersects(const Geography& a, const Geography& b);

}  // namespace nebula

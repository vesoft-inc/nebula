/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/datatypes/Geography.h"

namespace nebula {

// Covers returns whether geography b covers geography b.
// If no point in b lies exterior of b, a covers b.
// http://lin-ear-th-inking.blogspot.com/2007/06/subtleties-of-ogc-covers-spatial.html
bool covers(const Geography& a, const Geography& b);

bool coveredBy(const Geography& a, const Geography& b);

}  // namespace nebula

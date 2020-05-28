/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_COLUMNTYPEDEF_H_
#define PARSER_COLUMNTYPEDEF_H_

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/meta_types.h"

namespace nebula {
struct ColumnTypeDef {
    explicit ColumnTypeDef(meta::cpp2::PropertyType t, int16_t l = 0) : type(t), typeLen(l) {}

    meta::cpp2::PropertyType  type;
    int16_t                   typeLen;
};
}
#endif  // PARSER_COLUMNTYPEDEF_H_

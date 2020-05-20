/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_SCHEMAUTIL_H_
#define META_SCHEMAUTIL_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {
class SchemaUtil final {
public:
    SchemaUtil() = default;
    ~SchemaUtil() = default;

public:
    static bool checkType(std::vector<cpp2::ColumnDef> &columns);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SCHEMAUTIL_H_

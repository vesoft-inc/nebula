/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_SCHEMAUTIL_H_
#define META_SCHEMAUTIL_H_

#include "common/base/ObjectPool.h"
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {
class SchemaUtil final {
 public:
  SchemaUtil() = default;
  ~SchemaUtil() = default;

 public:
  static bool checkType(std::vector<cpp2::ColumnDef> &columns);

 private:
  static bool checkType(ObjectPool *pool,
                        const std::string &name,
                        cpp2::ColumnDef &column,
                        const Value &value);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SCHEMAUTIL_H_

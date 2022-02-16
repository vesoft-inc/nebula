/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_SCHEMAUTIL_H_
#define META_SCHEMAUTIL_H_

#include <string>  // for string
#include <vector>  // for vector

#include "common/base/ObjectPool.h"
#include "common/datatypes/Value.h"  // for Value
#include "meta/processors/BaseProcessor.h"

namespace nebula {
class ObjectPool;
namespace meta {
namespace cpp2 {
class ColumnDef;
}  // namespace cpp2
}  // namespace meta

class ObjectPool;

namespace meta {
namespace cpp2 {
class ColumnDef;
}  // namespace cpp2

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

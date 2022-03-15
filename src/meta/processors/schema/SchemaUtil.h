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
  /**
   * @brief Check if default value valid considering the nullable properties and its type.
   *
   * @param columns
   * @return true
   * @return false
   */
  static bool checkType(std::vector<cpp2::ColumnDef> &columns);

 private:
  /**
   * @brief Check default value
   *
   * @param pool
   * @param name column name
   * @param column column definition
   * @param value default value
   * @return true
   * @return false
   */
  static bool checkType(ObjectPool *pool,
                        const std::string &name,
                        cpp2::ColumnDef &column,
                        const Value &value);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SCHEMAUTIL_H_

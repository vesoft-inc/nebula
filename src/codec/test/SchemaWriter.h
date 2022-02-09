/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CODEC_TEST_SCHEMAWRITER_H_
#define CODEC_TEST_SCHEMAWRITER_H_

#include "codec/test/ResultSchemaProvider.h"
#include "common/base/Base.h"
#include "common/expression/Expression.h"

namespace nebula {

class SchemaWriter : public ResultSchemaProvider {
 public:
  explicit SchemaWriter(SchemaVer ver = 0) : ResultSchemaProvider(ver) {}

  SchemaWriter& appendCol(folly::StringPiece name,
                          nebula::cpp2::PropertyType type,
                          int32_t fixedStrLen = 0,
                          bool nullable = false,
                          Expression* defaultValue = nullptr,
                          meta::cpp2::GeoShape geoShape = meta::cpp2::GeoShape::ANY) noexcept;

 private:
};

}  // namespace nebula
#endif  // CODEC_TEST_SCHEMAWRITER_H_

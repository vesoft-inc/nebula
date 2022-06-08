/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_META_NEBULASCHEMAPROVIDER_H_
#define COMMON_META_NEBULASCHEMAPROVIDER_H_

#include <folly/RWSpinLock.h>

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/meta/SchemaProviderIf.h"

namespace nebula {
namespace meta {

class NebulaSchemaProvider : public SchemaProviderIf {
  friend class FileBasedSchemaManager;

 public:
  class SchemaField final : public SchemaProviderIf::Field {
   public:
    SchemaField(std::string name,
                nebula::cpp2::PropertyType type,
                bool nullable,
                bool hasDefault,
                std::string defaultValue,
                size_t size,
                size_t offset,
                size_t nullFlagPos,
                cpp2::GeoShape geoShape)
        : name_(std::move(name)),
          type_(std::move(type)),
          nullable_(nullable),
          hasDefault_(hasDefault),
          defaultValue_(defaultValue),
          size_(size),
          offset_(offset),
          nullFlagPos_(nullFlagPos),
          geoShape_(geoShape) {}

    const char* name() const override {
      return name_.c_str();
    }

    nebula::cpp2::PropertyType type() const override {
      return type_;
    }

    bool nullable() const override {
      return nullable_;
    }

    bool hasDefault() const override {
      return hasDefault_;
    }

    const std::string& defaultValue() const override {
      return defaultValue_;
    }

    size_t size() const override {
      return size_;
    }

    size_t offset() const override {
      return offset_;
    }

    size_t nullFlagPos() const override {
      DCHECK(nullable_);
      return nullFlagPos_;
    }

    cpp2::GeoShape geoShape() const override {
      return geoShape_;
    }

   private:
    std::string name_;
    nebula::cpp2::PropertyType type_;
    bool nullable_;
    bool hasDefault_;
    std::string defaultValue_;
    size_t size_;
    size_t offset_;
    size_t nullFlagPos_;
    cpp2::GeoShape geoShape_;
  };

 public:
  explicit NebulaSchemaProvider(SchemaVer ver) : ver_(ver), numNullableFields_(0) {}

  SchemaVer getVersion() const noexcept override;
  // Returns the size of fields_
  size_t getNumFields() const noexcept override;
  size_t getNumNullableFields() const noexcept override;

  // Returns the total space in bytes occupied by the fields_
  size_t size() const noexcept override;

  int64_t getFieldIndex(const std::string& name) const override;
  const char* getFieldName(int64_t index) const override;

  nebula::cpp2::PropertyType getFieldType(int64_t index) const override;
  nebula::cpp2::PropertyType getFieldType(const std::string& name) const override;

  const SchemaProviderIf::Field* field(int64_t index) const override;
  const SchemaProviderIf::Field* field(const std::string& name) const override;

  void addField(folly::StringPiece name,
                nebula::cpp2::PropertyType type,
                size_t fixedStrLen = 0,
                bool nullable = false,
                std::string defaultValue = "",
                cpp2::GeoShape geoShape = cpp2::GeoShape::ANY);

  static std::size_t fieldSize(nebula::cpp2::PropertyType type, std::size_t fixedStrLimit);

  void setProp(cpp2::SchemaProp schemaProp);

  const cpp2::SchemaProp getProp() const;

  StatusOr<std::pair<std::string, int64_t>> getTTLInfo() const;

  bool hasNullableCol() const {
    return numNullableFields_ != 0;
  }

 protected:
  NebulaSchemaProvider() = default;

 protected:
  SchemaVer ver_{-1};

  // fieldname -> index
  std::unordered_map<std::string, int64_t> fieldNameIndex_;
  std::vector<SchemaField> fields_;
  size_t numNullableFields_;
  cpp2::SchemaProp schemaProp_;
};

}  // namespace meta
}  // namespace nebula
#endif  // COMMON_META_NEBULASCHEMAPROVIDER_H_

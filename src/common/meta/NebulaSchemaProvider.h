/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_META_NEBULASCHEMAPROVIDER_H_
#define COMMON_META_NEBULASCHEMAPROVIDER_H_

#include <folly/RWSpinLock.h>
#include <gtest/gtest_prod.h>

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/expression/Expression.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

class NebulaSchemaProvider {
  friend class RowReaderV2Test;

 public:
  class SchemaField final {
   public:
    SchemaField(const std::string& name,
                nebula::cpp2::PropertyType type,
                bool nullable,
                bool hasDefault,
                std::string defaultValue,
                size_t size,
                size_t offset,
                size_t nullFlagPos,
                cpp2::GeoShape geoShape)
        : name_(name),
          type_(type),
          nullable_(nullable),
          hasDefault_(hasDefault),
          defaultValue_(defaultValue),
          size_(size),
          offset_(offset),
          nullFlagPos_(nullFlagPos),
          geoShape_(geoShape) {}

    const char* name() const {
      return name_.c_str();
    }

    nebula::cpp2::PropertyType type() const {
      return type_;
    }

    bool nullable() const {
      return nullable_;
    }

    bool hasDefault() const {
      return hasDefault_;
    }

    const std::string& defaultValue() const {
      return defaultValue_;
    }

    size_t size() const {
      return size_;
    }

    size_t offset() const {
      return offset_;
    }

    size_t nullFlagPos() const {
      DCHECK(nullable_);
      return nullFlagPos_;
    }

    cpp2::GeoShape geoShape() const {
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

  class Iterator final {
    friend class NebulaSchemaProvider;

   public:
    const SchemaField& operator*() const {
      return *field_;
    }

    const SchemaField* operator->() const {
      return field_;
    }

    Iterator& operator++() {
      if (field_) {
        index_++;
        field_ = schema_->field(index_);
      }
      return *this;
    }

    Iterator& operator+(uint16_t steps) {
      if (field_) {
        index_ += steps;
        field_ = schema_->field(index_);
      }
      return *this;
    }

    operator bool() const {
      return static_cast<bool>(field_);
    }

    bool operator==(const Iterator& rhs) const {
      return schema_ == rhs.schema_ && (index_ == rhs.index_ || (!field_ && !rhs.field_));
    }

   private:
    const NebulaSchemaProvider* schema_;
    size_t numFields_;
    int64_t index_;
    const SchemaField* field_;

   private:
    explicit Iterator(const NebulaSchemaProvider* schema, int64_t idx = 0)
        : schema_(schema), numFields_(schema_->getNumFields()), index_(idx) {
      field_ = schema_->field(index_);
    }
  };

 public:
  explicit NebulaSchemaProvider(SchemaVer ver) : ver_(ver), numNullableFields_(0) {}

  NebulaSchemaProvider() : ver_(0), numNullableFields_(0) {}

  SchemaVer getVersion() const noexcept;
  // Returns the size of fields_
  size_t getNumFields() const noexcept;
  size_t getNumNullableFields() const noexcept;

  // Returns the total space in bytes occupied by the fields_
  size_t size() const noexcept;

  int64_t getFieldIndex(const std::string& name) const;
  const char* getFieldName(int64_t index) const;

  nebula::cpp2::PropertyType getFieldType(int64_t index) const;
  nebula::cpp2::PropertyType getFieldType(const std::string& name) const;

  const SchemaField* field(int64_t index) const;
  const SchemaField* field(const std::string& name) const;

  void addField(const std::string& name,
                nebula::cpp2::PropertyType type,
                size_t fixedStrLen = 0,
                bool nullable = false,
                std::string defaultValue = "",
                cpp2::GeoShape geoShape = cpp2::GeoShape::ANY);

  Iterator begin() const {
    return Iterator(this, 0);
  }

  Iterator end() const {
    return Iterator(this, getNumFields());
  }

  void setProp(cpp2::SchemaProp schemaProp);

  const cpp2::SchemaProp getProp() const;

  StatusOr<std::pair<std::string, int64_t>> getTTLInfo() const;

  bool hasNullableCol() const {
    return numNullableFields_ != 0;
  }

 private:
  std::size_t fieldSize(nebula::cpp2::PropertyType type, std::size_t fixedStrLimit);

 private:
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

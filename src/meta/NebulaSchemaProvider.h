/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_NEBULASCHEMAPROVIDER_H_
#define META_NEBULASCHEMAPROVIDER_H_

#include "base/Base.h"
#include <folly/RWSpinLock.h>
#include "meta/SchemaProviderIf.h"

namespace nebula {
namespace meta {

class NebulaSchemaProvider : public SchemaProviderIf {
    friend class FileBasedSchemaManager;
public:
    class SchemaField final : public SchemaProviderIf::Field {
    public:
        SchemaField(std::string name, nebula::cpp2::ValueType type)
            : name_(std::move(name))
            , type_(std::move(type)) {}

        const char* getName() const override {
            return name_.c_str();
        }

        const nebula::cpp2::ValueType& getType() const override {
            return type_;
        }

        bool isValid() const override {
            return true;
        }

        bool hasDefaultValue() const override {
            return hasDefaultValue_;
        }

        VariantType getDefaultValue() const override {
            return defaultValue_;
        }

        void setDefaultValue(VariantType value) {
            hasDefaultValue_ = true;
            defaultValue_ = std::move(value);
        }

    private:
        std::string name_;
        nebula::cpp2::ValueType type_;
        bool hasDefaultValue_{false};
        VariantType defaultValue_;
    };

public:
    explicit NebulaSchemaProvider(SchemaVer ver) : ver_(ver) {}

    SchemaVer getVersion() const noexcept override;
    size_t getNumFields() const noexcept override;

    int64_t getFieldIndex(const folly::StringPiece name) const override;
    const char* getFieldName(int64_t index) const override;

    const nebula::cpp2::ValueType& getFieldType(int64_t index) const override;
    const nebula::cpp2::ValueType& getFieldType(const folly::StringPiece name) const override;

    std::shared_ptr<const SchemaProviderIf::Field> field(int64_t index) const override;
    std::shared_ptr<const SchemaProviderIf::Field> field(
        const folly::StringPiece name) const override;

    nebula::cpp2::Schema toSchema() const override;

    void addField(folly::StringPiece name, nebula::cpp2::ValueType&& type);

    void addDefaultValue(folly::StringPiece name, const nebula::cpp2::Value &value);

    void setProp(nebula::cpp2::SchemaProp schemaProp);

    const nebula::cpp2::SchemaProp getProp() const;
    const StatusOr<VariantType> getDefaultValue(const folly::StringPiece name) const override;
    const StatusOr<VariantType> getDefaultValue(int64_t index) const override;

protected:
    NebulaSchemaProvider() = default;

protected:
    SchemaVer ver_{-1};

    // fieldname -> index
    std::unordered_map<std::string, int64_t>   fieldNameIndex_;
    std::vector<std::shared_ptr<SchemaField>>  fields_;
    nebula::cpp2::SchemaProp                   schemaProp_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_NEBULASCHEMAPROVIDER_H_


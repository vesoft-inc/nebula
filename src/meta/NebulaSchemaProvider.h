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
        SchemaField(std::string name, cpp2::PropertyType type)
            : name_(std::move(name))
            , type_(std::move(type)) {}

        const char* name() const override {
            return name_.c_str();
        }

        const cpp2::PropertyType type() const override {
            return type_;
        }

        bool isValid() const override {
            return true;
        }

        bool hasDefault() const override {
            return hasDefault_;
        }

        const Value& defaultValue() const override {
            return defaultValue_;
        }

    private:
        std::string name_;
        cpp2::PropertyType type_;
        bool hasDefault_;
        Value defaultValue_;
    };

public:
    explicit NebulaSchemaProvider(SchemaVer ver) : ver_(ver) {}

    SchemaVer getVersion() const noexcept override;
    size_t getNumFields() const noexcept override;

    int64_t getFieldIndex(const folly::StringPiece name) const override;
    const char* getFieldName(int64_t index) const override;

    const cpp2::PropertyType getFieldType(int64_t index) const override;
    const cpp2::PropertyType getFieldType(const folly::StringPiece name) const override;

    std::shared_ptr<const SchemaProviderIf::Field> field(int64_t index) const override;
    std::shared_ptr<const SchemaProviderIf::Field> field(
        const folly::StringPiece name) const override;

    void addField(folly::StringPiece name, cpp2::PropertyType& type);

    void setProp(cpp2::SchemaProp schemaProp);

    const cpp2::SchemaProp getProp() const;

protected:
    NebulaSchemaProvider() = default;

protected:
    SchemaVer ver_{-1};

    // fieldname -> index
    std::unordered_map<std::string, int64_t>    fieldNameIndex_;
    std::vector<std::shared_ptr<SchemaField>>   fields_;
    cpp2::SchemaProp                            schemaProp_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_NEBULASCHEMAPROVIDER_H_


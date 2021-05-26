/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATAMAN_RESULTSCHEMAPROVIDER_H_
#define DATAMAN_RESULTSCHEMAPROVIDER_H_

#include "base/Base.h"
#include "meta/SchemaProviderIf.h"

namespace nebula {

class ResultSchemaProvider : public meta::SchemaProviderIf {
    using ColumnDefs = std::vector<cpp2::ColumnDef>;

public:
    class ResultSchemaField : public meta::SchemaProviderIf::Field {
    public:
        explicit ResultSchemaField(
            const cpp2::ColumnDef* col = nullptr);

        const char* getName() const override;
        const cpp2::ValueType& getType() const override;
        bool isValid() const override;
        bool hasDefaultValue() const override;
        VariantType getDefaultValue() const override;

    private:
        const cpp2::ColumnDef* column_;
    };


public:
    explicit ResultSchemaProvider(cpp2::Schema);
    ~ResultSchemaProvider() override = default;

    SchemaVer getVersion() const noexcept override {
        return schemaVer_;
    }

    size_t getNumFields() const noexcept override;

    int64_t getFieldIndex(const folly::StringPiece name) const override;
    const char* getFieldName(int64_t index) const override;

    const cpp2::ValueType& getFieldType(int64_t index) const override;
    const cpp2::ValueType& getFieldType(const folly::StringPiece name) const override;

    std::shared_ptr<const meta::SchemaProviderIf::Field> field(int64_t index) const override;
    std::shared_ptr<const meta::SchemaProviderIf::Field> field(
        const folly::StringPiece name) const override;

    nebula::cpp2::Schema toSchema() const override;

    const StatusOr<VariantType> getDefaultValue(const folly::StringPiece name) const override;
    const StatusOr<VariantType> getDefaultValue(int64_t index) const override;

protected:
    SchemaVer schemaVer_{0};

    ColumnDefs columns_;

    // Map of Hash64(field_name) -> array index
    UnorderedMap<uint64_t, int64_t> nameIndex_;

    // Default constructor, only used by SchemaWriter
    explicit ResultSchemaProvider(SchemaVer ver = 0) : schemaVer_(ver) {}
};

}  // namespace nebula
#endif  // DATAMAN_RESULTSCHEMAPROVIDER_H_

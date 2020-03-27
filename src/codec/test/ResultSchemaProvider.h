/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CODEC_TEST_RESULTSCHEMAPROVIDER_H_
#define CODEC_TEST_RESULTSCHEMAPROVIDER_H_

#include "base/Base.h"
#include "meta/SchemaProviderIf.h"

namespace nebula {

class ResultSchemaProvider : public meta::SchemaProviderIf {
    using ColumnDefs = std::vector<meta::cpp2::ColumnDef>;

public:
    class ResultSchemaField : public meta::SchemaProviderIf::Field {
    public:
        explicit ResultSchemaField(const meta::cpp2::ColumnDef* col = nullptr);

        const char* name() const override;
        const meta::cpp2::PropertyType type() const override;
        bool isValid() const override;
        bool hasDefault() const override;
        const Value& defaultValue() const override;

    private:
        const meta::cpp2::ColumnDef* column_;
    };


public:
    explicit ResultSchemaProvider(meta::cpp2::Schema);
    virtual ~ResultSchemaProvider() = default;

    SchemaVer getVersion() const noexcept override {
        return schemaVer_;
    }

    size_t getNumFields() const noexcept override;

    int64_t getFieldIndex(const folly::StringPiece name) const override;
    const char* getFieldName(int64_t index) const override;

    const meta::cpp2::PropertyType getFieldType(int64_t index) const override;
    const meta::cpp2::PropertyType getFieldType(const folly::StringPiece name)
        const override;

    std::shared_ptr<const meta::SchemaProviderIf::Field>
    field(int64_t index) const override;

    std::shared_ptr<const meta::SchemaProviderIf::Field>
    field(const folly::StringPiece name) const override;

protected:
    SchemaVer schemaVer_{0};

    ColumnDefs columns_;
    // Map of Hash64(field_name) -> array index
    std::unordered_map<uint64_t, int64_t> nameIndex_;

    // Default constructor, only used by SchemaWriter
    explicit ResultSchemaProvider(SchemaVer ver = 0) : schemaVer_(ver) {}
};

}  // namespace nebula
#endif  // CODEC_TEST_RESULTSCHEMAPROVIDER_H_

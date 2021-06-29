/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CODEC_TEST_RESULTSCHEMAPROVIDER_H_
#define CODEC_TEST_RESULTSCHEMAPROVIDER_H_

#include "common/base/Base.h"
#include "common/meta/SchemaProviderIf.h"

namespace nebula {

class ResultSchemaProvider : public meta::SchemaProviderIf {
public:
    class ResultSchemaField : public meta::SchemaProviderIf::Field {
    public:
        explicit ResultSchemaField(std::string name,
                                   meta::cpp2::PropertyType type,
                                   int16_t size,
                                   bool nullable,
                                   int32_t offset,
                                   size_t nullFlagPos,
                                   Expression* defaultValue = nullptr);

        const char* name() const override;
        meta::cpp2::PropertyType type() const override;
        bool nullable() const override;
        bool hasDefault() const override;
        Expression* defaultValue() const override;
        size_t size() const override;
        size_t offset() const override;
        size_t nullFlagPos() const override;

    private:
        std::string name_;
        meta::cpp2::PropertyType type_;
        int16_t size_;
        bool nullable_;
        int32_t offset_;
        size_t nullFlagPos_;
        Expression* defaultValue_;
    };


public:
    virtual ~ResultSchemaProvider() = default;

    SchemaVer getVersion() const noexcept override {
        return schemaVer_;
    }

    size_t getNumFields() const noexcept override;

    size_t getNumNullableFields() const noexcept override;

    size_t size() const noexcept override;

    int64_t getFieldIndex(const std::string& name) const override;
    const char* getFieldName(int64_t index) const override;

    meta::cpp2::PropertyType getFieldType(int64_t index) const override;
    meta::cpp2::PropertyType getFieldType(const std::string& name)
        const override;

    const meta::SchemaProviderIf::Field* field(int64_t index) const override;
    const meta::SchemaProviderIf::Field* field(const std::string& name)
        const override;

protected:
    SchemaVer schemaVer_{0};

    std::vector<ResultSchemaField> columns_;
    // Map of Hash64(field_name) -> array index
    std::unordered_map<uint64_t, int64_t> nameIndex_;
    size_t numNullableFields_{0};

    // Default constructor, only used by SchemaWriter
    explicit ResultSchemaProvider(SchemaVer ver = 0) : schemaVer_(ver) {}
};

}  // namespace nebula
#endif  // CODEC_TEST_RESULTSCHEMAPROVIDER_H_

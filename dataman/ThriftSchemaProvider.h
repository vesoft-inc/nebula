/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef DATAMAN_THRIFTSCHEMAWRAPPER_H_
#define DATAMAN_THRIFTSCHEMAWRAPPER_H_

#include "base/Base.h"
#include "dataman/SchemaProviderIf.h"

namespace vesoft {
namespace vgraph {

/**
 * A wrapper class for cpp2::Schema
 *
 * It will not take the ownership of the schema
 */
class ThriftSchemaProvider : public SchemaProviderIf {
    using ColumnDefs = std::vector<cpp2::ColumnDef>;

public:
    class ThriftSchemaField : public Field {
    public:
        explicit ThriftSchemaField(const cpp2::ColumnDef* col = nullptr);

        const char* getName() const override;
        const cpp2::ValueType* getType() const override;
        bool isValid() const override;

    private:
        const cpp2::ColumnDef* column_;
    };


public:
    explicit ThriftSchemaProvider(const cpp2::Schema*);
    virtual ~ThriftSchemaProvider() = default;

    int32_t getLatestVer() const noexcept override;
    int32_t getNumFields(int32_t /*ver*/) const noexcept override;

    int32_t getFieldIndex(const folly::StringPiece name,
                          int32_t ver) const override;
    const char* getFieldName(int32_t index, int32_t ver) const override;

    const cpp2::ValueType* getFieldType(int32_t index,
                                        int32_t ver) const override;
    const cpp2::ValueType* getFieldType(const folly::StringPiece name,
                                        int32_t ver) const override;

    std::unique_ptr<Field> field(int32_t index, int32_t ver) const override;
    std::unique_ptr<Field> field(const folly::StringPiece name,
                                 int32_t ver) const override;

protected:
    const ColumnDefs* columns_;
    // Map of Hash64(field_name) -> array index
    UnorderedMap<uint64_t, int32_t> nameIndex_;
};

}  // namespace vgraph
}  // namespace vesoft
#endif  // DATAMAN_THRIFTSCHEMAWRAPPER_H_

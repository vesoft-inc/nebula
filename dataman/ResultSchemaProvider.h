/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef DATAMAN_RESULTSCHEMAPROVIDER_H_
#define DATAMAN_RESULTSCHEMAPROVIDER_H_

#include "base/Base.h"
#include "dataman/SchemaProviderIf.h"

namespace vesoft {
namespace vgraph {

class ResultSchemaProvider : public SchemaProviderIf {
    using ColumnDefs = std::vector<cpp2::ColumnDef>;

public:
    class ResultSchemaField : public Field {
    public:
        explicit ResultSchemaField(const cpp2::ColumnDef* col = nullptr);

        const char* getName() const override;
        const cpp2::ValueType* getType() const override;
        bool isValid() const override;

    private:
        const cpp2::ColumnDef* column_;
    };


public:
    explicit ResultSchemaProvider(cpp2::Schema&&);
    virtual ~ResultSchemaProvider() = default;

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
    ColumnDefs columns_;
    // Map of Hash64(field_name) -> array index
    UnorderedMap<uint64_t, int32_t> nameIndex_;

    // Default constructor, only used by SchemaWriter
    ResultSchemaProvider() = default;
};

}  // namespace vgraph
}  // namespace vesoft
#endif  // DATAMAN_RESULTSCHEMAPROVIDER_H_

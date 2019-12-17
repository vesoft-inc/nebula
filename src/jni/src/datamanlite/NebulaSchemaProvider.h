/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATAMAN_CODEC_NEBULASCHEMAPROVIDER_H_
#define DATAMAN_CODEC_NEBULASCHEMAPROVIDER_H_

#include <unordered_map>
#include <vector>
#include "datamanlite/SchemaProviderIf.h"
#include "datamanlite/Slice.h"

namespace nebula {
namespace dataman {
namespace codec {

class NebulaSchemaProvider : public SchemaProviderIf {
public:
    class SchemaField final : public SchemaProviderIf::Field {
    public:
        SchemaField(std::string name, ValueType type)
            : name_(std::move(name))
            , type_(type) {}

        const char* getName() const override {
            return name_.c_str();
        }

        const ValueType& getType() const override {
            return type_;
        }

        bool isValid() const override {
            return true;
        }

        bool hasDefault() const override {
            return hasDefault_;
        }

        std::string getDefaultValue() const override {
            return defaultValue_;
        }

    private:
        std::string name_;
        ValueType type_;
        bool hasDefault_;
        std::string defaultValue_;
    };

public:
    NebulaSchemaProvider() = default;
    explicit NebulaSchemaProvider(SchemaVer ver) : ver_(ver) {}

    SchemaVer getVersion() const noexcept override;
    size_t getNumFields() const noexcept override;

    int64_t getFieldIndex(const Slice& name) const override;
    const char* getFieldName(int64_t index) const override;

    const ValueType& getFieldType(int64_t index) const override;
    const ValueType& getFieldType(const Slice& name) const override;

    std::shared_ptr<const SchemaProviderIf::Field> field(int64_t index) const override;
    std::shared_ptr<const SchemaProviderIf::Field> field(const Slice& name) const override;

    void addField(const Slice& name, ValueType type);


protected:
    SchemaVer ver_{0};

    // fieldname -> index
    std::unordered_map<std::string, int64_t>   fieldNameIndex_;
    std::vector<std::shared_ptr<SchemaField>>  fields_;
};

}  // namespace codec
}  // namespace dataman
}  // namespace nebula

#endif  // DATAMAN_CODEC_NEBULASCHEMAPROVIDER_H_


/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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

    private:
        std::string name_;
        nebula::cpp2::ValueType type_;
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

    void addField(folly::StringPiece name, nebula::cpp2::ValueType&& type);

protected:
    NebulaSchemaProvider() = default;

protected:
    SchemaVer ver_{-1};

    // fieldname -> index
    std::unordered_map<std::string, int64_t> fieldNameIndex_;
    std::vector<std::shared_ptr<SchemaField>>  fields_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_NEBULASCHEMAPROVIDER_H_


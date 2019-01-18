/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_SCHEMAMANAGER_H_
#define META_SCHEMAMANAGER_H_

#include "base/Base.h"
#include <folly/RWSpinLock.h>
#include "meta/SchemaProviderIf.h"

namespace nebula {
namespace meta {

class FileBasedSchemaManager;

class SchemaManager : public SchemaProviderIf {
    friend class FileBasedSchemaManager;
public:
    class SchemaField final : public SchemaProviderIf::Field {
    public:
        SchemaField(std::string name, storage::cpp2::ValueType type)
            : name_(std::move(name))
            , type_(std::move(type)) {}

        const char* getName() const override {
            return name_.c_str();
        }
        const storage::cpp2::ValueType& getType() const override {
            return type_;
        }
        bool isValid() const override {
            return true;
        }

    private:
        std::string name_;
        storage::cpp2::ValueType type_;
    };

public:
    static std::shared_ptr<const SchemaProviderIf> getTagSchema(
        GraphSpaceID space, TagID tag, int32_t ver = -1);
    static std::shared_ptr<const SchemaProviderIf> getTagSchema(
        const folly::StringPiece spaceName,
        const folly::StringPiece tagName,
        int32_t ver = -1);
    // Returns a negative number when the schema does not exist
    static int32_t getNewestTagSchemaVer(GraphSpaceID space, TagID tag);
    static int32_t getNewestTagSchemaVer(const folly::StringPiece spaceName,
                                         const folly::StringPiece tagName);

    static std::shared_ptr<const SchemaProviderIf> getEdgeSchema(
        GraphSpaceID space, EdgeType edge, int32_t ver = -1);
    static std::shared_ptr<const SchemaProviderIf> getEdgeSchema(
        const folly::StringPiece spaceName,
        const folly::StringPiece typeName,
        int32_t ver = -1);
    // Returns a negative number when the schema does not exist
    static int32_t getNewestEdgeSchemaVer(GraphSpaceID space, EdgeType edge);
    static int32_t getNewestEdgeSchemaVer(const folly::StringPiece spaceName,
                                          const folly::StringPiece typeName);

    static GraphSpaceID toGraphSpaceID(const folly::StringPiece spaceName);
    static TagID toTagID(const folly::StringPiece tagName);
    static EdgeType toEdgeType(const folly::StringPiece typeName);

public:
    int32_t getVersion() const noexcept override;
    size_t getNumFields() const noexcept override;

    int64_t getFieldIndex(const folly::StringPiece name) const override;
    const char* getFieldName(int64_t index) const override;

    const storage::cpp2::ValueType& getFieldType(int64_t index) const override;
    const storage::cpp2::ValueType& getFieldType(const folly::StringPiece name) const override;

    std::shared_ptr<const SchemaProviderIf::Field> field(int64_t index) const override;
    std::shared_ptr<const SchemaProviderIf::Field> field(
        const folly::StringPiece name) const override;

protected:
    static void init();

    SchemaManager() = default;

protected:
    int32_t ver_{0};

    // fieldname -> index
    std::unordered_map<std::string, int64_t> fieldNameIndex_;
    std::vector<std::shared_ptr<SchemaField>>  fields_;

protected:
    static folly::RWSpinLock tagLock_;
    static std::unordered_map<std::pair<GraphSpaceID, TagID>,
                              // version -> schema
                              std::map<int32_t, std::shared_ptr<const SchemaProviderIf>>>
        tagSchemas_;

    static folly::RWSpinLock edgeLock_;
    static std::unordered_map<std::pair<GraphSpaceID, EdgeType>,
                              // version -> schema
                              std::map<int32_t, std::shared_ptr<const SchemaProviderIf>>>
        edgeSchemas_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_SCHEMAMANAGER_H_


/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/meta/SchemaProviderIf.h"
#include "common/meta/SchemaManager.h"
#include "storage/test/AdHocSchemaManager.h"

namespace nebula {
namespace kvstore {

class TestUtils {
public:
    static std::unique_ptr<meta::SchemaManager> mockSchemaMan() {
        auto* schemaMan = new storage::AdHocSchemaManager();
        for (GraphSpaceID spaceId = 0; spaceId < 3; spaceId++) {
            schemaMan->addEdgeSchema(
                spaceId, 101 /*edge type*/, TestUtils::genEdgeSchemaProvider(10, 10));
            schemaMan->addEdgeSchema(
                spaceId, 102 /*edge type*/, TestUtils::genEdgeSchemaProvider(5, 5));
            for (auto tagId = 3001; tagId < 3010; tagId++) {
                schemaMan->addTagSchema(
                    spaceId, tagId, TestUtils::genTagSchemaProvider(tagId, 3, 3));
            }
        }
        std::unique_ptr<meta::SchemaManager> sm(schemaMan);
        return sm;
    }

private:
    // It will generate SchemaProvider with some int fields and string fields
    static std::shared_ptr<meta::SchemaProviderIf> genEdgeSchemaProvider(
            int32_t intFieldsNum,
            int32_t stringFieldsNum) {
        nebula::cpp2::Schema schema;
        for (auto i = 0; i < intFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = nebula::cpp2::SupportedType::INT;
            schema.columns.emplace_back(std::move(column));
        }
        for (auto i = intFieldsNum; i < intFieldsNum + stringFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = nebula::cpp2::SupportedType::STRING;
            schema.columns.emplace_back(std::move(column));
        }
        return std::make_shared<ResultSchemaProvider>(std::move(schema));
    }


    // It will generate tag SchemaProvider with some int fields and string fields
    static std::shared_ptr<meta::SchemaProviderIf> genTagSchemaProvider(
            TagID tagId,
            int32_t intFieldsNum,
            int32_t stringFieldsNum) {
        nebula::cpp2::Schema schema;
        for (auto i = 0; i < intFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
            column.type.type = nebula::cpp2::SupportedType::INT;
            schema.columns.emplace_back(std::move(column));
        }
        for (auto i = intFieldsNum; i < intFieldsNum + stringFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
            column.type.type = nebula::cpp2::SupportedType::STRING;
            schema.columns.emplace_back(std::move(column));
        }
        return std::make_shared<ResultSchemaProvider>(std::move(schema));
    }
};

}  // namespace kvstore
}  // namespace nebula

/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/test/MockIndexManager.h"
#include <memory>
#include <vector>

// tag index:
//     person()
//     book(name(32))
// edge index:
//    like()

namespace nebula {
namespace graph {

void MockIndexManager::init() {
    // index related
    meta::cpp2::IndexItem person_no_props_index;
    person_no_props_index.set_index_id(233);
    person_no_props_index.set_index_name("person_no_props_index");
    meta::cpp2::SchemaID personSchemaId;
    personSchemaId.set_tag_id(2);
    person_no_props_index.set_schema_id(std::move(personSchemaId));
    person_no_props_index.set_schema_name("person");

    meta::cpp2::IndexItem book_name_index;
    book_name_index.set_index_id(234);
    book_name_index.set_index_name("book_name_index");
    meta::cpp2::SchemaID bookSchemaId;
    bookSchemaId.set_tag_id(5);
    book_name_index.set_schema_id(std::move(bookSchemaId));
    book_name_index.set_schema_name("book");
    meta::cpp2::ColumnDef field;
    field.set_name("name");
    meta::cpp2::ColumnTypeDef type;
    type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
    type.set_type_length(32);
    field.set_type(std::move(type));
    book_name_index.set_fields({});
    book_name_index.fields_ref().value().emplace_back(std::move(field));

    meta::cpp2::IndexItem like_index;
    like_index.set_index_id(235);
    like_index.set_index_name("like_index");
    meta::cpp2::SchemaID likeSchemaId;
    likeSchemaId.set_edge_type(3);
    like_index.set_schema_id(std::move(likeSchemaId));
    like_index.set_schema_name("like");

    tagIndexes_.emplace(1, std::vector<std::shared_ptr<meta::cpp2::IndexItem>>{});
    tagIndexes_[1].emplace_back(
        std::make_shared<meta::cpp2::IndexItem>(std::move(person_no_props_index)));
    tagIndexes_[1].emplace_back(
        std::make_shared<meta::cpp2::IndexItem>(std::move(book_name_index)));
    edgeIndexes_.emplace(1, std::vector<std::shared_ptr<meta::cpp2::IndexItem>>{});
    edgeIndexes_[1].emplace_back(
        std::make_shared<meta::cpp2::IndexItem>(std::move(like_index)));
}

}   // namespace graph
}   // namespace nebula

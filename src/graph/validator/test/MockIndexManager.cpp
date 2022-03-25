/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/test/MockIndexManager.h"

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
  person_no_props_index.index_id_ref() = 233;
  person_no_props_index.index_name_ref() = "person_no_props_index";
  nebula::cpp2::SchemaID personSchemaId;
  personSchemaId.tag_id_ref() = 2;
  person_no_props_index.schema_id_ref() = std::move(personSchemaId);
  person_no_props_index.schema_name_ref() = "person";

  meta::cpp2::IndexItem book_name_index;
  book_name_index.index_id_ref() = 234;
  book_name_index.index_name_ref() = "book_name_index";
  nebula::cpp2::SchemaID bookSchemaId;
  bookSchemaId.tag_id_ref() = 5;
  book_name_index.schema_id_ref() = std::move(bookSchemaId);
  book_name_index.schema_name_ref() = "book";
  meta::cpp2::ColumnDef field;
  field.name_ref() = "name";
  meta::cpp2::ColumnTypeDef type;
  type.type_ref() = nebula::cpp2::PropertyType::FIXED_STRING;
  type.type_length_ref() = 32;
  field.type_ref() = std::move(type);
  book_name_index.fields_ref() = {};
  book_name_index.fields_ref().value().emplace_back(std::move(field));

  meta::cpp2::IndexItem like_index;
  like_index.index_id_ref() = 235;
  like_index.index_name_ref() = "like_index";
  nebula::cpp2::SchemaID likeSchemaId;
  likeSchemaId.edge_type_ref() = 3;
  like_index.schema_id_ref() = std::move(likeSchemaId);
  like_index.schema_name_ref() = "like";

  tagIndexes_.emplace(1, std::vector<std::shared_ptr<meta::cpp2::IndexItem>>{});
  tagIndexes_[1].emplace_back(
      std::make_shared<meta::cpp2::IndexItem>(std::move(person_no_props_index)));
  tagIndexes_[1].emplace_back(std::make_shared<meta::cpp2::IndexItem>(std::move(book_name_index)));
  edgeIndexes_.emplace(1, std::vector<std::shared_ptr<meta::cpp2::IndexItem>>{});
  edgeIndexes_[1].emplace_back(std::make_shared<meta::cpp2::IndexItem>(std::move(like_index)));
}

}  // namespace graph
}  // namespace nebula

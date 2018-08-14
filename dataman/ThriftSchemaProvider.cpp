/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "dataman/ThriftSchemaProvider.h"

namespace vesoft {
namespace vgraph {

using namespace folly::hash;

/***********************************
 *
 * ThriftSchemaField
 *
 **********************************/
ThriftSchemaProvider::ThriftSchemaField::ThriftSchemaField(
    const cpp2::ColumnDef* col)
        : column_(col) {
}


const char* ThriftSchemaProvider::ThriftSchemaField::getName() const {
    DCHECK(!!column_);
    return column_->get_name().c_str();
}


const cpp2::ValueType* ThriftSchemaProvider::ThriftSchemaField::getType() const {
    DCHECK(!!column_);
    return &(column_->get_type());
}


bool ThriftSchemaProvider::ThriftSchemaField::isValid() const {
    return column_ != nullptr;
}


/***********************************
 *
 * ThriftSchemaProvider
 *
 **********************************/
ThriftSchemaProvider::ThriftSchemaProvider(const cpp2::Schema* schema)
        : columns_(&(schema->get_columns())) {
    for (int i = 0; i < columns_->size(); i++) {
        const std::string& name = (*columns_)[i].get_name();
        nameIndex_.emplace(
            std::make_pair(SpookyHashV2::Hash64(name.data(), name.size(), 0), i)
        );
    }
}


int32_t ThriftSchemaProvider::getLatestVer() const noexcept {
    return 0;
}


int32_t ThriftSchemaProvider::getNumFields(int32_t /*ver*/) const noexcept {
    return columns_->size();
}


int32_t ThriftSchemaProvider::getFieldIndex(const folly::StringPiece name,
                                           int32_t ver) const {
    uint64_t hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
    auto iter = nameIndex_.find(hash);
    if (iter == nameIndex_.end()) {
        return -1;
    }
    return iter->second;
}


const char* ThriftSchemaProvider::getFieldName(int32_t index,
                                              int32_t ver) const {
    if (index < 0 || index >= columns_->size()) {
        return nullptr;
    }
    return (*columns_)[index].get_name().c_str();
}


const cpp2::ValueType* ThriftSchemaProvider::getFieldType(int32_t index,
                                                         int32_t ver) const {
    if (index < 0 || index >= columns_->size()) {
        return nullptr;
    }

    return &((*columns_)[index].get_type());
}


const cpp2::ValueType* ThriftSchemaProvider::getFieldType(
        const folly::StringPiece name,
        int32_t ver) const {
    int32_t index = getFieldIndex(name, ver);
    if (index < 0) {
        return nullptr;
    }
    return &((*columns_)[index].get_type());
}


std::unique_ptr<ThriftSchemaProvider::Field>
ThriftSchemaProvider::field(int32_t index, int32_t ver) const {
    if (index < 0 || index >= columns_->size()) {
        return std::unique_ptr<Field>();
    }
    return std::make_unique<ThriftSchemaField>(&((*columns_)[index]));
}


std::unique_ptr<ThriftSchemaProvider::Field>
ThriftSchemaProvider::field(const folly::StringPiece name, int32_t ver) const {
    int32_t index = getFieldIndex(name, ver);
    if (index < 0) {
        return std::unique_ptr<Field>();
    }
    return std::make_unique<ThriftSchemaField>(&((*columns_)[index]));
}

}  // namespace vgraph
}  // namespace vesoft


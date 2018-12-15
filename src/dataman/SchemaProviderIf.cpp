/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "dataman/SchemaProviderIf.h"

namespace nebula {

/******************************************
 *
 * Iterator implementation
 *
 *****************************************/
SchemaProviderIf::Iterator::Iterator(const SchemaProviderIf* schema,
                                     int32_t ver,
                                     int32_t idx)
        : schema_(schema)
        , schemaVer_(ver)
        , numFields_(schema_->getNumFields(schemaVer_))
        , index_(idx) {
    field_ = schema_->field(index_, schemaVer_);
}


const SchemaProviderIf::Field& SchemaProviderIf::Iterator::operator*() const {
    return *field_;
}


const SchemaProviderIf::Field* SchemaProviderIf::Iterator::operator->() const {
    return field_.get();
}


SchemaProviderIf::Iterator& SchemaProviderIf::Iterator::operator++() {
    if (field_) {
        index_++;
        field_ = schema_->field(index_, schemaVer_);
    }
    return *this;
}


SchemaProviderIf::Iterator& SchemaProviderIf::Iterator::operator+(uint16_t steps) {
    if (field_) {
        index_ += steps;
        field_ = schema_->field(index_, schemaVer_);
    }
    return *this;
}


SchemaProviderIf::Iterator::operator bool() const {
    return (bool)field_;
}


bool SchemaProviderIf::Iterator::operator==(const Iterator& rhs) const {
    return schema_ == rhs.schema_ &&
           schemaVer_ == rhs.schemaVer_ &&
           (index_ == rhs.index_ || (!field_ && !rhs.field_));
}


/******************************************
 *
 * Iterator implementation
 *
 *****************************************/
SchemaProviderIf::Iterator SchemaProviderIf::begin(int32_t ver) const {
    return Iterator(this, ver, 0);
}


SchemaProviderIf::Iterator SchemaProviderIf::end(int32_t ver) const {
    return Iterator(this, ver, getNumFields(ver));
}

}  // namespace nebula


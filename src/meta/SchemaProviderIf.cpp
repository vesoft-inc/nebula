/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/SchemaProviderIf.h"

namespace nebula {
namespace meta {

/******************************************
 *
 * Iterator implementation
 *
 *****************************************/
SchemaProviderIf::Iterator::Iterator(const SchemaProviderIf* schema,
                                     int64_t idx)
        : schema_(schema)
        , numFields_(schema_->getNumFields())
        , index_(idx) {
    field_ = schema_->field(index_);
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
        field_ = schema_->field(index_);
    }
    return *this;
}


SchemaProviderIf::Iterator& SchemaProviderIf::Iterator::operator+(uint16_t steps) {
    if (field_) {
        index_ += steps;
        field_ = schema_->field(index_);
    }
    return *this;
}


SchemaProviderIf::Iterator::operator bool() const {
    return static_cast<bool>(field_);
}


bool SchemaProviderIf::Iterator::operator==(const Iterator& rhs) const {
    return schema_ == rhs.schema_ &&
           (index_ == rhs.index_ || (!field_ && !rhs.field_));
}


/******************************************
 *
 * Iterator implementation
 *
 *****************************************/
SchemaProviderIf::Iterator SchemaProviderIf::begin() const {
    return Iterator(this, 0);
}


SchemaProviderIf::Iterator SchemaProviderIf::end() const {
    return Iterator(this, getNumFields());
}

}  // namespace meta
}  // namespace nebula


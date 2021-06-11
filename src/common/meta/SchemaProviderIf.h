/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_META_SCHEMAPROVIDERIF_H_
#define COMMON_META_SCHEMAPROVIDERIF_H_

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/expression/Expression.h"


namespace nebula {
namespace meta {

class SchemaProviderIf {
public:
    // This is an interface class
    class Field {
    public:
        virtual ~Field() = default;

        virtual const char* name() const = 0;
        virtual cpp2::PropertyType type() const = 0;
        virtual bool nullable() const = 0;
        virtual bool hasDefault() const = 0;
        virtual Expression* defaultValue() const = 0;
        // This method returns the number of bytes the field will occupy
        // when the field is persisted on the storage medium
        // For the variant length string, the size will return 8
        // (4 bytes for offset and 4 bytes for string length)
        // For the fixed length string, the size will be the pre-defined
        // string length
        virtual size_t size() const = 0;
        // In v1, this always returns 0
        // In v2, this returns the offset of the field
        virtual size_t offset() const = 0;
        // In v1, this always returns 0
        // In v2, if the field is nullable, it returns the position of
        // the null flag bit, otherwise, it returns 0
        virtual size_t nullFlagPos() const = 0;
    };

    // Inherited classes do not need to implement the Iterator
    class Iterator final {
        friend class SchemaProviderIf;

    public:
        const Field& operator*() const {
            return *field_;
        }

        const Field* operator->() const {
            return field_;
        }

        Iterator& operator++() {
            if (field_) {
                index_++;
                field_ = schema_->field(index_);
            }
            return *this;
        }

        Iterator& operator+(uint16_t steps) {
            if (field_) {
                index_ += steps;
                field_ = schema_->field(index_);
            }
            return *this;
        }

        operator bool() const {
            return static_cast<bool>(field_);
        }

        bool operator==(const Iterator& rhs) const {
            return schema_ == rhs.schema_ &&
                   (index_ == rhs.index_ || (!field_ && !rhs.field_));
        }

    private:
        const SchemaProviderIf* schema_;
        size_t numFields_;
        int64_t index_;
        const Field* field_;

    private:
        explicit Iterator(const SchemaProviderIf* schema,
                          int64_t idx = 0)
                : schema_(schema)
                , numFields_(schema_->getNumFields())
                , index_(idx) {
            field_ = schema_->field(index_);
        }
    };

public:
    virtual ~SchemaProviderIf() = default;

    virtual SchemaVer getVersion() const noexcept = 0;
    virtual size_t getNumFields() const noexcept = 0;
    virtual size_t getNumNullableFields() const noexcept = 0;

    // Return the number of bytes occupied by when each row of data
    // persisted on the disk
    virtual size_t size() const noexcept = 0;

    virtual int64_t getFieldIndex(const std::string& name) const = 0;
    virtual const char* getFieldName(int64_t index) const = 0;

    virtual cpp2::PropertyType getFieldType(int64_t index) const = 0;
    virtual cpp2::PropertyType getFieldType(const std::string& name) const = 0;

    virtual const Field* field(int64_t index) const = 0;
    virtual const Field* field(const std::string& name) const = 0;

    /******************************************
     *
     * Iterator implementation
     *
     *****************************************/
    Iterator begin() const {
        return Iterator(this, 0);
    }


    Iterator end() const {
        return Iterator(this, getNumFields());
    }
};


}  // namespace meta
}  // namespace nebula
#endif  // COMMON_META_SCHEMAPROVIDERIF_H_
